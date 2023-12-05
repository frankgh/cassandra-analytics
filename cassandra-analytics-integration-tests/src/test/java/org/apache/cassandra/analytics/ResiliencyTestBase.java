/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.analytics;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import com.datastax.driver.core.ConsistencyLevel;
import o.a.c.analytics.sidecar.shaded.testing.adapters.base.StorageJmxOperations;
import o.a.c.analytics.sidecar.shaded.testing.common.JmxClient;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.Row;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.spark.bulkwriter.DecoratedKey;
import org.apache.cassandra.spark.bulkwriter.Tokenizer;
import org.apache.cassandra.spark.common.schema.ColumnType;
import org.apache.cassandra.spark.common.schema.ColumnTypes;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import static junit.framework.TestCase.assertTrue;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for resiliency tests. Contains helper methods for data generation and validation
 */
public abstract class ResiliencyTestBase extends IntegrationTestBase
{
    private static final String createTableStmt = "create table if not exists %s (id int, course text, marks int, primary key (id));";
    protected static final String retrieveRows = "select * from " + TEST_KEYSPACE + ".%s";
    public static final int rowCount = 1000;

    public QualifiedTableName initializeSchema()
    {
        return initializeSchema(ImmutableMap.of("datacenter1", 1));
    }

    public QualifiedTableName initializeSchema(Map<String, Integer> rf)
    {
        createTestKeyspace(rf);
        return createTestTable(createTableStmt);
    }

    public SparkConf generateSparkConf()
    {
        SparkConf sparkConf = new SparkConf()
                              .setAppName("Integration test Spark Cassandra Bulk Reader Job")
                              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                              .set("spark.master", "local[8,4]");
        BulkSparkConf.setupSparkConf(sparkConf, true);
        KryoRegister.setup(sparkConf);
        return sparkConf;
    }

    public SparkSession generateSparkSession(SparkConf sparkConf)
    {
        return SparkSession.builder()
                           .config(sparkConf)
                           .getOrCreate();
    }

    public Set<String> getDataForRange(Range<BigInteger> range)
    {
        // Iterate through all data entries; filter only entries that belong to range; convert to strings
        return generateExpectedData().stream()
                   .filter(t -> range.contains(t._1().getToken()))
                   .map(t -> t._2()[0] + ":" + t._2()[1] + ":" + t._2()[2])
                   .collect(Collectors.toSet());
    }

    public List<Tuple2<DecoratedKey, Object[]>> generateExpectedData()
    {
        // "create table if not exists %s (id int, course text, marks int, primary key (id));";
        List<ColumnType<?>> columnTypes = Arrays.asList(ColumnTypes.INT);
        Tokenizer tokenizer = new Tokenizer(Arrays.asList(0),
                                            Arrays.asList("id"),
                                            columnTypes,
                                            true
        );
        return IntStream.range(0, rowCount).mapToObj(recordNum -> {
            Object[] columns = new Object[]
                               {
                               recordNum, "course" + recordNum, recordNum
                               };
            return Tuple2.apply(tokenizer.getDecoratedKey(columns), columns);
        }).collect(Collectors.toList());
    }

    public Map<IUpgradeableInstance, Set<String>> getInstanceData(List<IUpgradeableInstance> instances,
                                                                  boolean isPending)
    {

        return instances.stream().collect(Collectors.toMap(Function.identity(),
                                                           i -> filterTokenRangeData(getRangesForInstance(i, isPending))));
    }

    public Set<String> filterTokenRangeData(List<Range<BigInteger>> ranges)
    {
        return ranges.stream()
                 .map(r -> getDataForRange(r))
                 .flatMap(Collection::stream)
                 .collect(Collectors.toSet());
    }

    private List<Range<BigInteger>> getRangesForInstance(IUpgradeableInstance instance, boolean isPending)
    {
        IInstanceConfig config = instance.config();
        JmxClient client = JmxClient.builder()
                                    .host(config.broadcastAddress().getAddress().getHostAddress())
                                    .port(config.jmxPort())
                                    .build();
        StorageJmxOperations ss = client.proxy(StorageJmxOperations.class, "org.apache.cassandra.db:type=StorageService");

        Map<List<String>, List<String>> ranges = isPending ? ss.getPendingRangeToEndpointWithPortMap(TEST_KEYSPACE)
                                                           : ss.getRangeToEndpointWithPortMap(TEST_KEYSPACE);

        // filter ranges that belong to the instance
        return ranges.entrySet()
                            .stream()
                            .filter(e -> e.getValue().contains(instance.broadcastAddress().getAddress().getHostAddress()
                                                               + ":" + instance.broadcastAddress().getPort()))
                            .map(e -> unwrapRanges(e.getKey()))
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
    }

    /**
     * Returns the expected set of rows as strings for each instance in the cluster
     */
    public Map<IUpgradeableInstance, Set<String>> generateExpectedInstanceData(UpgradeableCluster cluster,
                                                                                List<IUpgradeableInstance> pendingNodes)
    {
        List<IUpgradeableInstance> instances = cluster.stream().collect(Collectors.toList());
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData = getInstanceData(instances, false);
        // Use pending ranges to get data for each transitioning instance
        Map<IUpgradeableInstance, Set<String>> transitioningInstanceData = getInstanceData(pendingNodes, true);
        expectedInstanceData.putAll(transitioningInstanceData.entrySet()
                                                             .stream()
                                                             .filter(e -> !e.getValue().isEmpty())
                                                             .collect(Collectors.toMap(Map.Entry::getKey,
                                                                                       Map.Entry::getValue)));
        return  expectedInstanceData;
    }

    private List<Range<BigInteger>> unwrapRanges(List<String> range)
    {
        List<Range<BigInteger>> ranges = new ArrayList<Range<BigInteger>>();
        BigInteger start = new BigInteger(range.get(0));
        BigInteger end = new BigInteger(range.get(1));
        if (start.compareTo(end) > 0)
        {
            ranges.add(Range.openClosed(start, BigInteger.valueOf(Long.MAX_VALUE)));
            ranges.add(Range.openClosed(BigInteger.valueOf(Long.MIN_VALUE), end));
        }
        else
        {
            ranges.add(Range.openClosed(start, end));
        }
        return ranges;
    }

    public Dataset<org.apache.spark.sql.Row> generateData(SparkSession spark)
    {
        SQLContext sql = spark.sqlContext();
        StructType schema = new StructType()
                            .add("id", IntegerType, false)
                            .add("course", StringType, false)
                            .add("marks", IntegerType, false);

        List<org.apache.spark.sql.Row> rows = IntStream.range(0, rowCount)
                                                       .mapToObj(recordNum -> {
                                                           String course = "course" + recordNum;
                                                           ArrayList<Object> values = new ArrayList<>(Arrays.asList(recordNum, course, recordNum));
                                                           return RowFactory.create(values.toArray());
                                                       }).collect(Collectors.toList());
        return sql.createDataFrame(rows, schema);
    }

    public void validateData(String tableName, ConsistencyLevel cl)
    {
        String query = String.format(retrieveRows, tableName);
        try
        {
            SimpleQueryResult resultSet = sidecarTestContext.cluster().get(1).coordinator()
                                                            .executeWithResult(query, mapConsistencyLevel(cl));
            Set<String> rows = new HashSet<>();
            for (SimpleQueryResult it = resultSet; it.hasNext();)
            {
                Row row = it.next();
                if (row.get("id") == null || row.get("course") == null || row.get("marks") == null)
                {
                    throw new RuntimeException("Unrecognized row in table");
                }

                int id = row.getInteger("id");
                String course = row.getString("course");
                int marks = row.getInteger("marks");
                rows.add(id + ":" + course + ":" + marks);
            }
            for (int i = 0; i < rowCount; i++)
            {
                String expectedRow = i + ":course" + i + ":" + i;
                rows.remove(expectedRow);
            }
            assertTrue(rows.isEmpty());
        }
        catch (Exception ex)
        {
            logger.error("Validation Query failed", ex);
            throw ex;
        }
    }

    private org.apache.cassandra.distributed.api.ConsistencyLevel mapConsistencyLevel(ConsistencyLevel cl)
    {
        return org.apache.cassandra.distributed.api.ConsistencyLevel.valueOf(cl.name());
    }

    public void validateNodeSpecificData(QualifiedTableName table,
                                         Map<IUpgradeableInstance, Set<String>> expectedInstanceData)
    {
        validateNodeSpecificData(table, expectedInstanceData, true);
    }
    public void validateNodeSpecificData(QualifiedTableName table,
                                         Map<IUpgradeableInstance, Set<String>> expectedInstanceData,
                                         boolean hasNewNodes)
    {
        for (IUpgradeableInstance instance : expectedInstanceData.keySet())
        {
            SimpleQueryResult qr = instance.executeInternalWithResult(String.format(retrieveRows, table.tableName()));
            Set<String> rows = new HashSet<>();
            while (qr.hasNext())
            {
                org.apache.cassandra.distributed.api.Row row = qr.next();
                int id = row.getInteger("id");
                String course = row.getString("course");
                int marks = row.getInteger("marks");
                rows.add(id + ":" + course + ":" + marks);
            }

            if (hasNewNodes)
            {
                assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedInstanceData.get(instance));
            }
            else
            {
                assertThat(rows).containsAll(expectedInstanceData.get(instance));
            }
        }
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext)
    throws IOException
    {
        return getMultiDCCluster(initializer, cassandraTestContext, null);
    }

    protected UpgradeableCluster getMultiDCCluster(BiConsumer<ClassLoader, Integer> initializer,
                                                   ConfigurableCassandraTestContext cassandraTestContext,
                                                   Consumer<UpgradeableCluster.Builder> additionalConfigurator)
    throws IOException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier mdcTokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                   annotation.newNodesPerDc(),
                                                                                   annotation.numDcs(),
                                                                                   1);

        int totalNodeCount = (annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs();
        return cassandraTestContext.configureAndStartCluster(
        builder -> {
            builder.withInstanceInitializer(initializer);
            builder.withTokenSupplier(mdcTokenSupplier);
            builder.withNodeIdTopology(networkTopology(totalNodeCount,
                                                       (nodeId) -> nodeId % 2 != 0 ?
                                                                   dcAndRack("datacenter1", "rack1") :
                                                                   dcAndRack("datacenter2", "rack2")));

            if (additionalConfigurator != null)
            {
                additionalConfigurator.accept(builder);
            }
        });
    }

    protected QualifiedTableName bulkWriteData(ConsistencyLevel writeCL) throws InterruptedException
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<String> sidecarInstances = generateSidecarInstances((annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs());

        ImmutableMap<String, Integer> rf;
        if (annotation.numDcs() > 1 && annotation.useCrossDcKeyspace())
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF, "datacenter2", DEFAULT_RF);
        }
        else
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF);
        }

        QualifiedTableName schema = initializeSchema(rf);
        Thread.sleep(2000);

        SparkConf sparkConf = generateSparkConf();
        SparkSession spark = generateSparkSession(sparkConf);
        Dataset<org.apache.spark.sql.Row> df = generateData(spark);

        DataFrameWriter<org.apache.spark.sql.Row> dfWriter = df.write()
                                                               .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                                               .option("bulk_writer_cl", writeCL.name())
                                                               .option("local_dc", "datacenter1")
                                                               .option("sidecar_instances", String.join(",", sidecarInstances))
                                                               .option("sidecar_port", String.valueOf(server.actualPort()))
                                                               .option("keyspace", schema.keyspace())
                                                               .option("table", schema.tableName())
                                                               .option("number_splits", "-1")
                                                               .mode("append");

        dfWriter.save();
        return schema;
    }

    protected List<String> generateSidecarInstances(int numNodes)
    {
        List<String> sidecarInstances = new ArrayList<>();
        sidecarInstances.add("localhost");
        for (int i = 2; i <= numNodes; i++)
        {
            sidecarInstances.add("localhost" + i);
        }
        return sidecarInstances;
    }
}

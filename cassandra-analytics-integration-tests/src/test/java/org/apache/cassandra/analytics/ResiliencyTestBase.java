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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxTestContext;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import o.a.c.analytics.sidecar.shaded.testing.common.data.TokenRangeReplicasResponse;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(ResiliencyTestBase.class);
    private static final String createTableStmt = "create table if not exists %s (id int, course text, marks int, primary key (id));";
    private static final String retrieveRows = "select * from " + TEST_KEYSPACE + ".%s";
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

    public Set<String> getDataForRange(List<Tuple2<DecoratedKey, Object[]>> data, Range<BigInteger> range)
    {
        return data.stream()
                   .filter(t -> range.contains(t._1().getToken()))
                   .map(t -> t._2()[0] + ":" + t._2()[1] + ":" + t._2()[2])
                   .collect(Collectors.toSet());
    }

    public List<Tuple2<DecoratedKey, Object[]>> generateData(int rowCount)
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

    public void validateData(Session session, String tableName, ConsistencyLevel cl)
    {
        PreparedStatement preparedStatement = session.prepare(String.format(retrieveRows, tableName));
        preparedStatement.setConsistencyLevel(cl);
        BoundStatement boundStatement = preparedStatement.bind();
        ResultSet resultSet = session.execute(boundStatement);
        Set<String> rows = new HashSet<>();
        for (Row row : resultSet.all())
        {
            if (row.isNull("id") || row.isNull("course") || row.isNull("marks"))
            {
                throw new RuntimeException("Unrecognized row in table");
            }

            int id = row.getInt("id");
            String course = row.getString("course");
            int marks = row.getInt("marks");
            rows.add(id + ":" + course + ":" + marks);
        }

        for (int i = 0; i < rowCount; i++)
        {
            String expectedRow = i + ":course" + i + ":" + i;
            rows.remove(expectedRow);
        }
        assertTrue(rows.isEmpty());
    }

    public void validateTransientNodeData(VertxTestContext context,
                                          QualifiedTableName table,
                                          List<IUpgradeableInstance> transientNodes) throws Exception
    {
        validateTransientNodeData(context, table, transientNodes, false);
    }
    public void validateTransientNodeData(VertxTestContext context,
                                           QualifiedTableName table,
                                           List<IUpgradeableInstance> transientNodes,
                                          boolean isMoving) throws Exception
    {
        List<Tuple2<DecoratedKey, Object[]>> tupleData = generateData(rowCount);
        retrieveMappingWithKeyspace(context, TEST_KEYSPACE, response -> {
            TokenRangeReplicasResponse mappingResponse = response.bodyAsJson(TokenRangeReplicasResponse.class);
            assertThat(response.statusCode()).isEqualTo(HttpResponseStatus.OK.code());

            for (IUpgradeableInstance instance : transientNodes)
            {
                validateRowsOnInstance(instance, table.tableName(), mappingResponse, tupleData, isMoving);
            }

            context.completeNow();
        });
    }

    public void validateRowsOnInstance(IUpgradeableInstance instance,
                                       String tableName,
                                       TokenRangeReplicasResponse tokenRangesResponse,
                                       List<Tuple2<DecoratedKey, Object[]>> tupleData,
                                       boolean isMoving)
    {
        Set<String> expectedRows = filterRowsForInstance(tokenRangesResponse,
                                                         instance,
                                                         tupleData);

        SimpleQueryResult qr = instance.executeInternalWithResult(String.format(retrieveRows, tableName));
        Set<String> rows = new HashSet<>();
        while (qr.hasNext())
        {
            org.apache.cassandra.distributed.api.Row row = qr.next();
            int id = row.getInteger("id");
            String course = row.getString("course");
            int marks = row.getInteger("marks");
            rows.add(id + ":" + course + ":" + marks);
        }

        if (isMoving)
        {
            assertThat(rows).containsAll(expectedRows);
        }
        else
        {
            assertThat(rows).containsExactlyInAnyOrderElementsOf(expectedRows);
        }
    }

    private Set<String> filterRowsForInstance(TokenRangeReplicasResponse mappingResponse,
                                              IUpgradeableInstance instance,
                                              List<Tuple2<DecoratedKey, Object[]>> tupleData)
    {
        List<Range<BigInteger>> ranges = getTokenRangesForInstance(mappingResponse, instance);
        return ranges.stream()
                     .map(r -> getDataForRange(tupleData, r))
                     .flatMap(Collection::stream)
                     .collect(Collectors.toSet());
    }

    private List<Range<BigInteger>> getTokenRangesForInstance(TokenRangeReplicasResponse mappingResponse,
                                                              IUpgradeableInstance instance)
    {
        String instanceAddress = String.format("%s:%s",
                                               instance.broadcastAddress().getAddress().getHostAddress(),
                                               instance.broadcastAddress().getPort());
        return mappingResponse.writeReplicas()
                              .stream()
                              .filter(i -> i.replicasByDatacenter().values().stream()
                                            .flatMap(Collection::stream)
                                            .anyMatch(ins -> ins.equals(instanceAddress)))
                              .map(i -> Range.openClosed(BigInteger.valueOf(Long.parseLong(i.start())),
                                                         BigInteger.valueOf(Long.parseLong(i.end()))))
                              .collect(Collectors.toList());
    }

    void retrieveMappingWithKeyspace(VertxTestContext context, String keyspace,
                                     Handler<HttpResponse<Buffer>> verifier) throws Exception
    {
        String testRoute = "/api/v1/keyspaces/" + keyspace + "/token-range-replicas";
        testWithClient(context, client -> {
            client.get(server.actualPort(), "127.0.0.1", testRoute)
                  .send(context.succeeding(verifier));
        });
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

    protected QualifiedTableName bulkWriteData(boolean isCrossDCKeyspace, ConsistencyLevel writeCL)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        List<String> sidecarInstances = generateSidecarInstances((annotation.nodesPerDc() + annotation.newNodesPerDc()) * annotation.numDcs());

        ImmutableMap<String, Integer> rf;
        if (annotation.numDcs() > 1 && isCrossDCKeyspace)
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF, "datacenter2", DEFAULT_RF);
        }
        else
        {
            rf = ImmutableMap.of("datacenter1", DEFAULT_RF);
        }

        QualifiedTableName schema = initializeSchema(rf);

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

    protected QualifiedTableName bulkWriteData(ConsistencyLevel writeCL)
    {
        return bulkWriteData(false, writeCL);
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

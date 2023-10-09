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

package org.apache.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static junit.framework.TestCase.assertTrue;
import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Base class for resiliency tests. Contains helper methods for data generation and validation
 */
public abstract class ResiliencyTestBase extends IntegrationTestBase
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ResiliencyTestBase.class);
    private static final String createTableStmt = "create table if not exists %s (id int, course text, marks int, primary key (id));";
    private static final String retrieveRows = "select * from " + TEST_KEYSPACE + ".%s";
    private static final int rowCount = 1000;

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
                              .set("spark.master", "local[8]");
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

    QualifiedTableName bulkWriteData()
    {
        ImmutableMap<String, Integer> rf = ImmutableMap.of("datacenter1", 3);
        QualifiedTableName schema = initializeSchema(rf);

        SparkConf sparkConf = generateSparkConf();
        SparkSession spark = generateSparkSession(sparkConf);
        Dataset<org.apache.spark.sql.Row> df = generateData(spark);

        LOGGER.info("Spark Conf: " + sparkConf.toDebugString());

        // A constant timestamp and TTL can be used by setting the following options.
        // .option(WriterOptions.TTL.name(), TTLOption.constant(20))
        // .option(WriterOptions.TIMESTAMP.name(), TimestampOption.constant(System.currentTimeMillis() * 1000))
        DataFrameWriter<org.apache.spark.sql.Row> dfWriter = df.write()
                                                               .format("org.apache.cassandra.spark.sparksql.CassandraDataSink")
                                                               .option("sidecar_instances", "localhost,localhost2,localhost3")
                                                               .option("sidecar_port", String.valueOf(server.actualPort()))
                                                               .option("keyspace", schema.keyspace())
                                                               .option("table", schema.tableName())
                                                               .option("local_dc", "datacenter1")
                                                               .option("bulk_writer_cl", "LOCAL_QUORUM")
                                                               .option("number_splits", "-1")
                                                               .mode("append");
        dfWriter.save();
        return schema;
    }
}

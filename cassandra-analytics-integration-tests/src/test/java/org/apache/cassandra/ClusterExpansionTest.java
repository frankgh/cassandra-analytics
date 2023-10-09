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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.spark.example.SampleCassandraJob;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class ClusterExpansionTest extends ResiliencyTestBase
{
    @CassandraIntegrationTest(nodesPerDc = 3, gossip = true, network = true)
    public void sampleResiliencyTest()
    {
        QualifiedTableName schema = bulkWriteData();

        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.EACH_QUORUM);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, gossip = true, network = true)
    public void runSimpleTest()
    {
        sidecarTestContext.cluster().schemaChange(
        "  CREATE KEYSPACE spark_test WITH replication = "
        + "{'class': 'NetworkTopologyStrategy', 'datacenter1': '3'}\n"
        + "      AND durable_writes = true;");
        sidecarTestContext.cluster().schemaChange("CREATE TABLE spark_test.test (\n"
                                                  + "          id BIGINT PRIMARY KEY,\n"
                                                  + "          course BLOB,\n"
                                                  + "          marks BIGINT\n"
                                                  + "     );");

        UpgradeableCluster cluster = sidecarTestContext.cluster();
        IUpgradeableInstance instance = cluster.get(1);
        IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                    instance.config().localDatacenter(),
                                                                    instance.config().localRack(),
                                                                    inst -> inst.with(Feature.NETWORK,
                                                                                      Feature.GOSSIP,
                                                                                      Feature.JMX,
                                                                                      Feature.NATIVE_PROTOCOL));
        new Thread(() -> newInstance.startup(cluster)).start();
        cluster.get(4).startup(cluster);

        SampleCassandraJob.main(new String[]
                                {
                                String.valueOf(server.actualPort())
                                });
        ClusterUtils.awaitRingState(instance, newInstance, "Normal");
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, gossip = true, network = true, buildCluster = false)
    public void expandBySingleNodeTest(ConfigurableCassandraTestContext cassandraTestContext) throws IOException
    {
        BBHelperSingleJoiningNode.reset();
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc() + annotation.newNodesPerDc(),
                                                                            1);
        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperSingleJoiningNode::install);
            builder.withTokenSupplier(tokenSupplier);
        });

        QualifiedTableName schema;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);

            List<IUpgradeableInstance> newInstances = new ArrayList<>();
            // Go over new nodes and add them once for each DC
            for (int i = 0; i < annotation.newNodesPerDc(); i++)
            {
                int dcNodeIdx = 1; // Use node 2's DC
                for (int dc = 1; dc <= annotation.numDcs(); dc++)
                {
                    IUpgradeableInstance dcNode = cluster.get(dcNodeIdx++);
                    IUpgradeableInstance newInstance = ClusterUtils.addInstance(cluster,
                                                                                dcNode.config().localDatacenter(),
                                                                                dcNode.config().localRack(),
                                                                                inst -> {
                                                                                    inst.set("auto_bootstrap", true);
                                                                                    inst.with(Feature.GOSSIP,
                                                                                              Feature.JMX,
                                                                                              Feature.NATIVE_PROTOCOL);
                                                                                });
                    new Thread(() -> newInstance.startup(cluster)).start();
                    newInstances.add(newInstance);
                }
            }

            Uninterruptibles.awaitUninterruptibly(BBHelperSingleJoiningNode.transientStateStart, 2, TimeUnit.MINUTES);

            for (IUpgradeableInstance newInstance : newInstances)
            {
                ClusterUtils.awaitRingState(seed, newInstance, "Joining");
            }

            schema = bulkWriteData();
        }
        finally
        {
            for (int i = 0;
                 i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                BBHelperSingleJoiningNode.transientStateEnd.countDown();
            }
        }
        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.EACH_QUORUM);
    }

    /**
     * ByteBuddy helper for a single joining node
     */
    @Shared
    public static class BBHelperSingleJoiningNode
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 3 node cluster with 1 joining node
            // We intercept the bootstrap of the leaving node (4) to validate token ranges
            if (nodeNumber == 4)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperSingleJoiningNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        public static boolean bootstrap(Collection<?> tokens,
                                        long bootstrapTimeoutMillis,
                                        @SuperCall Callable<Boolean> orig) throws Exception
        {
            boolean result = orig.call();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return result;
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}

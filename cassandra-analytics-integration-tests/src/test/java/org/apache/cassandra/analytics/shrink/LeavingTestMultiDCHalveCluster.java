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

package org.apache.cassandra.analytics.shrink;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datastax.driver.core.ConsistencyLevel;
import io.vertx.junit5.VertxExtension;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

@ExtendWith(VertxExtension.class)
class LeavingTestMultiDCHalveCluster extends LeavingBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void allReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transientStateStart,
                               BBHelperHalveClusterMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ALL,
                               ConsistencyLevel.ONE,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadLocalQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transientStateStart,
                               BBHelperHalveClusterMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.LOCAL_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void localQuorumReadEachQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transientStateStart,
                               BBHelperHalveClusterMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.LOCAL_QUORUM,
                               ConsistencyLevel.EACH_QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void quorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transientStateStart,
                               BBHelperHalveClusterMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM,
                               false);
    }

    @CassandraIntegrationTest(nodesPerDc = 6, numDcs = 2, network = true, gossip = true, buildCluster = false)
    void oneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperHalveClusterMultiDC.reset();
        int leavingNodesPerDC = 3;
        UpgradeableCluster cluster = getMultiDCCluster(BBHelperHalveClusterMultiDC::install, cassandraTestContext);

        runLeavingTestScenario(leavingNodesPerDC,
                               BBHelperHalveClusterMultiDC.transientStateStart,
                               BBHelperHalveClusterMultiDC.transientStateEnd,
                               cluster,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL,
                               false);
    }

    /**
     * ByteBuddy helper for halve cluster size with multi-DC
     */
    @Shared
    public static class BBHelperHalveClusterMultiDC
    {
        static CountDownLatch transientStateStart = new CountDownLatch(6);
        static CountDownLatch transientStateEnd = new CountDownLatch(6);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves halving the size of a 12 node cluster (6 per DC)
            // We intercept the shutdown of the removed nodes (7-12) to validate token ranges
            if (nodeNumber > 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("unbootstrap"))
                               .intercept(MethodDelegation.to(BBHelperHalveClusterMultiDC.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static void unbootstrap(@SuperCall Callable<?> orig) throws Exception
        {
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            orig.call();
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(6);
            transientStateEnd = new CountDownLatch(6);
        }
    }
}

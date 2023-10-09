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
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
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
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.NodeMovementTest.BBHelperMovingNode.transientStateEnd;
import static org.apache.cassandra.NodeMovementTest.BBHelperMovingNode.transientStateStart;

public class NodeMovementTest extends ResiliencyTestBase
{

    public static final int MOVING_NODE_IDX = 5;

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeDuringBulkWriteTest(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNode.reset();
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperMovingNode::install);
            builder.withTokenSupplier(tokenSupplier);
        });

        QualifiedTableName schema;
        long moveTarget = getMoveTargetToken(cluster);
        int movingNodeIndex = MOVING_NODE_IDX;
        IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);

        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            new Thread(() -> movingNode.nodetoolResult("move", "--", Long.toString(moveTarget))
                                       .asserts()
                                       .success()).start();

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");

            schema = bulkWriteData();
        }
        finally
        {
            transientStateEnd.countDown();
        }

        ClusterUtils.awaitRingState(cluster.get(1), movingNode, "Normal");
        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.QUORUM);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeFailedDuringBulkWriteTest(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNodeFailure.reset();
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperMovingNodeFailure::install);
            builder.withTokenSupplier(tokenSupplier);
        });

        QualifiedTableName schema;
        long moveTarget = getMoveTargetToken(cluster);
        int movingNodeIndex = MOVING_NODE_IDX;
        IUpgradeableInstance movingNode = cluster.get(movingNodeIndex);

        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            new Thread(() -> movingNode.nodetoolResult("move", "--", Long.toString(moveTarget))
                                       .asserts()
                                       .success()).start();

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(BBHelperMovingNodeFailure.transientStateStart, 2, TimeUnit.MINUTES);
            ClusterUtils.awaitRingState(seed, movingNode, "Moving");

            schema = bulkWriteData();
        }
        finally
        {
            BBHelperMovingNodeFailure.transientStateEnd.countDown();
        }

        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.QUORUM);
        ClusterUtils.awaitRingState(cluster.get(1), movingNode, "Moving");
    }

    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMovingNodeFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNodeFailure.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

//        public static Future<?> move(@SuperCall Callable<?> orig) throws Exception
//        {
//            orig.call();
//            transientStateStart.countDown();
//            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
//
//            throw new IOException("Simulated node movement failures");
//        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);

            throw new IOException("Simulated node movement failures"); // Throws exception to nodetool
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }


    /**
     * ByteBuddy Helper for a single moving node
     */
    @Shared
    public static class BBHelperMovingNode
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == MOVING_NODE_IDX)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.RangeRelocator")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("stream"))
                               .intercept(MethodDelegation.to(BBHelperMovingNode.class))
                               // Defer class loading until all dependencies are loaded
                               .make(TypeResolutionStrategy.Lazy.INSTANCE, typePool)
                               .load(cl, ClassLoadingStrategy.Default.INJECTION);
            }
        }

        @SuppressWarnings("unused")
        public static Future<?> stream(@SuperCall Callable<Future<?>> orig) throws Exception
        {
            Future<?> res = orig.call();
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return res;
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }

        protected long getMoveTargetToken(UpgradeableCluster cluster)
        {
            CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
            IUpgradeableInstance seed = cluster.get(1);
            // The target token to move the node to is calculated by adding an offset to the seed node token which
            // is half of the range between 2 tokens.
            // For multi-DC case (specifically 2 DCs), since neighbouring tokens can be consecutive, we use tokens 1
            // and 3 to calculate the offset
            int nextIndex = (annotation.numDcs() > 1) ? 3 : 2;
            long t2 = Long.parseLong(seed.config().getString("initial_token"));
            long t3 = Long.parseLong(cluster.get(nextIndex).config().getString("initial_token"));
            return (t2 + ((t3 - t2) / 2));
        }
}

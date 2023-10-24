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

package org.apache.cassandra.analytics.movement;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.TypeResolutionStrategy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.pool.TypePool;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;

public class NodeMovementTest extends NodeMovementBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeQuorumReadAndWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNode.reset();
        runMovingNodeTest(cassandraTestContext,
                          BBHelperMovingNode::install,
                          BBHelperMovingNode.transientStateStart,
                          BBHelperMovingNode.transientStateEnd,
                          false,
                          false,
                          ConsistencyLevel.QUORUM,
                          ConsistencyLevel.QUORUM);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNode.reset();
        runMovingNodeTest(cassandraTestContext,
                          BBHelperMovingNode::install,
                          BBHelperMovingNode.transientStateStart,
                          BBHelperMovingNode.transientStateEnd,
                          false,
                          false,
                          ConsistencyLevel.ONE,
                          ConsistencyLevel.ALL);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeAllReadOneWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNode.reset();
        runMovingNodeTest(cassandraTestContext,
                          BBHelperMovingNode::install,
                          BBHelperMovingNode.transientStateStart,
                          BBHelperMovingNode.transientStateEnd,
                          false,
                          false,
                          ConsistencyLevel.ALL,
                          ConsistencyLevel.ONE);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeFailureQuorumReadAndWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNodeFailure.reset();
        runMovingNodeTest(cassandraTestContext,
                          BBHelperMovingNodeFailure::install,
                          BBHelperMovingNodeFailure.transientStateStart,
                          BBHelperMovingNodeFailure.transientStateEnd,
                          false,
                          true,
                          ConsistencyLevel.QUORUM,
                          ConsistencyLevel.QUORUM);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, network = true, gossip = true, buildCluster = false)
    void moveNodeFailureOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperMovingNodeFailure.reset();
        runMovingNodeTest(cassandraTestContext,
                          BBHelperMovingNodeFailure::install,
                          BBHelperMovingNodeFailure.transientStateStart,
                          BBHelperMovingNodeFailure.transientStateEnd,
                          false,
                          true,
                          ConsistencyLevel.ONE,
                          ConsistencyLevel.ALL);
    }

    /**
     * ByteBuddy Helper for a single moving node simulating a move failure
     */
    @Shared
    public static class BBHelperMovingNodeFailure
    {
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Moving the 5th node in the test case
            if (nodeNumber == SINGLE_DC_MOVING_NODE_IDX)
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
            if (nodeNumber == SINGLE_DC_MOVING_NODE_IDX)
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
}

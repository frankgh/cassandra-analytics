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

package org.apache.cassandra.analytics.expansion;

import java.util.Collection;
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
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

@ExtendWith(VertxExtension.class)
public class JoiningTestSingleNode extends JoiningBaseTest
{
    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void singleJoinNodeOneReadALLWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleJoiningNode.reset();
        runJoiningTestScenario(cassandraTestContext,
                               BBHelperSingleJoiningNode::install,
                               BBHelperSingleJoiningNode.transientStateStart,
                               BBHelperSingleJoiningNode.transientStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void singleJoinNodeFailureOneReadALLWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleJoiningNodeFailure.reset();
        runJoiningTestScenario(cassandraTestContext,
                               BBHelperSingleJoiningNode::install,
                               BBHelperSingleJoiningNode.transientStateStart,
                               BBHelperSingleJoiningNode.transientStateEnd,
                               ConsistencyLevel.ONE,
                               ConsistencyLevel.ALL);
    }

    @CassandraIntegrationTest(nodesPerDc = 3, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void singleJoinNodeFailureQuorumReadQuorumWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperSingleJoiningNodeFailure.reset();
        runJoiningTestScenario(cassandraTestContext,
                               BBHelperSingleJoiningNode::install,
                               BBHelperSingleJoiningNode.transientStateStart,
                               BBHelperSingleJoiningNode.transientStateEnd,
                               ConsistencyLevel.QUORUM,
                               ConsistencyLevel.QUORUM);
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

    /**
     * ByteBuddy helper for a single joining node failure case
     */
    @Shared
    public static class BBHelperSingleJoiningNodeFailure
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
            throw new UnsupportedOperationException("Simulated failure");
        }

        public static void reset()
        {
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}

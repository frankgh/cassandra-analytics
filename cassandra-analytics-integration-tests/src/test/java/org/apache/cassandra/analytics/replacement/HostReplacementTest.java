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

package org.apache.cassandra.analytics.replacement;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

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
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

public class HostReplacementTest extends HostReplacementBaseTest
{

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void hostReplacementQuorumReadAndWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNode.reset();
        runReplacementTest(cassandraTestContext,
                           BBHelperReplacementsNode::install,
                           BBHelperReplacementsNode.transientStateStart,
                           BBHelperReplacementsNode.transientStateEnd,
                           BBHelperReplacementsNode.nodeStart,
                           false,
                           false,
                           ConsistencyLevel.QUORUM,
                           ConsistencyLevel.QUORUM);
    }

    // Note: The following test depends on sidecar fix: https://issues.apache.org/jira/browse/CASSANDRASC-78
    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void hostReplacementOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNode.reset();
        runReplacementTest(cassandraTestContext,
                           BBHelperReplacementsNode::install,
                           BBHelperReplacementsNode.transientStateStart,
                           BBHelperReplacementsNode.transientStateEnd,
                           BBHelperReplacementsNode.nodeStart,
                           false,
                           false,
                           ConsistencyLevel.ONE,
                           ConsistencyLevel.ALL);
    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void hostReplacementFailureQuorumReadAndWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNodeFailure.reset();
        runReplacementTest(cassandraTestContext,
                           BBHelperReplacementsNodeFailure::install,
                           BBHelperReplacementsNodeFailure.transientStateStart,
                           BBHelperReplacementsNodeFailure.transientStateEnd,
                           BBHelperReplacementsNodeFailure.nodeStart,
                           false,
                           true,
                           ConsistencyLevel.QUORUM,
                           ConsistencyLevel.QUORUM);

    }

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void hostReplacementFailureOneReadAllWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNodeFailure.reset();
        runReplacementTest(cassandraTestContext,
                           BBHelperReplacementsNodeFailure::install,
                           BBHelperReplacementsNodeFailure.transientStateStart,
                           BBHelperReplacementsNodeFailure.transientStateEnd,
                           BBHelperReplacementsNodeFailure.nodeStart,
                           false,
                           true,
                           ConsistencyLevel.ONE,
                           ConsistencyLevel.ALL);

    }

    /**
     * ByteBuddy helper for a single node replacement
     */
    @Shared
    public static class BBHelperReplacementsNode
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a replacement node
            // We intercept the bootstrap of the replacement (6th) node to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsNode.class))
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
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }

    /**
     * ByteBuddy helper for a single node replacement
     */
    @Shared
    public static class BBHelperReplacementsNodeFailure
    {
        // Additional latch used here to sequentially start the 2 new nodes to isolate the loading
        // of the shared Cassandra system property REPLACE_ADDRESS_FIRST_BOOT across instances
        static CountDownLatch nodeStart = new CountDownLatch(1);
        static CountDownLatch transientStateStart = new CountDownLatch(1);
        static CountDownLatch transientStateEnd = new CountDownLatch(1);

        public static void install(ClassLoader cl, Integer nodeNumber)
        {
            // Test case involves 5 node cluster with a replacement node
            // We intercept the bootstrap of the replacement (6th) node to validate token ranges
            if (nodeNumber == 6)
            {
                TypePool typePool = TypePool.Default.of(cl);
                TypeDescription description = typePool.describe("org.apache.cassandra.service.StorageService")
                                                      .resolve();
                new ByteBuddy().rebase(description, ClassFileLocator.ForClassLoader.of(cl))
                               .method(named("bootstrap").and(takesArguments(2)))
                               .intercept(MethodDelegation.to(BBHelperReplacementsNodeFailure.class))
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
            nodeStart.countDown();
            // trigger bootstrap start and wait until bootstrap is ready from test
            transientStateStart.countDown();
            Uninterruptibles.awaitUninterruptibly(transientStateEnd);
            throw new UnsupportedOperationException("Simulated failure");
            // return result;
        }

        public static void reset()
        {
            nodeStart = new CountDownLatch(1);
            transientStateStart = new CountDownLatch(1);
            transientStateEnd = new CountDownLatch(1);
        }
    }
}

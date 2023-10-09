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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;
import org.apache.cassandra.utils.Shared;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.HostReplacementTest.BBHelperReplacementsNode.nodeStart;
import static org.apache.cassandra.HostReplacementTest.BBHelperReplacementsNode.transientStateEnd;
import static org.apache.cassandra.HostReplacementTest.BBHelperReplacementsNode.transientStateStart;
import static org.assertj.core.api.Assertions.assertThat;

public class HostReplacementTest extends ResiliencyTestBase
{

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void nodeReplacementDuringBulkWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNode.reset();

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);
        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperReplacementsNode::install);
            builder.withTokenSupplier(tokenSupplier);
        });

        List<IUpgradeableInstance> nodesToRemove = Collections.singletonList(cluster.get(cluster.size()));
        QualifiedTableName schema = null;
        List<IUpgradeableInstance> newNodes;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            List<String> removedNodeAddresses = nodesToRemove.stream()
                                                             .map(n ->
                                                                  n.config()
                                                                   .broadcastAddress()
                                                                   .getAddress()
                                                                   .getHostAddress())
                                                             .collect(Collectors.toList());

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<String> removedNodeTokens = ring.stream()
                                                 .filter(i -> removedNodeAddresses.contains(i.getAddress()))
                                                 .map(ClusterUtils.RingInstanceDetails::getToken)
                                                 .collect(Collectors.toList());

            stopNodes(seed, nodesToRemove);
            newNodes = startReplacementNodes(nodeStart, cluster, nodesToRemove);

            // Wait until replacement nodes are in JOINING state
            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);

            // Verify state of replacement nodes
            for (IUpgradeableInstance newInstance : newNodes)
            {
                ClusterUtils.awaitRingState(newInstance, newInstance, "Joining");
                ClusterUtils.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();
                Optional<ClusterUtils.RingInstanceDetails> replacementInstance = ClusterUtils.ring(seed)
                                                                                             .stream()
                                                                                             .filter(
                                                                                             i -> i.getAddress()
                                                                                                   .equals(newAddress))
                                                                                             .findFirst();
                assertThat(replacementInstance).isPresent();
                // Verify that replacement node tokens match the removed nodes
                assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
                schema = bulkWriteData();
            }
        }
        finally
        {
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transientStateEnd.countDown();
            }
        }

        ClusterUtils.awaitRingState(cluster.get(1), newNodes.get(0), "Normal");
        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.QUORUM);
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

    @CassandraIntegrationTest(nodesPerDc = 5, newNodesPerDc = 1, network = true, gossip = true, buildCluster = false)
    void nodeReplacementFailureDuringBulkWrite(ConfigurableCassandraTestContext cassandraTestContext) throws Exception
    {
        BBHelperReplacementsNodeFailure.reset();

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);
        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(BBHelperReplacementsNodeFailure::install);
            builder.withTokenSupplier(tokenSupplier);
        });

        List<IUpgradeableInstance> nodesToRemove = Collections.singletonList(cluster.get(cluster.size()));
        List<String> removedNodeAddresses = nodesToRemove.stream()
                                                         .map(n ->
                                                              n.config()
                                                               .broadcastAddress()
                                                               .getAddress()
                                                               .getHostAddress())
                                                         .collect(Collectors.toList());
        QualifiedTableName schema = null;
        List<IUpgradeableInstance> newNodes;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<String> removedNodeTokens = ring.stream()
                                                 .filter(i -> removedNodeAddresses.contains(i.getAddress()))
                                                 .map(ClusterUtils.RingInstanceDetails::getToken)
                                                 .collect(Collectors.toList());

            stopNodes(seed, nodesToRemove);
            newNodes = startReplacementNodes(BBHelperReplacementsNodeFailure.nodeStart, cluster, nodesToRemove);

            // Wait until replacement nodes are in JOINING state
            Uninterruptibles.awaitUninterruptibly(BBHelperReplacementsNodeFailure.transientStateStart, 2, TimeUnit.MINUTES);

            // Verify state of replacement nodes
            for (IUpgradeableInstance newInstance : newNodes)
            {
                ClusterUtils.awaitRingState(newInstance, newInstance, "Joining");
                ClusterUtils.awaitGossipStatus(newInstance, newInstance, "BOOT_REPLACE");

                String newAddress = newInstance.config().broadcastAddress().getAddress().getHostAddress();

                Optional<ClusterUtils.RingInstanceDetails> replacementInstance =
                getMatchingInstanceFromRing(seed, newAddress);
                assertThat(replacementInstance).isPresent();
                // Verify that replacement node tokens match the removed nodes
                assertThat(removedNodeTokens).contains(replacementInstance.get().getToken());
                schema = bulkWriteData();
            }
        }
        finally
        {
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                BBHelperReplacementsNodeFailure.transientStateEnd.countDown();
            }
        }

        Session session = maybeGetSession();
        validateData(session, schema.tableName(), ConsistencyLevel.QUORUM);

        Optional<ClusterUtils.RingInstanceDetails> replacementNode =
        getMatchingInstanceFromRing(cluster.get(1), newNodes.get(0).broadcastAddress().getAddress().getHostAddress());
        // Validate that the replacement node did not succeed in joining (if still visible in ring)
        if (replacementNode.isPresent())
        {
            assertThat(replacementNode.get().getState()).isNotEqualTo("Normal");
        }

        Optional<ClusterUtils.RingInstanceDetails> removedNode =
        getMatchingInstanceFromRing(cluster.get(1), removedNodeAddresses.get(0));
        // Validate that the removed node is "Down" (if still visible in ring)
        if (removedNode.isPresent())
        {
            assertThat(removedNode.get().getStatus()).isEqualTo("Down");
        }
    }

    private Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IUpgradeableInstance seed, String newAddress)
    {
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(
                           i -> i.getAddress()
                                 .equals(newAddress))
                           .findFirst();
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

    private List<IUpgradeableInstance> startReplacementNodes(CountDownLatch nodeStart,
                                                             UpgradeableCluster cluster,
                                                             List<IUpgradeableInstance> nodesToRemove)
    {
        List<IUpgradeableInstance> newNodes = new ArrayList<>();
        // Launch replacements nodes with the config of the removed nodes
        for (IUpgradeableInstance removed : nodesToRemove)
        {
            // Add new instance for each removed instance as a replacement of its owned token
            IInstanceConfig removedConfig = removed.config();
            String remAddress = removedConfig.broadcastAddress().getAddress().getHostAddress();
            int remPort = removedConfig.getInt("storage_port");
            IUpgradeableInstance replacement =
            ClusterUtils.addInstance(cluster, removedConfig,
                                     c -> {
                                         c.set("auto_bootstrap", true);
                                         // explicitly DOES NOT set instances that failed startup as "shutdown"
                                         // so subsequent attempts to shut down the instance are honored
                                         c.set("dtest.api.startup.failure_as_shutdown", false);
                                         c.with(Feature.GOSSIP,
                                                Feature.JMX,
                                                Feature.NATIVE_PROTOCOL);
                                     });

            new Thread(() -> ClusterUtils.start(replacement, (properties) -> {
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SKIP_SCHEMA_CHECK, true);
                properties.set(CassandraRelevantProperties.BOOTSTRAP_SCHEMA_DELAY_MS,
                               TimeUnit.SECONDS.toMillis(10L));
                properties.with("cassandra.broadcast_interval_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(30L)));
                properties.with("cassandra.ring_delay_ms",
                                Long.toString(TimeUnit.SECONDS.toMillis(10L)));
                // This property tells cassandra that this new instance is replacing the node with
                // address remAddress and port remPort
                properties.with("cassandra.replace_address_first_boot", remAddress + ":" + remPort);
            })).start();

            Uninterruptibles.awaitUninterruptibly(nodeStart, 2, TimeUnit.MINUTES);
            newNodes.add(replacement);
        }
        return newNodes;
    }

    private void stopNodes(IUpgradeableInstance seed, List<IUpgradeableInstance> removedNodes)
    {
        for (IUpgradeableInstance nodeToRemove : removedNodes)
        {
            ClusterUtils.stopUnchecked(nodeToRemove);
            String remAddress = nodeToRemove.config().broadcastAddress().getAddress().getHostAddress();

            List<ClusterUtils.RingInstanceDetails> ring = ClusterUtils.ring(seed);
            List<ClusterUtils.RingInstanceDetails> match = ring.stream()
                                                               .filter((d) -> d.getAddress().equals(remAddress))
                                                               .collect(Collectors.toList());
            assertThat(match.stream().anyMatch(r -> r.getStatus().equals("Down"))).isTrue();
        }
    }

}

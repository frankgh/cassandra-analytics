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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.analytics.TestTokenSupplier;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;
import org.apache.cassandra.testing.ConfigurableCassandraTestContext;

import static junit.framework.TestCase.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;

public class JoiningBaseTest extends ResiliencyTestBase
{
    void runJoiningTestScenario(CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                boolean isCrossDCKeyspace,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure)
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        QualifiedTableName table = null;
        List<IUpgradeableInstance> newInstances = new ArrayList<>();
        try
        {
            IUpgradeableInstance seed = cluster.get(1);

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

            Uninterruptibles.awaitUninterruptibly(transientStateStart, 2, TimeUnit.MINUTES);

            for (IUpgradeableInstance newInstance : newInstances)
            {
                ClusterUtils.awaitRingState(seed, newInstance, "Joining");
            }

            if (!isFailure)
            {
                table = bulkWriteData(isCrossDCKeyspace, writeCL);
                assertNotNull(table);
                Session session = maybeGetSession();
                validateData(session, table.tableName(), readCL);
            }
        }
        finally
        {
            if (!isFailure)
            {
                for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
                {
                    transientStateEnd.countDown();
                }
            }
        }

        /**
         * We fail after triggering bulk writer job. We want to make sure that read validation clears if the
         * if failure happens in transient node
         */
        if (isFailure)
        {
            table = bulkWriteData(isCrossDCKeyspace, writeCL);
            for (int i = 0; i < (annotation.newNodesPerDc() * annotation.numDcs()); i++)
            {
                transientStateEnd.countDown();
            }
            assertNotNull(table);
            Session session = maybeGetSession();
            validateData(session, table.tableName(), readCL);

            for (IUpgradeableInstance joiningNode : newInstances)
            {
                Optional<ClusterUtils.RingInstanceDetails> joiningNodeDetails = getMatchingInstanceFromRing(cluster.get(1), joiningNode.broadcastAddress()
                                                                                                                                       .getAddress()
                                                                                                                                       .getHostAddress());
                joiningNodeDetails.ifPresent(ringInstanceDetails -> assertThat(ringInstanceDetails.getState()).isNotEqualTo("Normal"));
            }
        }
    }

    void runJoiningTestScenario(ConfigurableCassandraTestContext cassandraTestContext,
                                BiConsumer<ClassLoader, Integer> instanceInitializer,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure)
    throws Exception
    {

        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        TokenSupplier tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(annotation.nodesPerDc(),
                                                                                annotation.newNodesPerDc(),
                                                                                annotation.numDcs(),
                                                                                1);

        UpgradeableCluster cluster = cassandraTestContext.configureAndStartCluster(builder -> {
            builder.withInstanceInitializer(instanceInitializer);
            builder.withTokenSupplier(tokenSupplier);
        });

        runJoiningTestScenario(transientStateStart,
                               transientStateEnd,
                               cluster,
                               true,
                               readCL,
                               writeCL,
                               isFailure);
    }

    private Optional<ClusterUtils.RingInstanceDetails> getMatchingInstanceFromRing(IUpgradeableInstance seed,
                                                                                   String ipAddress)
    {
        return ClusterUtils.ring(seed)
                           .stream()
                           .filter(i -> i.getAddress().equals(ipAddress))
                           .findFirst();
    }
}

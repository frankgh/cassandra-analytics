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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import o.a.c.analytics.sidecar.shaded.testing.common.data.QualifiedTableName;
import org.apache.cassandra.analytics.ResiliencyTestBase;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

class LeavingBaseTest extends ResiliencyTestBase
{
    void runLeavingTestScenario(int leavingNodesPerDC,
                                CountDownLatch transitioningStateStart,
                                CountDownLatch transitioningStateEnd,
                                UpgradeableCluster cluster,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure) throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        QualifiedTableName table;
        List<IUpgradeableInstance> leavingNodes;
        Map<IUpgradeableInstance, Set<String>> expectedInstanceData;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            leavingNodes = decommissionNodes(cluster, leavingNodesPerDC, annotation.numDcs());

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transitioningStateStart);

            leavingNodes.forEach(instance -> ClusterUtils.awaitRingState(seed, instance, "Leaving"));
            table = bulkWriteData(writeCL);

            List<IUpgradeableInstance> instances = cluster.stream().collect(Collectors.toList());
            expectedInstanceData = generateExpectedInstanceData(cluster, leavingNodes);
        }
        finally
        {
            for (int i = 0; i < leavingNodesPerDC; i++)
            {
                transitioningStateEnd.countDown();
            }
        }

        assertNotNull(table);
        validateData(table.tableName(), readCL);
        validateNodeSpecificData(table, expectedInstanceData, false);

        // For tests that involve LEAVE failures, we validate that the leaving nodes are part of the cluster
        if (isFailure)
        {
            // check leave node are part of cluster when leave fails
            assertTrue(areLeavingNodesPartOfCluster(cluster.get(1), leavingNodes));
        }
    }

    private List<IUpgradeableInstance> decommissionNodes(UpgradeableCluster cluster, int leavingNodesPerDC, int numDcs)
    {
        List<IUpgradeableInstance> leavingNodes = new ArrayList<>();
        for (int i = 0; i < leavingNodesPerDC * numDcs; i++)
        {
            IUpgradeableInstance node = cluster.get(cluster.size() - i);
            new Thread(() -> node.nodetoolResult("decommission").asserts().success()).start();
            leavingNodes.add(node);
        }

        return leavingNodes;
    }

    private boolean areLeavingNodesPartOfCluster(IUpgradeableInstance seed, List<IUpgradeableInstance> leavingNodes)
    {
        Set<String> leavingAddresses = leavingNodes.stream()
                                                   .map(node -> node.broadcastAddress().getAddress().getHostAddress())
                                                   .collect(Collectors.toSet());
        ClusterUtils.ring(seed).forEach(i -> leavingAddresses.remove(i.getAddress()));
        return leavingAddresses.isEmpty();
    }
}

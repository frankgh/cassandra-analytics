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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import io.vertx.junit5.VertxTestContext;
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
    void runLeavingTestScenario(VertxTestContext context,
                                int leavingNodesPerDC,
                                CountDownLatch transientStateStart,
                                CountDownLatch transientStateEnd,
                                UpgradeableCluster cluster,
                                ConsistencyLevel readCL,
                                ConsistencyLevel writeCL,
                                boolean isFailure) throws Exception
    {
        CassandraIntegrationTest annotation = sidecarTestContext.cassandraTestContext().annotation;
        QualifiedTableName table;
        List<IUpgradeableInstance> leavingNodes;
        try
        {
            IUpgradeableInstance seed = cluster.get(1);
            leavingNodes = stopNodes(cluster, leavingNodesPerDC, annotation.numDcs());

            // Wait until nodes have reached expected state
            Uninterruptibles.awaitUninterruptibly(transientStateStart);

            leavingNodes.forEach(instance -> ClusterUtils.awaitRingState(seed, instance, "Leaving"));
            table = bulkWriteData(annotation.numDcs() > 1, writeCL);
        }
        finally
        {
            for (int i = 0; i < leavingNodesPerDC; i++)
            {
                transientStateEnd.countDown();
            }
        }

        Session session = maybeGetSession();
        assertNotNull(table);
        validateData(session, table.tableName(), readCL);

        // For tests that involve LEAVE failures, we validate that the leaving nodes are part of the cluster
        if (isFailure)
        {
            // check leave node are part of cluster when leave fails
            assertTrue(areLeavingNodesPartOfCluster(cluster.get(1), leavingNodes));
            context.completeNow();
        }
        else
        {
            validateTransientNodeData(context, table, leavingNodes);
        }
    }

    private List<IUpgradeableInstance> stopNodes(UpgradeableCluster cluster, int leavingNodesPerDC, int numDcs)
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

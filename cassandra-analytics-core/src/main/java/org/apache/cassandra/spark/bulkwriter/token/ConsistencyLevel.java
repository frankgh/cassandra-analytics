/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.bulkwriter.token;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;

public interface ConsistencyLevel
{
    boolean isLocal();

    Logger LOGGER = LoggerFactory.getLogger(ConsistencyLevel.class);

    /**
     * Checks if the consistency guarantees are maintained, given the failed, blocked and replacing instances, consistency-level and the replication-factor.
     * <pre>
     * - QUORUM based consistency levels check for quorum using the write-replica-set (instead of RF) as they include healthy and pending nodes.
     *   This is done to ensure that writes go to a quorum of healthy nodes while accounting for potential failure in pending nodes becoming healthy.
     * - ONE and TWO consistency guarantees are maintained by ensuring that the failures leave us with at-least the corresponding healthy
     *   (and non-pending) nodes.
     *
     *   For both the above cases, blocked instances are also considered as failures while performing consistency checks.
     *   Write replicas are adjusted to exclude replacement nodes for consistency checks, if we have replacement nodes that are not among the failed instances.
     *   This is to ensure that we are writing to sufficient non-replacement nodes as replacements can potentially fail leaving us with fewer nodes.
     * </pre>
     */
    boolean checkConsistency(CassandraRing ring,
                             TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                             Collection<? extends CassandraInstance> failedInsts,
                             String localDC);

    // Check if successful writes forms quorum of non-replacing nodes - N/A as quorum is if there are no failures/blocked
    enum CL implements ConsistencyLevel
    {
        ALL
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                return failedInsts.isEmpty() && tokenRangeMapping.getBlockedInstances().isEmpty();
            }
        },

        EACH_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Preconditions.checkArgument(ring.getReplicationFactor().getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "EACH_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                for (final String dataCenter : ring.getReplicationFactor().getOptions().keySet())
                {
                    Set<String> dcReplacingInstances = tokenRangeMapping.getReplacementInstances(dataCenter);
                    Set<String> dcFailedInstances = failedInsts.stream()
                                                               .filter(inst -> inst.getDataCenter().matches(dataCenter))
                                                               .map(CassandraInstance::getIpAddress)
                                                               .collect(Collectors.toSet());

                    final long dcWriteReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(dataCenter),
                                                                                             dcReplacingInstances,
                                                                                             dcFailedInstances);

                    long dcBlockedInstancesCount = tokenRangeMapping.getBlockedInstances(dataCenter).size();

                    if ((dcFailedInstances.size() + dcBlockedInstancesCount) > (dcWriteReplicaCount - (dcWriteReplicaCount / 2 + 1)))
                    {
                        return false;
                    }
                }

                return true;
            }
        },
        QUORUM
        {
            // Keyspaces exist with RF 1 or 2
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Set<String> replacingInstances = tokenRangeMapping.getReplacementInstances();
                Set<String> failedInstanceIPs = failedInsts.stream().map(CassandraInstance::getIpAddress).collect(Collectors.toSet());
                final long writeReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(),
                                                                                       replacingInstances,
                                                                                       failedInstanceIPs);
                return (failedInsts.size() + tokenRangeMapping.getBlockedInstances().size()) <= (writeReplicaCount - (writeReplicaCount / 2 + 1));
            }
        },
        LOCAL_QUORUM
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Preconditions.checkArgument(ring.getReplicationFactor().getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "LOCAL_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                Set<String> dcFailedInstances = failedInsts.stream()
                                                           .filter(inst -> inst.getDataCenter().matches(localDC))
                                                           .map(CassandraInstance::getIpAddress)
                                                           .collect(Collectors.toSet());

                long dcBlockedInstancesCount = tokenRangeMapping.getBlockedInstances(localDC).size();

                Set<String> dcReplacingInstances = tokenRangeMapping.getReplacementInstances(localDC);
                final long dcWriteReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(localDC),
                                                                                         dcReplacingInstances,
                                                                                         dcFailedInstances);
                return (dcFailedInstances.size() + dcBlockedInstancesCount) <= (dcWriteReplicaCount - (dcWriteReplicaCount / 2 + 1));
            }
        },
        ONE
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Set<String> replacingInstances = tokenRangeMapping.getReplacementInstances();

                Set<String> failedInstanceIPs = failedInsts.stream().map(CassandraInstance::getIpAddress).collect(Collectors.toSet());

                final long writeReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(),
                                                                                       replacingInstances,
                                                                                       failedInstanceIPs);
                return (failedInsts.size() + tokenRangeMapping.getBlockedInstances().size())
                       <= (writeReplicaCount - tokenRangeMapping.getPendingReplicas().size() - 1);
            }
        },
        TWO
        {
            @Override
            public boolean isLocal()
            {
                return false;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Set<String> replacingInstances = tokenRangeMapping.getReplacementInstances();
                Set<String> failedInstanceIPs = failedInsts.stream().map(CassandraInstance::getIpAddress).collect(Collectors.toSet());

                final long writeReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(),
                                                                                       replacingInstances,
                                                                                       failedInstanceIPs);
                return (failedInsts.size() + tokenRangeMapping.getBlockedInstances().size())
                       <= (writeReplicaCount - tokenRangeMapping.getPendingReplicas().size() - 2);
            }
        },
        LOCAL_ONE
        {
            @Override
            public boolean isLocal()
            {
                return true;
            }

            @Override
            public boolean checkConsistency(final CassandraRing ring,
                                            final TokenRangeMapping<? extends CassandraInstance> tokenRangeMapping,
                                            final Collection<? extends CassandraInstance> failedInsts,
                                            final String localDC)
            {
                Preconditions.checkArgument(ring.getReplicationFactor().getReplicationStrategy() != ReplicationFactor.ReplicationStrategy.SimpleStrategy,
                                            "LOCAL_QUORUM doesn't make sense for SimpleStrategy keyspaces");

                Set<String> dcFailedInstances = failedInsts.stream()
                                                           .filter(inst -> inst.getDataCenter().matches(localDC))
                                                           .map(CassandraInstance::getIpAddress)
                                                           .collect(Collectors.toSet());
                long dcBlockedInstancesCount = tokenRangeMapping.getBlockedInstances(localDC).size();
                Set<String> dcReplacingInstances = tokenRangeMapping.getReplacementInstances(localDC);
                // TODO: Ensure quorum in non-pending replica-set while writing
                final long dcWriteReplicaCount = maybeUpdateWriteReplicasForReplacements(tokenRangeMapping.getWriteReplicas(localDC),
                                                                                         dcReplacingInstances,
                                                                                         dcFailedInstances);
                return (dcFailedInstances.size() + dcBlockedInstancesCount) <= (dcWriteReplicaCount - tokenRangeMapping.getPendingReplicas(localDC).size() - 1);
            }
        };

        private static long maybeUpdateWriteReplicasForReplacements(Set<String> writeReplicas, Set<String> replacingInstances, Set<String> failedInstances)
        {
            // Exclude replacement nodes from write-replicas if replacements are NOT among failed instances
            if (!replacingInstances.isEmpty() && Collections.disjoint(failedInstances, replacingInstances))
            {

                return writeReplicas.stream().filter(r -> !replacingInstances.contains(r)).count();
            }
            return writeReplicas.size();
        }
    }
}

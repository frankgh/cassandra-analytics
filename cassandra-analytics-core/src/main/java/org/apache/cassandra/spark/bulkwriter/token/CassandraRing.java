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

import java.io.NotSerializableException;
import java.io.Serializable;

import org.apache.cassandra.spark.bulkwriter.RingInstance;
import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * CassandraRing is designed to have one unique way of handling Cassandra token/topology information across all Cassandra
 * tooling. This class is made Serializable so it's easy to use it from Hadoop/Spark. As Cassandra token ranges are
 * dependent on Replication strategy, ring makes sense for a specific keyspace only. It is made to be immutable for the
 * sake of simplicity.
 * <p>
 * Token ranges are calculated assuming Cassandra racks are not being used, but controlled by assigning tokens properly.
 * This is the case for now with managed clusters. We should re-think about this if it changes in future.
 * <p>
 * Where the token information is coming from is not scope of this class. One easy way is to getting it from CasCluster,
 * which might not be very accurate during cluster grow/shrink due to block lists and hidden instances. We could create
 * ring based on "nodetool ring" coming from Cassandra. CassandraRingBuilder in mgrclient creates ring from
 * ring of node in NORMAL state.
 * <p>
 *
 * <p>
 * {@link CassandraInstance} doesn't extend {@link Serializable}. So, it is possible to use {@link CassandraRing} with
 * non serializable {@link CassandraInstance}. If we try to serialize ring with those instances, you will see
 * {@link NotSerializableException}. One of the serializable implementation of {@link CassandraInstance} is at
 * {@link RingInstance}.
 */
public class CassandraRing implements Serializable
{
    private final Partitioner partitioner;
    private final String keyspace;
    private transient ReplicationFactor replicationFactor;

    public CassandraRing(final Partitioner partitioner,
                         final String keyspace,
                         final ReplicationFactor replicationFactor)
    {
        this.partitioner = partitioner;
        this.keyspace = keyspace;
        this.replicationFactor = replicationFactor;
    }

    public String getKeyspace()
    {
        return keyspace;
    }

    public ReplicationFactor getReplicationFactor()
    {
        return replicationFactor;
    }

    public Partitioner getPartitioner()
    {
        return partitioner;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
        {
            return true;
        }
        if (other == null || getClass() != other.getClass())
        {
            return false;
        }

        final CassandraRing that = (CassandraRing) other;

        if (partitioner != that.partitioner)
        {
            return false;
        }
        if (!keyspace.equals(that.keyspace))
        {
            return false;
        }
        if (replicationFactor.getReplicationStrategy() != that.replicationFactor.getReplicationStrategy()
            || !replicationFactor.getOptions().equals(that.replicationFactor.getOptions()))
        {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = partitioner.hashCode();
        result = 31 * result + keyspace.hashCode();
        result = 31 * result + replicationFactor.hashCode();
        return result;
    }
}

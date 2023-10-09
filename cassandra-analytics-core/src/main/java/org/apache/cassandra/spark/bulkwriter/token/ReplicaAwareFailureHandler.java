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

import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;

import org.apache.cassandra.spark.common.model.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

public class ReplicaAwareFailureHandler<Instance extends CassandraInstance>
{
    private final RangeMap<BigInteger, Multimap<Instance, String>> failedRangesMap = TreeRangeMap.create();

    public ReplicaAwareFailureHandler(Partitioner partitioner)
    {
        failedRangesMap.put(Range.closed(partitioner.minToken(), partitioner.maxToken()), ArrayListMultimap.create());
    }

    /**
     * Adds a new token range as a failed token range, with errors on given instance.
     * <p>
     * It's guaranteed that failedRangesMap has overlapping ranges for the range we are trying to insert (Check
     * constructor, we are adding complete ring first).
     * <p>
     * So the scheme is to get list of overlapping ranges first. For each overlapping range get the failure map.
     * Make a copy of the map and add new failure to this map. It's important we make the copy and not use the
     * one returned from failedRangesMap map. As our range could be overlapping partially and the map could be used
     * by other range.
     *
     * @param tokenRange  the range which failed
     * @param casInstance the instance on which the range failed
     * @param errMessage  the error that occurred for this particular range/instance pair
     */
    public void addFailure(Range<BigInteger> tokenRange, Instance casInstance, String errMessage)
    {
        RangeMap<BigInteger, Multimap<Instance, String>> overlappingFailures = failedRangesMap.subRangeMap(tokenRange);
        RangeMap<BigInteger, Multimap<Instance, String>> mappingsToAdd = TreeRangeMap.create();

        for (Map.Entry<Range<BigInteger>, Multimap<Instance, String>> entry : overlappingFailures.asMapOfRanges().entrySet())
        {
            Multimap<Instance, String> newErrorMap = ArrayListMultimap.create(entry.getValue());
            newErrorMap.put(casInstance, errMessage);
            mappingsToAdd.put(entry.getKey(), newErrorMap);
        }
        failedRangesMap.putAll(mappingsToAdd);
    }

    public List<Instance> getFailedInstances()
    {
        return failedRangesMap.asMapOfRanges().values()
                              .stream()
                              .map(Multimap::keySet)
                              .flatMap(Collection::stream)
                              .collect(Collectors.toList());
    }

    /**
     * Given the number of failed instances for each token range, validates if the consistency guarantees are maintained
     * for the size of the ring and the consistency level.
     * @return list of failed entries for token ranges that break consistency. This should ideally be empty for a
     * successful operation.
     */
    public Collection<AbstractMap.SimpleEntry<Range<BigInteger>, Multimap<Instance, String>>>
    getFailedEntries(final CassandraRing ring,
                     final TokenRangeMapping<Instance> tokenRangeMapping,
                     final ConsistencyLevel cl,
                     final String localDC)
    {

        final List<AbstractMap.SimpleEntry<Range<BigInteger>, Multimap<Instance, String>>> failedEntries =
        new ArrayList<>();

        for (final Map.Entry<Range<BigInteger>, Multimap<Instance, String>> failedRangeEntry
        : failedRangesMap.asMapOfRanges().entrySet())
        {
            final Multimap<Instance, String> errorMap = failedRangeEntry.getValue();
            final Collection<Instance> failedInstances = errorMap.keySet()
                                                                 .stream()
                                                                 .filter(inst ->
                                                                         !errorMap.get(inst).isEmpty())
                                                                 .collect(Collectors.toList());
            if (!cl.checkConsistency(ring, tokenRangeMapping, failedInstances, localDC))
            {
                failedEntries.add(new AbstractMap.SimpleEntry<>(failedRangeEntry.getKey(),
                                                                failedRangeEntry.getValue()));
            }
        }

        return failedEntries;
    }

    public boolean hasFailed(final CassandraRing ring,
                             final TokenRangeMapping<Instance> tokenRange,
                             final ConsistencyLevel cl,
                             final String localDC)
    {
        return !getFailedEntries(ring, tokenRange, cl, localDC).isEmpty();
    }
}

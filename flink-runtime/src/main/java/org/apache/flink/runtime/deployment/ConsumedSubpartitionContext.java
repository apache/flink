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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/*
 * Helper class used to track and manage the relationships between shuffle descriptors and their
 * associated subpartitions.
 */
class ConsumedSubpartitionContext implements Serializable {
    private static final long serialVersionUID = 1L;

    /** The number of consumed shuffle descriptors. */
    private final int numConsumedShuffleDescriptors;

    /**
     * A mapping between ranges of consumed shuffle descriptors and their corresponding subpartition
     * ranges.
     */
    private final Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap;

    private ConsumedSubpartitionContext(
            int numConsumedShuffleDescriptors,
            Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap) {
        this.numConsumedShuffleDescriptors = numConsumedShuffleDescriptors;
        this.consumedShuffleDescriptorToSubpartitionRangeMap =
                checkNotNull(consumedShuffleDescriptorToSubpartitionRangeMap);
    }

    public int getNumConsumedShuffleDescriptors() {
        return numConsumedShuffleDescriptors;
    }

    public Collection<IndexRange> getConsumedShuffleDescriptorRanges() {
        return Collections.unmodifiableCollection(
                consumedShuffleDescriptorToSubpartitionRangeMap.keySet());
    }

    public IndexRange getConsumedSubpartitionRange(Integer shuffleDescriptorIndex) {
        for (Map.Entry<IndexRange, IndexRange> entry :
                consumedShuffleDescriptorToSubpartitionRangeMap.entrySet()) {
            IndexRange shuffleDescriptorRange = entry.getKey();
            if (shuffleDescriptorIndex >= shuffleDescriptorRange.getStartIndex()
                    && shuffleDescriptorIndex <= shuffleDescriptorRange.getEndIndex()) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException(
                "Cannot find consumed subpartition range for shuffle descriptor index "
                        + shuffleDescriptorIndex);
    }

    /**
     * Builds a {@link ConsumedSubpartitionContext} based on the provided inputs.
     *
     * <p>Note: The construction is based on subscribing to consecutive subpartitions of the same
     * partition. If this assumption is violated, the calculation of the number of consumed
     * ShuffleDescriptors will be inaccurate.
     *
     * @param consumedSubpartitionGroups a mapping of consumed partition index ranges to
     *     subpartition ranges.
     * @param consumedResultPartitions an iterator of {@link IntermediateResultPartitionID} for the
     *     consumed result partitions.
     * @param partitions all partition ids of consumed {@link IntermediateResult}.
     * @return a {@link ConsumedSubpartitionContext} instance constructed from the input parameters.
     */
    public static ConsumedSubpartitionContext buildConsumedSubpartitionContext(
            Map<IndexRange, IndexRange> consumedSubpartitionGroups,
            Iterator<IntermediateResultPartitionID> consumedResultPartitions,
            IntermediateResultPartitionID[] partitions) {
        Map<IntermediateResultPartitionID, Integer> partitionIdToShuffleDescriptorIndexMap =
                new HashMap<>();
        while (consumedResultPartitions.hasNext()) {
            IntermediateResultPartitionID partitionId = consumedResultPartitions.next();
            partitionIdToShuffleDescriptorIndexMap.put(
                    partitionId, partitionIdToShuffleDescriptorIndexMap.size());
        }

        Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap =
                new LinkedHashMap<>();
        int numConsumedShuffleDescriptors = 0;
        for (Map.Entry<IndexRange, IndexRange> entry : consumedSubpartitionGroups.entrySet()) {
            IndexRange partitionRange = entry.getKey();
            IndexRange subpartitionRange = entry.getValue();
            IndexRange shuffleDescriptorRange =
                    new IndexRange(
                            partitionIdToShuffleDescriptorIndexMap.get(
                                    partitions[partitionRange.getStartIndex()]),
                            partitionIdToShuffleDescriptorIndexMap.get(
                                    partitions[partitionRange.getEndIndex()]));
            checkState(partitionRange.size() == shuffleDescriptorRange.size());
            numConsumedShuffleDescriptors += shuffleDescriptorRange.size();
            consumedShuffleDescriptorToSubpartitionRangeMap.put(
                    shuffleDescriptorRange, subpartitionRange);
        }
        return new ConsumedSubpartitionContext(
                numConsumedShuffleDescriptors, consumedShuffleDescriptorToSubpartitionRangeMap);
    }

    /**
     * Builds a {@link ConsumedSubpartitionContext} using a given number of consumed shuffle
     * descriptors and a single {@link IndexRange} representing the consumed subpartition range.
     *
     * <p>Note: This method is designed as a compatibility method. It assumes that the task will
     * subscribe to all shuffle descriptors and to the same subpartitions for every descriptor.
     *
     * @param numConsumedShuffleDescriptors the total number of consumed shuffle descriptors; must
     *     be greater than 0.
     * @param consumedSubpartitionRange the range of consumed subpartitions.
     * @return a {@link ConsumedSubpartitionContext} instance constructed from the input parameters.
     */
    public static ConsumedSubpartitionContext buildConsumedSubpartitionContext(
            int numConsumedShuffleDescriptors, IndexRange consumedSubpartitionRange) {
        checkState(numConsumedShuffleDescriptors > 0);
        return new ConsumedSubpartitionContext(
                numConsumedShuffleDescriptors,
                Map.of(
                        new IndexRange(0, numConsumedShuffleDescriptors - 1),
                        consumedSubpartitionRange));
    }

    @Override
    public String toString() {
        return String.format(
                "ConsumedSubpartitionContext [num consumed shuffle descriptors: %s, "
                        + "consumed shuffle descriptors to subpartition range: %s]",
                numConsumedShuffleDescriptors, consumedShuffleDescriptorToSubpartitionRangeMap);
    }
}

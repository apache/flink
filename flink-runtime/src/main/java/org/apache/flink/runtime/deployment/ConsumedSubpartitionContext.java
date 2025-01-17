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
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
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
     *
     * <p>For ALL_TO_ALL, the consumed partition range to subpartition range might be like this:
     * task1: [0, 10] -> [0, 0]; task2: [0,5] -> [1,1]; task3: [6,10] -> [1,1], [0,10] -> [2,2].
     * Since ALL_TO_ALL shares the same set of shuffle descriptors, the index mapping from partition
     * rang to shuffle descriptor range is: [0,10]->[0,10]. Finally, the shuffle descriptor range to
     * subpartition range mappings are: task1: [0, 10] -> [0, 0]; task2: [0,5] -> [1,1]; task3:
     * [6,10] -> [1,1], [0,10] -> [2,2].
     *
     * <p>For POINTWISE, the consumed partition range to subpartition range might be like this:
     * task1: [0, 0] -> [0, 10]; task2: [1, 1] -> [0,5]; task3: [1,1] -> [6,10], [2,10]->[0,10]. The
     * mappings from partition rang to shuffle descriptor range for each task are: task1: [0,0] ->
     * [0,0]; task2: [1,1] -> [0,0]; task3: [1,1] -> [0,0], [2,10] -> [1,9]. Finally, the shuffle
     * descriptor range to subpartition range mappings are: task1: [0,0] -> [0,10]; task2: [0,0] ->
     * [0,5]; task3: [0,0] -> [6,10], [1,9] -> [0,10].
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
        // The original consumed shuffle descriptors may have overlaps, we need to deduplicate it
        // by merging.
        return Collections.unmodifiableCollection(
                mergeIndexRanges(consumedShuffleDescriptorToSubpartitionRangeMap.keySet()));
    }

    public IndexRange getConsumedSubpartitionRange(int shuffleDescriptorIndex) {
        //  For ALL_TO_ALL the consumedShuffleDescriptorToSubpartitionRange might like this:
        //  [0,10] -> [2,2], [0,5] -> [3,3], we need to find all the consumed subpartition ranges
        // and return the merged result.
        List<IndexRange> consumedSubpartitionRanges = new ArrayList<>();
        for (Map.Entry<IndexRange, IndexRange> entry :
                consumedShuffleDescriptorToSubpartitionRangeMap.entrySet()) {
            IndexRange shuffleDescriptorRange = entry.getKey();
            if (shuffleDescriptorIndex >= shuffleDescriptorRange.getStartIndex()
                    && shuffleDescriptorIndex <= shuffleDescriptorRange.getEndIndex()) {
                consumedSubpartitionRanges.add(entry.getValue());
            }
        }
        List<IndexRange> mergedConsumedSubpartitionRanges =
                mergeIndexRanges(consumedSubpartitionRanges);
        checkState(
                mergedConsumedSubpartitionRanges.size() == 1,
                "Illegal consumed subpartition range for shuffle descriptor index "
                        + shuffleDescriptorIndex);
        return mergedConsumedSubpartitionRanges.get(0);
    }

    /**
     * Builds a {@link ConsumedSubpartitionContext} based on the provided inputs.
     *
     * <p>Note: The construction is based on subscribing to consecutive subpartitions of the same
     * partition. If this assumption is violated, an exception will be thrown.
     *
     * @param consumedSubpartitionGroups a mapping of consumed partition index ranges to
     *     subpartition ranges.
     * @param consumedPartitionGroup partition group consumed by the task.
     * @param partitionIdRetriever a function that retrieves the {@link
     *     IntermediateResultPartitionID} for a given index.
     * @return a {@link ConsumedSubpartitionContext} instance constructed from the input parameters.
     */
    public static ConsumedSubpartitionContext buildConsumedSubpartitionContext(
            Map<IndexRange, IndexRange> consumedSubpartitionGroups,
            ConsumedPartitionGroup consumedPartitionGroup,
            Function<Integer, IntermediateResultPartitionID> partitionIdRetriever) {
        Map<IntermediateResultPartitionID, Integer> resultPartitionsInOrder =
                consumedPartitionGroup.getResultPartitionsInOrder();
        // If only one range is included and the index range size is the same as the number of
        // shuffle descriptors, it means that the task will subscribe to all partitions, i.e., the
        // partition range is one-to-one corresponding to the shuffle descriptors. Therefore, we can
        // directly construct the ConsumedSubpartitionContext using the subpartition range.
        if (consumedSubpartitionGroups.size() == 1
                && consumedSubpartitionGroups.keySet().iterator().next().size()
                        == resultPartitionsInOrder.size()) {
            return buildConsumedSubpartitionContext(
                    resultPartitionsInOrder.size(),
                    consumedSubpartitionGroups.values().iterator().next());
        }

        Map<IndexRange, IndexRange> consumedShuffleDescriptorToSubpartitionRangeMap =
                new LinkedHashMap<>();
        for (Map.Entry<IndexRange, IndexRange> entry : consumedSubpartitionGroups.entrySet()) {
            IndexRange partitionRange = entry.getKey();
            IndexRange subpartitionRange = entry.getValue();
            // The shuffle descriptor index is consistent with the index in resultPartitionsInOrder.
            IndexRange shuffleDescriptorRange =
                    new IndexRange(
                            resultPartitionsInOrder.get(
                                    partitionIdRetriever.apply(partitionRange.getStartIndex())),
                            resultPartitionsInOrder.get(
                                    partitionIdRetriever.apply(partitionRange.getEndIndex())));
            checkState(
                    partitionRange.size() == shuffleDescriptorRange.size()
                            && !consumedShuffleDescriptorToSubpartitionRangeMap.containsKey(
                                    shuffleDescriptorRange));
            consumedShuffleDescriptorToSubpartitionRangeMap.put(
                    shuffleDescriptorRange, subpartitionRange);
        }
        // For ALL_TO_ALL, there might be overlaps in shuffle descriptor to subpartition range map:
        // [0,10] -> [2,2], [0,5] -> [3,3], so we need to count consumed shuffle descriptors after
        // merging.
        int numConsumedShuffleDescriptors = 0;
        List<IndexRange> mergedConsumedShuffleDescriptor =
                mergeIndexRanges(consumedShuffleDescriptorToSubpartitionRangeMap.keySet());
        for (IndexRange range : mergedConsumedShuffleDescriptor) {
            numConsumedShuffleDescriptors += range.size();
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

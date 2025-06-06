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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;

/** Information of All-To-All result. */
public class AllToAllBlockingResultInfo extends AbstractBlockingResultInfo {

    private final boolean singleSubpartitionContainsAllData;

    private boolean isBroadcast;

    /**
     * Aggregated subpartition bytes, which aggregates the subpartition bytes with the same
     * subpartition index in different partitions. Note that We can aggregate them because they will
     * be consumed by the same downstream task.
     */
    @Nullable protected List<Long> aggregatedSubpartitionBytes;

    @VisibleForTesting
    AllToAllBlockingResultInfo(
            IntermediateDataSetID resultId,
            int numOfPartitions,
            int numOfSubpartitions,
            boolean isBroadcast,
            boolean singleSubpartitionContainsAllData) {
        this(
                resultId,
                numOfPartitions,
                numOfSubpartitions,
                singleSubpartitionContainsAllData,
                new HashMap<>());
        this.isBroadcast = isBroadcast;
    }

    AllToAllBlockingResultInfo(
            IntermediateDataSetID resultId,
            int numOfPartitions,
            int numOfSubpartitions,
            boolean singleSubpartitionContainsAllData,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        super(resultId, numOfPartitions, numOfSubpartitions, subpartitionBytesByPartitionIndex);
        this.singleSubpartitionContainsAllData = singleSubpartitionContainsAllData;
        this.isBroadcast = singleSubpartitionContainsAllData;
    }

    @Override
    public boolean isBroadcast() {
        return isBroadcast;
    }

    @Override
    public boolean isSingleSubpartitionContainsAllData() {
        return singleSubpartitionContainsAllData;
    }

    void setBroadcast(boolean isBroadcast) {
        this.isBroadcast = isBroadcast;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public int getNumPartitions() {
        return numOfPartitions;
    }

    @Override
    public int getNumSubpartitions(int partitionIndex) {
        return numOfSubpartitions;
    }

    @Override
    public long getNumBytesProduced() {
        checkState(
                aggregatedSubpartitionBytes != null
                        || subpartitionBytesByPartitionIndex.size() == numOfPartitions,
                "Not all partition infos are ready");

        List<Long> bytes =
                Optional.ofNullable(aggregatedSubpartitionBytes)
                        .orElse(getAggregatedSubpartitionBytesInternal());
        if (singleSubpartitionContainsAllData) {
            return bytes.get(0);
        } else {
            return bytes.stream().reduce(0L, Long::sum);
        }
    }

    @Override
    public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {
        if (aggregatedSubpartitionBytes == null) {
            super.recordPartitionInfo(partitionIndex, partitionBytes);
        }
    }

    /**
     * This method should be called when fine-grained information is no longer needed. It will
     * aggregate and clears the fine-grained subpartition bytes to reduce space usage.
     *
     * <p>Once all partitions are finished and all consumer jobVertices are initialized, we can
     * convert the subpartition bytes to aggregated value to reduce the space usage, because the
     * distribution of source splits does not affect the distribution of data consumed by downstream
     * tasks of ALL_TO_ALL edges(Hashing or Rebalancing, we do not consider rare cases such as
     * custom partitions here).
     */
    protected void onFineGrainedSubpartitionBytesNotNeeded() {
        if (subpartitionBytesByPartitionIndex.size() == numOfPartitions) {
            if (this.aggregatedSubpartitionBytes == null) {
                this.aggregatedSubpartitionBytes = getAggregatedSubpartitionBytesInternal();
            }
            this.subpartitionBytesByPartitionIndex.clear();
        }
    }

    /**
     * Aggregates the bytes of subpartitions across all partition indices without modifying the
     * existing state. This method is intended for querying purposes only.
     *
     * <p>The method computes the sum of the bytes for each subpartition across all partitions and
     * returns a list containing these summed values.
     *
     * <p>This method is needed in scenarios where aggregated results are required, but fine-grained
     * statistics should remain not aggregated. Specifically, when not all consumer vertices of this
     * result info are created or initialized, this result info could not be aggregated. And the
     * existing consumer vertices of this info still require these aggregated result for scheduling.
     *
     * @return a list of aggregated byte counts for each subpartition.
     */
    private List<Long> getAggregatedSubpartitionBytesInternal() {
        long[] aggregatedBytes = new long[numOfSubpartitions];
        subpartitionBytesByPartitionIndex
                .values()
                .forEach(
                        subpartitionBytes -> {
                            checkState(subpartitionBytes.length == numOfSubpartitions);
                            for (int i = 0; i < subpartitionBytes.length; ++i) {
                                aggregatedBytes[i] += subpartitionBytes[i];
                            }
                        });

        return Arrays.stream(aggregatedBytes).boxed().collect(Collectors.toList());
    }

    @Override
    public void resetPartitionInfo(int partitionIndex) {
        if (aggregatedSubpartitionBytes == null) {
            super.resetPartitionInfo(partitionIndex);
        }
    }

    @Override
    public long getNumBytesProduced(
            IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
        if (partitionIndexRange.getStartIndex() == 0
                && partitionIndexRange.getEndIndex() == getNumPartitions() - 1) {
            return IntStream.rangeClosed(
                            subpartitionIndexRange.getStartIndex(),
                            subpartitionIndexRange.getEndIndex())
                    .mapToLong(i -> getAggregatedSubpartitionBytes().get(i))
                    .sum();
        } else {
            return super.getNumBytesProduced(partitionIndexRange, subpartitionIndexRange);
        }
    }

    public List<Long> getAggregatedSubpartitionBytes() {
        checkState(
                aggregatedSubpartitionBytes != null
                        || subpartitionBytesByPartitionIndex.size() == numOfPartitions,
                "Not all partition infos are ready");
        if (aggregatedSubpartitionBytes == null) {
            aggregatedSubpartitionBytes = getAggregatedSubpartitionBytesInternal();
        }
        return Collections.unmodifiableList(aggregatedSubpartitionBytes);
    }
}

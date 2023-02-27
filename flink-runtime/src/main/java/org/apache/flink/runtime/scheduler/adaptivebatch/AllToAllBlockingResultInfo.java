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

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** Information of All-To-All result. */
public class AllToAllBlockingResultInfo extends AbstractBlockingResultInfo {

    private final boolean isBroadcast;

    /**
     * Aggregated subpartition bytes, which aggregates the subpartition bytes with the same
     * subpartition index in different partitions. Note that We can aggregate them because they will
     * be consumed by the same downstream task.
     */
    @Nullable private List<Long> aggregatedSubpartitionBytes;

    AllToAllBlockingResultInfo(
            IntermediateDataSetID resultId,
            int numOfPartitions,
            int numOfSubpartitions,
            boolean isBroadcast) {
        super(resultId, numOfPartitions, numOfSubpartitions);
        this.isBroadcast = isBroadcast;
    }

    @Override
    public boolean isBroadcast() {
        return isBroadcast;
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
        checkState(aggregatedSubpartitionBytes != null, "Not all partition infos are ready");
        if (isBroadcast) {
            return aggregatedSubpartitionBytes.get(0);
        } else {
            return aggregatedSubpartitionBytes.stream().reduce(0L, Long::sum);
        }
    }

    @Override
    public long getNumBytesProduced(
            IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
        checkState(aggregatedSubpartitionBytes != null, "Not all partition infos are ready");
        checkState(
                partitionIndexRange.getStartIndex() == 0
                        && partitionIndexRange.getEndIndex() == numOfPartitions - 1,
                "For All-To-All edges, the partition range should always be [0, %s).",
                numOfPartitions);
        checkState(
                subpartitionIndexRange.getEndIndex() < numOfSubpartitions,
                "Subpartition index %s is out of range.",
                subpartitionIndexRange.getEndIndex());

        return aggregatedSubpartitionBytes
                .subList(
                        subpartitionIndexRange.getStartIndex(),
                        subpartitionIndexRange.getEndIndex() + 1)
                .stream()
                .reduce(0L, Long::sum);
    }

    @Override
    public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {
        // Once all partitions are finished, we can convert the subpartition bytes to aggregated
        // value to reduce the space usage, because the distribution of source splits does not
        // affect the distribution of data consumed by downstream tasks of ALL_TO_ALL edges(Hashing
        // or Rebalancing, we do not consider rare cases such as custom partitions here).
        if (aggregatedSubpartitionBytes == null) {
            super.recordPartitionInfo(partitionIndex, partitionBytes);

            if (subpartitionBytesByPartitionIndex.size() == numOfPartitions) {
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
                this.aggregatedSubpartitionBytes =
                        Arrays.stream(aggregatedBytes).boxed().collect(Collectors.toList());
                this.subpartitionBytesByPartitionIndex.clear();
            }
        }
    }

    @Override
    public void resetPartitionInfo(int partitionIndex) {
        if (aggregatedSubpartitionBytes == null) {
            super.resetPartitionInfo(partitionIndex);
        }
    }

    public List<Long> getAggregatedSubpartitionBytes() {
        checkState(aggregatedSubpartitionBytes != null, "Not all partition infos are ready");
        return Collections.unmodifiableList(aggregatedSubpartitionBytes);
    }
}

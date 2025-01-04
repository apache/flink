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

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Helper class that provides read-only information of input for {@link
 * VertexParallelismAndInputInfosDecider}.
 */
public class BlockingInputInfo implements BlockingResultInfo {
    /** The original blocking result information. */
    private final BlockingResultInfo blockingResultInfo;

    /** The type number of the input for co-tasks. */
    private final int inputTypeNumber;

    /**
     * If true, means that there are relationships between multiple inputs, if the data
     * corresponding to a specific join key from one input is split, the corresponding join key data
     * from the other inputs must be duplicated (meaning that it must be sent to the downstream
     * nodes where the split data is sent).
     */
    private final boolean interInputsKeyCorrelation;

    /**
     * If true, means that the data corresponding to a specific join key must be sent to the same
     * downstream subtask.
     */
    private final boolean intraInputKeyCorrelation;

    public BlockingInputInfo(
            BlockingResultInfo blockingResultInfo,
            int inputTypeNumber,
            boolean interInputsKeyCorrelation,
            boolean intraInputKeyCorrelation) {
        this.blockingResultInfo = checkNotNull(blockingResultInfo);
        this.inputTypeNumber = inputTypeNumber;
        this.interInputsKeyCorrelation = interInputsKeyCorrelation;
        this.intraInputKeyCorrelation = intraInputKeyCorrelation;
    }

    public int getInputTypeNumber() {
        return inputTypeNumber;
    }

    public boolean existIntraInputKeyCorrelation() {
        return intraInputKeyCorrelation;
    }

    public boolean existInterInputsKeyCorrelation() {
        return interInputsKeyCorrelation;
    }

    public List<Long> getAggregatedSubpartitionBytes() {
        checkState(blockingResultInfo instanceof AllToAllBlockingResultInfo);
        return ((AllToAllBlockingResultInfo) blockingResultInfo).getAggregatedSubpartitionBytes();
    }

    @Override
    public boolean isBroadcast() {
        return blockingResultInfo.isBroadcast();
    }

    @Override
    public boolean isPointwise() {
        return blockingResultInfo.isPointwise();
    }

    @Override
    public int getNumPartitions() {
        return blockingResultInfo.getNumPartitions();
    }

    @Override
    public int getNumSubpartitions(int partitionIndex) {
        return blockingResultInfo.getNumSubpartitions(partitionIndex);
    }

    @Override
    public long getNumBytesProduced() {
        return blockingResultInfo.getNumBytesProduced();
    }

    @Override
    public long getNumBytesProduced(
            IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
        long inputBytes = 0;
        Map<Integer, long[]> subpartitionBytesByPartitionIndex =
                blockingResultInfo.getSubpartitionBytesByPartitionIndex();
        checkState(!subpartitionBytesByPartitionIndex.isEmpty(), "Partition has been aggregated.");
        for (int i = partitionIndexRange.getStartIndex();
                i <= partitionIndexRange.getEndIndex();
                ++i) {
            checkState(
                    subpartitionIndexRange.getEndIndex()
                            < subpartitionBytesByPartitionIndex.get(i).length,
                    "Subpartition end index %s is out of range of partition %s.",
                    subpartitionIndexRange.getEndIndex(),
                    i);
            for (int j = subpartitionIndexRange.getStartIndex();
                    j <= subpartitionIndexRange.getEndIndex();
                    ++j) {
                inputBytes += subpartitionBytesByPartitionIndex.get(i)[j];
            }
        }
        return inputBytes;
    }

    @Override
    public IntermediateDataSetID getResultId() {
        return blockingResultInfo.getResultId();
    }

    @Override
    public boolean isSingleSubpartitionContainsAllData() {
        return blockingResultInfo.isSingleSubpartitionContainsAllData();
    }

    @Override
    public Map<Integer, long[]> getSubpartitionBytesByPartitionIndex() {
        return blockingResultInfo.getSubpartitionBytesByPartitionIndex();
    }

    @Override
    public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {
        throw new UnsupportedOperationException("Not allowed to modify read-only view.");
    }

    @Override
    public void resetPartitionInfo(int partitionIndex) {
        throw new UnsupportedOperationException("Not allowed to modify read-only view.");
    }
}

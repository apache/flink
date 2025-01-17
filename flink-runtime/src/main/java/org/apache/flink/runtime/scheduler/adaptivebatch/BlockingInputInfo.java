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
     * If true, means that there are relationships between multiple inputs, if the records
     * corresponding to the same key from one input is split, the corresponding key records from the
     * other inputs must be duplicated (meaning that it must be sent to the downstream nodes where
     * the split data is sent).
     */
    private final boolean interInputsKeysCorrelated;

    /**
     * If true, means that records with the same key are correlated and must be sent to the same
     * downstream task to be processed together.
     */
    private final boolean intraInputKeyCorrelated;

    public BlockingInputInfo(
            BlockingResultInfo blockingResultInfo,
            int inputTypeNumber,
            boolean interInputsKeysCorrelated,
            boolean intraInputKeyCorrelated) {
        this.blockingResultInfo = checkNotNull(blockingResultInfo);
        this.inputTypeNumber = inputTypeNumber;
        this.interInputsKeysCorrelated = interInputsKeysCorrelated;
        this.intraInputKeyCorrelated = intraInputKeyCorrelated;
    }

    public int getInputTypeNumber() {
        return inputTypeNumber;
    }

    public boolean isIntraInputKeyCorrelated() {
        return intraInputKeyCorrelated;
    }

    public boolean areInterInputsKeysCorrelated() {
        return interInputsKeysCorrelated;
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
        return blockingResultInfo.getNumBytesProduced(partitionIndexRange, subpartitionIndexRange);
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

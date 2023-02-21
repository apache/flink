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
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Base blocking result info. */
abstract class AbstractBlockingResultInfo implements BlockingResultInfo {

    private final IntermediateDataSetID resultId;

    protected final int numOfPartitions;

    protected final int numOfSubpartitions;

    /**
     * The subpartition bytes map. The key is the partition index, value is a subpartition bytes
     * list.
     */
    protected final Map<Integer, long[]> subpartitionBytesByPartitionIndex;

    AbstractBlockingResultInfo(
            IntermediateDataSetID resultId, int numOfPartitions, int numOfSubpartitions) {
        this.resultId = checkNotNull(resultId);
        this.numOfPartitions = numOfPartitions;
        this.numOfSubpartitions = numOfSubpartitions;
        this.subpartitionBytesByPartitionIndex = new HashMap<>();
    }

    @Override
    public IntermediateDataSetID getResultId() {
        return resultId;
    }

    @Override
    public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {
        checkState(partitionBytes.getSubpartitionBytes().length == numOfSubpartitions);
        subpartitionBytesByPartitionIndex.put(
                partitionIndex, partitionBytes.getSubpartitionBytes());
    }

    @Override
    public void resetPartitionInfo(int partitionIndex) {
        subpartitionBytesByPartitionIndex.remove(partitionIndex);
    }

    @VisibleForTesting
    int getNumOfRecordedPartitions() {
        return subpartitionBytesByPartitionIndex.size();
    }
}

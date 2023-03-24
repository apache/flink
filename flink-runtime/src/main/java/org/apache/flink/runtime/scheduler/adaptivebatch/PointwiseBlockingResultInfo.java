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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkState;

/** Information of Pointwise result. */
public class PointwiseBlockingResultInfo extends AbstractBlockingResultInfo {
    PointwiseBlockingResultInfo(
            IntermediateDataSetID resultId, int numOfPartitions, int numOfSubpartitions) {
        super(resultId, numOfPartitions, numOfSubpartitions);
    }

    @Override
    public boolean isBroadcast() {
        return false;
    }

    @Override
    public boolean isPointwise() {
        return true;
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
                subpartitionBytesByPartitionIndex.size() == numOfPartitions,
                "Not all partition infos are ready");
        return subpartitionBytesByPartitionIndex.values().stream()
                .flatMapToLong(Arrays::stream)
                .reduce(0L, Long::sum);
    }

    @Override
    public long getNumBytesProduced(
            IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
        long inputBytes = 0;
        for (int i = partitionIndexRange.getStartIndex();
                i <= partitionIndexRange.getEndIndex();
                ++i) {
            checkState(
                    subpartitionBytesByPartitionIndex.get(i) != null,
                    "Partition index %s is not ready.",
                    i);
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
}

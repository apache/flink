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

package org.apache.flink.runtime.scheduler.adaptivebatch.util;

import org.apache.flink.runtime.executiongraph.IndexRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class that describes the statistics of all subpartitions with a specific index within the
 * given partition range. It may represent a complete subpartition group or a part of the
 * subpartition group, depending on the partition range.
 */
public class SubpartitionSlice {

    /** The index of the subpartition of the subpartition slice. */
    int subpartitionIndex;

    /** The range of partitions that the subpartition slice covers. */
    IndexRange partitionRange;

    /** The size of the subpartition slice in bytes. */
    long dataBytes;

    public SubpartitionSlice(int subpartitionIndex, IndexRange partitionRange, long dataBytes) {
        this.subpartitionIndex = subpartitionIndex;
        this.partitionRange = checkNotNull(partitionRange);
        this.dataBytes = dataBytes;
    }

    public long getDataBytes() {
        return dataBytes;
    }

    public int getSubpartitionIndex() {
        return subpartitionIndex;
    }

    /**
     * SubpartitionSlice is used to describe a group of inputs with the same type number which may
     * have different numbers of partitions, so we need to use the specific partitions number to get
     * the correct partition range.
     *
     * <p>Example, given a specific typeNumber with 2 inputs, and partition counts of 3 and 2
     * respectively, if the current SubpartitionSlice's PartitionRange is [1,2], it may need
     * adjustment for the second input. the adjustment ensures that the PartitionRange aligns with
     * the expected partition count. <br>
     * -input 0: partition count = 3, valid PartitionRange = [0, 2] <br>
     * -input 1: partition count = 2, valid PartitionRange = [0, 1] <br>
     * If the SubpartitionSlice's PartitionRange is [1, 2], it should be corrected to [1, 1] for
     * typeNumber 1 to match its partition count.
     *
     * @param numPartitions the number of partitions
     * @return the partition range if the partition range is valid, empty otherwise
     */
    public Optional<IndexRange> getPartitionRange(int numPartitions) {
        if (partitionRange.getEndIndex() < numPartitions) {
            return Optional.of(partitionRange);
        } else if (partitionRange.getStartIndex() < numPartitions
                && partitionRange.getEndIndex() >= numPartitions) {
            return Optional.of(new IndexRange(partitionRange.getStartIndex(), numPartitions - 1));
        } else {
            return Optional.empty();
        }
    }

    public static SubpartitionSlice createSubpartitionSlice(
            int subpartitionIndex, IndexRange partitionRange, long aggregatedSubpartitionBytes) {
        return new SubpartitionSlice(
                subpartitionIndex, partitionRange, aggregatedSubpartitionBytes);
    }

    public static List<SubpartitionSlice> createSubpartitionSlices(
            int subpartitionIndex,
            List<IndexRange> partitionRanges,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        for (IndexRange partitionRange : partitionRanges) {
            subpartitionSlices.add(
                    createSubpartitionSlice(
                            subpartitionIndex, partitionRange, subpartitionBytesByPartitionIndex));
        }
        return subpartitionSlices;
    }

    private static SubpartitionSlice createSubpartitionSlice(
            int subpartitionIndex,
            IndexRange partitionRange,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        return new SubpartitionSlice(
                subpartitionIndex,
                partitionRange,
                getNumBytesByIndexRange(
                        subpartitionIndex, partitionRange, subpartitionBytesByPartitionIndex));
    }

    private static long getNumBytesByIndexRange(
            int subpartitionIndex,
            IndexRange partitionIndexRange,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        return IntStream.rangeClosed(
                        partitionIndexRange.getStartIndex(), partitionIndexRange.getEndIndex())
                .mapToLong(i -> subpartitionBytesByPartitionIndex.get(i)[subpartitionIndex])
                .sum();
    }
}

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

/** Helper class that provides information for subpartition slice. */
public class SubpartitionSlice {

    int subpartitionIndex;
    IndexRange partitionRange;

    long size;

    public SubpartitionSlice(int subpartitionIndex, IndexRange partitionRange, long size) {
        this.subpartitionIndex = subpartitionIndex;
        this.partitionRange = partitionRange;
        this.size = size;
    }

    public long getSize() {
        return size;
    }

    public int getSubpartitionIndex() {
        return subpartitionIndex;
    }

    /**
     * SubpartitionSlice is used to describe a group of inputs with the same type number which may
     * have different numbers of partitions, so we need to use the specific partitions number to get
     * the correct partition range.
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

    public static SubpartitionSlice createSubpartitionSlice(
            int subpartitionIndex,
            IndexRange partitionRange,
            Map<Integer, long[]> subpartitionBytesByPartitionIndex) {
        return new SubpartitionSlice(
                subpartitionIndex,
                partitionRange,
                getNumBytesByIndexRange(
                        subpartitionIndex, partitionRange, subpartitionBytesByPartitionIndex));
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

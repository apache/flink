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

import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetIntraCorrelation;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeSkewThreshold;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeTargetSize;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getMaxNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.hasSameNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.median;

/** Helper class that provides information of aggregated input infos. */
public class AggregatedBlockingInputInfo {
    private final int maxPartitionNum;

    private final long skewedThreshold;
    private final long targetSize;

    private final boolean hasSamePartitionNums;
    private final boolean existIntraInputCorrelation;

    private final Map<Integer, long[]> subpartitionBytesByPartition;
    private final long[] aggregatedSubpartitionBytes;

    public AggregatedBlockingInputInfo(
            long targetSize,
            long skewedThreshold,
            int maxPartitionNum,
            boolean hasSamePartitionNums,
            boolean existIntraInputCorrelation,
            Map<Integer, long[]> subpartitionBytesByPartition,
            long[] aggregatedSubpartitionBytes) {
        this.maxPartitionNum = maxPartitionNum;
        this.skewedThreshold = skewedThreshold;
        this.targetSize = targetSize;
        this.hasSamePartitionNums = hasSamePartitionNums;
        this.existIntraInputCorrelation = existIntraInputCorrelation;
        this.subpartitionBytesByPartition = subpartitionBytesByPartition;
        this.aggregatedSubpartitionBytes = aggregatedSubpartitionBytes;
    }

    public int getMaxPartitionNum() {
        return maxPartitionNum;
    }

    public long getTargetSize() {
        return targetSize;
    }

    public Map<Integer, long[]> getSubpartitionBytesByPartition() {
        return Collections.unmodifiableMap(subpartitionBytesByPartition);
    }

    public long getAggregatedSubpartitionBytes(int subpartitionIndex) {
        return aggregatedSubpartitionBytes[subpartitionIndex];
    }

    public boolean isSplittable() {
        return !existIntraInputCorrelation
                && hasSamePartitionNums
                && subpartitionBytesByPartition.isEmpty();
    }

    public boolean isSkewedSubpartition(int subpartitionIndex) {
        return aggregatedSubpartitionBytes[subpartitionIndex] > skewedThreshold;
    }

    private static long[] computeAggregatedSubpartitionBytes(
            List<BlockingInputInfo> inputInfos, int subpartitionNum) {
        long[] aggregatedSubpartitionBytes = new long[subpartitionNum];
        for (BlockingInputInfo inputInfo : inputInfos) {
            List<Long> subpartitionBytes = inputInfo.getAggregatedSubpartitionBytes();
            for (int i = 0; i < subpartitionBytes.size(); i++) {
                aggregatedSubpartitionBytes[i] += subpartitionBytes.get(i);
            }
        }
        return aggregatedSubpartitionBytes;
    }

    private static Map<Integer, long[]> computeSubpartitionBytesByPartitionIndex(
            List<BlockingInputInfo> inputInfos, int subpartitionNum) {
        Map<Integer, long[]> subpartitionBytesByPartitionIndex = new HashMap<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            inputInfo
                    .getSubpartitionBytesByPartitionIndex()
                    .forEach(
                            (partitionIdx, subPartitionBytes) -> {
                                long[] subpartitionBytes =
                                        subpartitionBytesByPartitionIndex.computeIfAbsent(
                                                partitionIdx, v -> new long[subpartitionNum]);
                                for (int i = 0; i < subpartitionNum; i++) {
                                    subpartitionBytes[i] += subPartitionBytes[i];
                                }
                            });
        }
        return subpartitionBytesByPartitionIndex;
    }

    public static AggregatedBlockingInputInfo createAggregatedBlockingInputInfo(
            long defaultSkewedThreshold,
            double skewedFactor,
            long dataVolumePerTask,
            int subPartitionNum,
            List<BlockingInputInfo> inputInfos) {
        long[] aggregatedSubpartitionBytes =
                computeAggregatedSubpartitionBytes(inputInfos, subPartitionNum);
        long skewedThreshold =
                computeSkewThreshold(
                        median(aggregatedSubpartitionBytes), skewedFactor, defaultSkewedThreshold);
        long targetSize =
                computeTargetSize(aggregatedSubpartitionBytes, subPartitionNum, dataVolumePerTask);
        return new AggregatedBlockingInputInfo(
                targetSize,
                skewedThreshold,
                getMaxNumPartitions(inputInfos),
                hasSameNumPartitions(inputInfos),
                checkAndGetIntraCorrelation(inputInfos),
                computeSubpartitionBytesByPartitionIndex(inputInfos, subPartitionNum),
                aggregatedSubpartitionBytes);
    }
}

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetIntraCorrelation;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeSkewThreshold;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeTargetSize;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getMaxNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.hasSameNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.median;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Helper class that aggregates input information with the same typeNumber so that they can be
 * processed as a single unit.
 */
public class AggregatedBlockingInputInfo {
    private static final Logger LOG = LoggerFactory.getLogger(AggregatedBlockingInputInfo.class);

    /** The maximum number of partitions among all aggregated inputs. */
    private final int maxPartitionNum;

    /** The threshold used to determine if a specific aggregated subpartition is skewed. */
    private final long skewedThreshold;

    /** The target size for splitting skewed aggregated subpartitions. */
    private final long targetSize;

    /**
     * Indicates whether the records corresponding to the same key must be sent to the same
     * downstream subtask.
     */
    private final boolean intraInputKeyCorrelated;

    /**
     * A map where the key is the partition index and the value is an array representing the size of
     * each subpartition for that partition. This map is used to provide fine-grained information
     * for splitting subpartitions with same index. If it is empty, means that the split operation
     * cannot be performed. In the following cases, this map will be empty: 1.
     * IntraInputKeyCorrelated is true. 2. The aggregated input infos have different num partitions.
     * 3. The SubpartitionBytesByPartitionIndex of inputs is empty.
     */
    private final Map<Integer, long[]> subpartitionBytesByPartition;

    /**
     * An array representing the aggregated size of each subpartition across all partitions. Each
     * element in the array corresponds to a subpartition.
     */
    private final long[] aggregatedSubpartitionBytes;

    private AggregatedBlockingInputInfo(
            long targetSize,
            long skewedThreshold,
            int maxPartitionNum,
            boolean intraInputKeyCorrelated,
            Map<Integer, long[]> subpartitionBytesByPartition,
            long[] aggregatedSubpartitionBytes) {
        this.maxPartitionNum = maxPartitionNum;
        this.skewedThreshold = skewedThreshold;
        this.targetSize = targetSize;
        this.intraInputKeyCorrelated = intraInputKeyCorrelated;
        this.subpartitionBytesByPartition = checkNotNull(subpartitionBytesByPartition);
        this.aggregatedSubpartitionBytes = checkNotNull(aggregatedSubpartitionBytes);
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
        return !intraInputKeyCorrelated && !subpartitionBytesByPartition.isEmpty();
    }

    public boolean isSkewedSubpartition(int subpartitionIndex) {
        return aggregatedSubpartitionBytes[subpartitionIndex] > skewedThreshold;
    }

    public int getNumSubpartitions() {
        return aggregatedSubpartitionBytes.length;
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
        // If inputInfos have different num partitions (means that these upstream have different
        // parallelisms), return an empty result to disable data splitting.
        if (!hasSameNumPartitions(inputInfos)) {
            LOG.warn(
                    "Input infos have different num partitions, skip calculate SubpartitionBytesByPartitionIndex");
            return Collections.emptyMap();
        }
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
            List<BlockingInputInfo> inputInfos) {
        int subpartitionNum = checkAndGetSubpartitionNum(inputInfos);
        long[] aggregatedSubpartitionBytes =
                computeAggregatedSubpartitionBytes(inputInfos, subpartitionNum);
        long skewedThreshold =
                computeSkewThreshold(
                        median(aggregatedSubpartitionBytes), skewedFactor, defaultSkewedThreshold);
        long targetSize =
                computeTargetSize(aggregatedSubpartitionBytes, skewedThreshold, dataVolumePerTask);
        boolean isIntraInputKeyCorrelated = checkAndGetIntraCorrelation(inputInfos);
        Map<Integer, long[]> subpartitionBytesByPartitionIndex;
        if (isIntraInputKeyCorrelated) {
            // subpartitions with same index will not be split, skipped calculate it
            subpartitionBytesByPartitionIndex = new HashMap<>();
        } else {
            subpartitionBytesByPartitionIndex =
                    computeSubpartitionBytesByPartitionIndex(inputInfos, subpartitionNum);
        }
        return new AggregatedBlockingInputInfo(
                targetSize,
                skewedThreshold,
                getMaxNumPartitions(inputInfos),
                isIntraInputKeyCorrelated,
                subpartitionBytesByPartitionIndex,
                aggregatedSubpartitionBytes);
    }
}

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
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlice;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetPartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createJobVertexInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getInputsWithIntraCorrelation;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getMinSubpartitionCount;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for pointwise input. */
public class PointwiseVertexInputInfoComputer {
    private static final Logger LOG =
            LoggerFactory.getLogger(PointwiseVertexInputInfoComputer.class);

    // Used to limit the maximum number of subpartition slices to prevent increasing the
    // time complexity of the parallelism deciding.
    private static final int MAX_NUM_SUBPARTITION_SLICES_FACTOR = 32;

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for POINTWISE, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume that `inputInfo` has two partitions, each partition has three subpartitions, their
     * data bytes are: {0->[1,2,1], 1->[2,1,2]}, and the expected parallelism is 3. The calculation
     * process is as follows: <br>
     * 1. Create subpartition slices for input which is composed of several subpartitions. The
     * created slice list and its data bytes are: [1,2,1,2,1,2] <br>
     * 2. Distribute the subpartition slices array into n balanced parts (described by `IndexRange`,
     * named SubpartitionSliceRanges) based on data volume: [0,1],[2,3],[4,5] <br>
     * 3. Reorganize the distributed results into a mapping of partition range to subpartition
     * range: {0 -> [0,1]}, {0->[2,2],1->[0,0]}, {1->[1,2]}. <br>
     * The final result is the `SubpartitionGroup` that each of the three parallel tasks need to
     * subscribe.
     *
     * @param inputInfos The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @param dataVolumePerTask proposed data volume per task for this set of inputInfo
     * @return the parallelism and vertex input infos
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            List<BlockingInputInfo> inputInfos,
            int parallelism,
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask) {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByInputIndex =
                createSubpartitionSlicesByInputIndex(inputInfos, maxParallelism);

        // Note: SubpartitionSliceRanges does not represent the real index of the subpartitions, but
        // the location of that subpartition in all subpartitions, as we aggregate all subpartitions
        // into a one-digit array to calculate.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        minParallelism,
                        maxParallelism,
                        dataVolumePerTask,
                        subpartitionSlicesByInputIndex);

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Cannot find a legal parallelism to evenly distribute data amount for inputs {}, "
                            + "fallback to compute a parallelism that can evenly distribute num subpartitions.",
                    inputInfos.stream()
                            .map(BlockingInputInfo::getResultId)
                            .collect(Collectors.toList()));
            // This computer is only used in the adaptive batch scenario, where isDynamicGraph
            // should always be true.
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(
                isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism));

        // Create vertex input infos based on the subpartition slice and ranges.
        return createJobVertexInputInfos(
                inputInfos,
                subpartitionSlicesByInputIndex,
                subpartitionSliceRanges,
                index -> index);
    }

    private static Map<Integer, List<SubpartitionSlice>> createSubpartitionSlicesByInputIndex(
            List<BlockingInputInfo> inputInfos, int maxParallelism) {
        int numSubpartitionSlices;
        List<BlockingInputInfo> inputsWithIntraCorrelation =
                getInputsWithIntraCorrelation(inputInfos);
        if (!inputsWithIntraCorrelation.isEmpty()) {
            // Ensure that when creating subpartition slices, data with intra-correlation will
            // not be split.
            numSubpartitionSlices = checkAndGetPartitionNum(inputsWithIntraCorrelation);
        } else {
            // Use the minimum of the two to avoid creating too many subpartition slices, which will
            // lead to too high the time complexity of the parallelism deciding.
            numSubpartitionSlices =
                    Math.min(
                            getMinSubpartitionCount(inputInfos),
                            MAX_NUM_SUBPARTITION_SLICES_FACTOR * maxParallelism);
        }

        Map<Integer, List<SubpartitionSlice>> subpartitionSlices = new HashMap<>();
        for (int i = 0; i < inputInfos.size(); ++i) {
            BlockingInputInfo inputInfo = inputInfos.get(i);
            subpartitionSlices.put(i, createSubpartitionSlices(inputInfo, numSubpartitionSlices));
        }

        return subpartitionSlices;
    }

    private static List<SubpartitionSlice> createSubpartitionSlices(
            BlockingInputInfo inputInfo, int total) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        int numPartitions = inputInfo.getNumPartitions();
        int numSubpartitions = checkAndGetSubpartitionNum(List.of(inputInfo));
        if (numPartitions >= total) {
            for (int i = 0; i < total; ++i) {
                int start = i * numPartitions / total;
                int nextStart = (i + 1) * numPartitions / total;
                IndexRange partitionRange = new IndexRange(start, nextStart - 1);
                IndexRange subpartitionRange = new IndexRange(0, numSubpartitions - 1);
                subpartitionSlices.add(
                        createSubpartitionSlice(
                                partitionRange,
                                subpartitionRange,
                                inputInfo.getNumBytesProduced(partitionRange, subpartitionRange)));
            }
        } else {
            for (int i = 0; i < numPartitions; i++) {
                int count = (i + 1) * total / numPartitions - i * total / numPartitions;
                checkState(count > 0 && count <= numSubpartitions);
                IndexRange partitionRange = new IndexRange(i, i);
                for (int j = 0; j < count; ++j) {
                    int start = j * numSubpartitions / count;
                    int nextStart = (j + 1) * numSubpartitions / count;
                    IndexRange subpartitionRange = new IndexRange(start, nextStart - 1);
                    subpartitionSlices.add(
                            createSubpartitionSlice(
                                    partitionRange,
                                    subpartitionRange,
                                    inputInfo.getNumBytesProduced(
                                            partitionRange, subpartitionRange)));
                }
            }
        }
        return subpartitionSlices;
    }
}

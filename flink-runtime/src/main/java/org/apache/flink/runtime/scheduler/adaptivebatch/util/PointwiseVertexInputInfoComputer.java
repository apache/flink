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

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlice;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.calculateDataVolumePerTaskForInput;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdJobVertexInputInfoForNonBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for pointwise input. */
public class PointwiseVertexInputInfoComputer {
    private static final Logger LOG =
            LoggerFactory.getLogger(PointwiseVertexInputInfoComputer.class);

    /**
     * Computes the input information for a job vertex based on the provided blocking input
     * information and parallelism.
     *
     * @param inputInfos List of blocking input information for the job vertex.
     * @param parallelism Parallelism of the job vertex.
     * @param dataVolumePerTask Proposed data volume per task for this set of inputInfo.
     * @return A map of intermediate data set IDs to their corresponding job vertex input
     *     information.
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            List<BlockingInputInfo> inputInfos, int parallelism, long dataVolumePerTask) {
        long totalDataBytes =
                inputInfos.stream().mapToLong(BlockingInputInfo::getNumBytesProduced).sum();
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            // Currently, we consider all inputs in this method must don't have inter-inputs key
            // correlation. If other possibilities are introduced in the future, please add new
            // branches to this method.
            checkState(!inputInfo.areInterInputsKeysCorrelated());
            if (inputInfo.isIntraInputKeyCorrelated()) {
                // In this case, we won't split subpartitions within the same partition, so need
                // to ensure NumPartitions >= parallelism.
                checkState(parallelism <= inputInfo.getNumPartitions());
            }
            vertexInputInfos.put(
                    inputInfo.getResultId(),
                    computeVertexInputInfo(
                            inputInfo,
                            parallelism,
                            calculateDataVolumePerTaskForInput(
                                    dataVolumePerTask,
                                    inputInfo.getNumBytesProduced(),
                                    totalDataBytes)));
        }
        return vertexInputInfos;
    }

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
     * @param inputInfo The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex. Since pointwise inputs always compute
     *     vertex input info one-by-one, we need a determined parallelism to ensure the final
     *     decided parallelism for all inputs is consistent.
     * @return the vertex input info
     */
    private static JobVertexInputInfo computeVertexInputInfo(
            BlockingInputInfo inputInfo, int parallelism, long dataVolumePerTask) {
        List<SubpartitionSlice> subpartitionSlices = createSubpartitionSlices(inputInfo);

        // Node: SubpartitionSliceRanges does not represent the real index of the subpartitions, but
        // the location of that subpartition in all subpartitions, as we aggregate all subpartitions
        // into a one-digit array to calculate.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        parallelism,
                        parallelism,
                        dataVolumePerTask,
                        Map.of(inputInfo.getInputTypeNumber(), subpartitionSlices));

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Cannot find a legal parallelism to evenly distribute data amount for input {}, "
                            + "fallback to compute a parallelism that can evenly distribute num subpartitions.",
                    inputInfo.getResultId());
            // This computer is only used in the adaptive batch scenario, where isDynamicGraph
            // should always be true.
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    inputInfo.getNumPartitions(),
                    parallelism,
                    inputInfo::getNumSubpartitions,
                    true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(isLegalParallelism(subpartitionSliceRanges.size(), parallelism, parallelism));

        // Create vertex input info based on the subpartition slice and ranges.
        return createJobVertexInputInfo(inputInfo, subpartitionSliceRanges, subpartitionSlices);
    }

    private static List<SubpartitionSlice> createSubpartitionSlices(BlockingInputInfo inputInfo) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        if (inputInfo.isIntraInputKeyCorrelated()) {
            // If the input has intra-input correlation, we need to ensure all subpartitions
            // in the same partition index are assigned to the same downstream concurrent task.
            for (int i = 0; i < inputInfo.getNumPartitions(); ++i) {
                IndexRange partitionRange = new IndexRange(i, i);
                IndexRange subpartitionRange =
                        new IndexRange(0, inputInfo.getNumSubpartitions(i) - 1);
                subpartitionSlices.add(
                        createSubpartitionSlice(
                                partitionRange,
                                subpartitionRange,
                                inputInfo.getNumBytesProduced(partitionRange, subpartitionRange)));
            }
        } else {
            for (int i = 0; i < inputInfo.getNumPartitions(); ++i) {
                IndexRange partitionRange = new IndexRange(i, i);
                for (int j = 0; j < inputInfo.getNumSubpartitions(i); ++j) {
                    IndexRange subpartitionRange = new IndexRange(j, j);
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

    private static JobVertexInputInfo createJobVertexInputInfo(
            BlockingInputInfo inputInfo,
            List<IndexRange> subpartitionSliceRanges,
            List<SubpartitionSlice> subpartitionSlices) {
        checkState(!inputInfo.isBroadcast());
        return createdJobVertexInputInfoForNonBroadcast(
                inputInfo, subpartitionSliceRanges, subpartitionSlices);
    }
}

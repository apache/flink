/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Util to compute {@link JobVertexInputInfo}s for execution job vertex. */
public class VertexInputInfoComputationUtils {

    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            ExecutionJobVertex ejv,
            Function<IntermediateDataSetID, IntermediateResult> intermediateResultRetriever)
            throws JobException {
        checkState(ejv.isParallelismDecided());
        final List<IntermediateResultInfo> intermediateResultInfos = new ArrayList<>();
        for (JobEdge edge : ejv.getJobVertex().getInputs()) {
            IntermediateResult ires = intermediateResultRetriever.apply(edge.getSourceId());
            if (ires == null) {
                throw new JobException(
                        "Cannot connect this job graph to the previous graph. No previous intermediate result found for ID "
                                + edge.getSourceId());
            }
            intermediateResultInfos.add(new IntermediateResultWrapper(ires));
        }
        return computeVertexInputInfos(
                ejv.getParallelism(), intermediateResultInfos, ejv.getGraph().isDynamic());
    }

    public static Map<IntermediateDataSetID, JobVertexInputInfo> computeVertexInputInfos(
            int parallelism,
            List<? extends IntermediateResultInfo> inputs,
            boolean isDynamicGraph) {

        checkArgument(parallelism > 0);
        final Map<IntermediateDataSetID, JobVertexInputInfo> jobVertexInputInfos =
                new LinkedHashMap<>();

        for (IntermediateResultInfo input : inputs) {
            int sourceParallelism = input.getNumPartitions();

            if (input.isPointwise()) {
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForPointwise(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph));
            } else {
                jobVertexInputInfos.putIfAbsent(
                        input.getResultId(),
                        computeVertexInputInfoForAllToAll(
                                sourceParallelism,
                                parallelism,
                                input::getNumSubpartitions,
                                isDynamicGraph,
                                input.isBroadcast()));
            }
        }

        return jobVertexInputInfos;
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#POINTWISE} edge. This
     * computation algorithm will evenly distribute subpartitions to downstream subtasks according
     * to the number of subpartitions. Different downstream subtasks consume roughly the same number
     * of subpartitions.
     *
     * @param sourceCount the parallelism of upstream
     * @param targetCount the parallelism of downstream
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @return the computed {@link JobVertexInputInfo}
     */
    static JobVertexInputInfo computeVertexInputInfoForPointwise(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph) {

        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();

        if (sourceCount >= targetCount) {
            for (int index = 0; index < targetCount; index++) {

                int start = index * sourceCount / targetCount;
                int end = (index + 1) * sourceCount / targetCount;

                IndexRange partitionRange = new IndexRange(start, end - 1);
                IndexRange subpartitionRange =
                        computeConsumedSubpartitionRange(
                                index,
                                1,
                                () -> numOfSubpartitionsRetriever.apply(start),
                                isDynamicGraph,
                                false);
                executionVertexInputInfos.add(
                        new ExecutionVertexInputInfo(index, partitionRange, subpartitionRange));
            }
        } else {
            for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

                int start = (partitionNum * targetCount + sourceCount - 1) / sourceCount;
                int end = ((partitionNum + 1) * targetCount + sourceCount - 1) / sourceCount;
                int numConsumers = end - start;

                IndexRange partitionRange = new IndexRange(partitionNum, partitionNum);
                // Variable used in lambda expression should be final or effectively final
                final int finalPartitionNum = partitionNum;
                for (int i = start; i < end; i++) {
                    IndexRange subpartitionRange =
                            computeConsumedSubpartitionRange(
                                    i,
                                    numConsumers,
                                    () -> numOfSubpartitionsRetriever.apply(finalPartitionNum),
                                    isDynamicGraph,
                                    false);
                    executionVertexInputInfos.add(
                            new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
                }
            }
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the {@link JobVertexInputInfo} for a {@link DistributionPattern#ALL_TO_ALL} edge.
     * This computation algorithm will evenly distribute subpartitions to downstream subtasks
     * according to the number of subpartitions. Different downstream subtasks consume roughly the
     * same number of subpartitions.
     *
     * @param sourceCount the parallelism of upstream
     * @param targetCount the parallelism of downstream
     * @param numOfSubpartitionsRetriever a retriever to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @param isBroadcast whether the edge is broadcast
     * @return the computed {@link JobVertexInputInfo}
     */
    static JobVertexInputInfo computeVertexInputInfoForAllToAll(
            int sourceCount,
            int targetCount,
            Function<Integer, Integer> numOfSubpartitionsRetriever,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        final List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        IndexRange partitionRange = new IndexRange(0, sourceCount - 1);
        for (int i = 0; i < targetCount; ++i) {
            IndexRange subpartitionRange =
                    computeConsumedSubpartitionRange(
                            i,
                            targetCount,
                            () -> numOfSubpartitionsRetriever.apply(0),
                            isDynamicGraph,
                            isBroadcast);
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange));
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Compute the consumed subpartition range for a subtask. This computation algorithm will evenly
     * distribute subpartitions to downstream subtasks according to the number of subpartitions.
     * Different downstream subtasks consume roughly the same number of subpartitions.
     *
     * @param consumerSubtaskIndex the subtask index
     * @param numConsumers the total number of consumers
     * @param numOfSubpartitionsSupplier a supplier to get the number of subpartitions
     * @param isDynamicGraph whether is dynamic graph
     * @param isBroadcast whether the edge is broadcast
     * @return the computed subpartition range
     */
    @VisibleForTesting
    static IndexRange computeConsumedSubpartitionRange(
            int consumerSubtaskIndex,
            int numConsumers,
            Supplier<Integer> numOfSubpartitionsSupplier,
            boolean isDynamicGraph,
            boolean isBroadcast) {
        int consumerIndex = consumerSubtaskIndex % numConsumers;
        if (!isDynamicGraph) {
            return new IndexRange(consumerIndex, consumerIndex);
        } else {
            int numSubpartitions = numOfSubpartitionsSupplier.get();
            if (isBroadcast) {
                // broadcast results have only one subpartition, and be consumed multiple times.
                checkArgument(numSubpartitions == 1);
                return new IndexRange(0, 0);
            } else {
                checkArgument(consumerIndex < numConsumers);
                checkArgument(numConsumers <= numSubpartitions);

                int start = consumerIndex * numSubpartitions / numConsumers;
                int nextStart = (consumerIndex + 1) * numSubpartitions / numConsumers;

                return new IndexRange(start, nextStart - 1);
            }
        }
    }

    private static class IntermediateResultWrapper implements IntermediateResultInfo {
        private final IntermediateResult intermediateResult;

        IntermediateResultWrapper(IntermediateResult intermediateResult) {
            this.intermediateResult = checkNotNull(intermediateResult);
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return intermediateResult.getId();
        }

        @Override
        public boolean isBroadcast() {
            return intermediateResult.isBroadcast();
        }

        @Override
        public boolean isPointwise() {
            return intermediateResult.getConsumingDistributionPattern()
                    == DistributionPattern.POINTWISE;
        }

        @Override
        public int getNumPartitions() {
            return intermediateResult.getNumberOfAssignedPartitions();
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            // Note that this method should only be called for dynamic graph.This method is used to
            // compute which sub-partitions a consumer vertex should consume, however, for
            // non-dynamic graph it is not needed, and the number of sub-partitions is not decided
            // at this stage, due to the execution edge are not created.
            checkState(
                    intermediateResult.getProducer().getGraph().isDynamic(),
                    "This method should only be called for dynamic graph.");
            return intermediateResult.getPartitions()[partitionIndex].getNumberOfSubpartitions();
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private VertexInputInfoComputationUtils() {}
}

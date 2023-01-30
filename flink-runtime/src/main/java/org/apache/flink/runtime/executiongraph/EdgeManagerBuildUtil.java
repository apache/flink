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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utilities for building {@link EdgeManager}. */
public class EdgeManagerBuildUtil {

    /**
     * Calculate the connections between {@link ExecutionJobVertex} and {@link IntermediateResult} *
     * based on the {@link DistributionPattern}.
     *
     * @param vertex the downstream consumer {@link ExecutionJobVertex}
     * @param intermediateResult the upstream consumed {@link IntermediateResult}
     */
    static void connectVertexToResult(
            ExecutionJobVertex vertex, IntermediateResult intermediateResult) {
        final DistributionPattern distributionPattern =
                intermediateResult.getConsumingDistributionPattern();
        final JobVertexInputInfo jobVertexInputInfo =
                vertex.getGraph()
                        .getJobVertexInputInfo(vertex.getJobVertexId(), intermediateResult.getId());

        switch (distributionPattern) {
            case POINTWISE:
                connectPointwise(vertex, intermediateResult, jobVertexInputInfo);
                break;
            case ALL_TO_ALL:
                connectAllToAll(vertex, intermediateResult, jobVertexInputInfo);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    /**
     * Given parallelisms of two job vertices, compute the max number of edges connected to a target
     * execution vertex from the source execution vertices. Note that edge is considered undirected
     * here. It can be an edge connected from an upstream job vertex to a downstream job vertex, or
     * in a reversed way.
     *
     * @param targetParallelism parallelism of the target job vertex.
     * @param sourceParallelism parallelism of the source job vertex.
     * @param distributionPattern the {@link DistributionPattern} of the connecting edge.
     */
    public static int computeMaxEdgesToTargetExecutionVertex(
            int targetParallelism, int sourceParallelism, DistributionPattern distributionPattern) {
        switch (distributionPattern) {
            case POINTWISE:
                return (sourceParallelism + targetParallelism - 1) / targetParallelism;
            case ALL_TO_ALL:
                return sourceParallelism;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    private static void connectAllToAll(
            ExecutionJobVertex jobVertex,
            IntermediateResult result,
            JobVertexInputInfo jobVertexInputInfo) {
        // check the vertex input info is legal
        jobVertexInputInfo
                .getExecutionVertexInputInfos()
                .forEach(
                        executionVertexInputInfo -> {
                            IndexRange partitionRange =
                                    executionVertexInputInfo.getPartitionIndexRange();
                            checkArgument(partitionRange.getStartIndex() == 0);
                            checkArgument(
                                    partitionRange.getEndIndex()
                                            == (result.getNumberOfAssignedPartitions() - 1));
                        });

        connectInternal(
                Arrays.asList(jobVertex.getTaskVertices()),
                Arrays.asList(result.getPartitions()),
                result.getResultType(),
                jobVertex.getGraph().getEdgeManager());
    }

    private static void connectPointwise(
            ExecutionJobVertex jobVertex,
            IntermediateResult result,
            JobVertexInputInfo jobVertexInputInfo) {

        Map<IndexRange, List<Integer>> consumersByPartition = new LinkedHashMap<>();

        for (ExecutionVertexInputInfo executionVertexInputInfo :
                jobVertexInputInfo.getExecutionVertexInputInfos()) {
            int consumerIndex = executionVertexInputInfo.getSubtaskIndex();
            IndexRange range = executionVertexInputInfo.getPartitionIndexRange();
            consumersByPartition.compute(
                    range,
                    (ignore, consumers) -> {
                        if (consumers == null) {
                            consumers = new ArrayList<>();
                        }
                        consumers.add(consumerIndex);
                        return consumers;
                    });
        }

        consumersByPartition.forEach(
                (range, subtasks) -> {
                    List<ExecutionVertex> taskVertices = new ArrayList<>();
                    List<IntermediateResultPartition> partitions = new ArrayList<>();
                    for (int index : subtasks) {
                        taskVertices.add(jobVertex.getTaskVertices()[index]);
                    }
                    for (int i = range.getStartIndex(); i <= range.getEndIndex(); ++i) {
                        partitions.add(result.getPartitions()[i]);
                    }
                    connectInternal(
                            taskVertices,
                            partitions,
                            result.getResultType(),
                            jobVertex.getGraph().getEdgeManager());
                });
    }

    /** Connect all execution vertices to all partitions. */
    private static void connectInternal(
            List<ExecutionVertex> taskVertices,
            List<IntermediateResultPartition> partitions,
            ResultPartitionType resultPartitionType,
            EdgeManager edgeManager) {
        checkState(!taskVertices.isEmpty());
        checkState(!partitions.isEmpty());

        ConsumedPartitionGroup consumedPartitionGroup =
                createAndRegisterConsumedPartitionGroupToEdgeManager(
                        taskVertices.size(), partitions, resultPartitionType, edgeManager);
        for (ExecutionVertex ev : taskVertices) {
            ev.addConsumedPartitionGroup(consumedPartitionGroup);
        }

        List<ExecutionVertexID> consumerVertices =
                taskVertices.stream().map(ExecutionVertex::getID).collect(Collectors.toList());
        ConsumerVertexGroup consumerVertexGroup =
                ConsumerVertexGroup.fromMultipleVertices(consumerVertices, resultPartitionType);
        for (IntermediateResultPartition partition : partitions) {
            partition.addConsumers(consumerVertexGroup);
        }

        consumedPartitionGroup.setConsumerVertexGroup(consumerVertexGroup);
        consumerVertexGroup.setConsumedPartitionGroup(consumedPartitionGroup);
    }

    private static ConsumedPartitionGroup createAndRegisterConsumedPartitionGroupToEdgeManager(
            int numConsumers,
            List<IntermediateResultPartition> partitions,
            ResultPartitionType resultPartitionType,
            EdgeManager edgeManager) {
        List<IntermediateResultPartitionID> partitionIds =
                partitions.stream()
                        .map(IntermediateResultPartition::getPartitionId)
                        .collect(Collectors.toList());
        ConsumedPartitionGroup consumedPartitionGroup =
                ConsumedPartitionGroup.fromMultiplePartitions(
                        numConsumers, partitionIds, resultPartitionType);
        finishAllDataProducedPartitions(partitions, consumedPartitionGroup);
        edgeManager.registerConsumedPartitionGroup(consumedPartitionGroup);
        return consumedPartitionGroup;
    }

    private static void finishAllDataProducedPartitions(
            List<IntermediateResultPartition> partitions,
            ConsumedPartitionGroup consumedPartitionGroup) {
        for (IntermediateResultPartition partition : partitions) {
            // this is for dynamic graph as consumedPartitionGroup has not been created when the
            // partition becomes finished.
            if (partition.hasDataAllProduced()) {
                consumedPartitionGroup.partitionFinished();
            }
        }
    }
}

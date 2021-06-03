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

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for building {@link EdgeManager}. */
public class EdgeManagerBuildUtil {

    /**
     * Calculate the connections between {@link ExecutionJobVertex} and {@link IntermediateResult} *
     * based on the {@link DistributionPattern}.
     *
     * @param vertex the downstream consumer {@link ExecutionJobVertex}
     * @param intermediateResult the upstream consumed {@link IntermediateResult}
     * @param distributionPattern the {@link DistributionPattern} of the edge that connects the
     *     upstream {@link IntermediateResult} and the downstream {@link IntermediateResult}
     */
    static void connectVertexToResult(
            ExecutionJobVertex vertex,
            IntermediateResult intermediateResult,
            DistributionPattern distributionPattern) {

        switch (distributionPattern) {
            case POINTWISE:
                connectPointwise(vertex.getTaskVertices(), intermediateResult);
                break;
            case ALL_TO_ALL:
                connectAllToAll(vertex.getTaskVertices(), intermediateResult);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized distribution pattern.");
        }
    }

    private static void connectAllToAll(
            ExecutionVertex[] taskVertices, IntermediateResult intermediateResult) {

        ConsumedPartitionGroup consumedPartitions =
                ConsumedPartitionGroup.fromMultiplePartitions(
                        Arrays.stream(intermediateResult.getPartitions())
                                .map(IntermediateResultPartition::getPartitionId)
                                .collect(Collectors.toList()));
        for (ExecutionVertex ev : taskVertices) {
            ev.addConsumedPartitionGroup(consumedPartitions);
        }

        ConsumerVertexGroup vertices =
                ConsumerVertexGroup.fromMultipleVertices(
                        Arrays.stream(taskVertices)
                                .map(ExecutionVertex::getID)
                                .collect(Collectors.toList()));
        for (IntermediateResultPartition partition : intermediateResult.getPartitions()) {
            partition.addConsumers(vertices);
        }
    }

    private static void connectPointwise(
            ExecutionVertex[] taskVertices, IntermediateResult intermediateResult) {

        final int sourceCount = intermediateResult.getPartitions().length;
        final int targetCount = taskVertices.length;

        if (sourceCount == targetCount) {
            for (int i = 0; i < sourceCount; i++) {
                ExecutionVertex executionVertex = taskVertices[i];
                IntermediateResultPartition partition = intermediateResult.getPartitions()[i];

                ConsumerVertexGroup consumerVertexGroup =
                        ConsumerVertexGroup.fromSingleVertex(executionVertex.getID());
                partition.addConsumers(consumerVertexGroup);

                ConsumedPartitionGroup consumedPartitionGroup =
                        ConsumedPartitionGroup.fromSinglePartition(partition.getPartitionId());
                executionVertex.addConsumedPartitionGroup(consumedPartitionGroup);
            }
        } else if (sourceCount > targetCount) {
            for (int index = 0; index < targetCount; index++) {

                ExecutionVertex executionVertex = taskVertices[index];
                ConsumerVertexGroup consumerVertexGroup =
                        ConsumerVertexGroup.fromSingleVertex(executionVertex.getID());

                int start = index * sourceCount / targetCount;
                int end = (index + 1) * sourceCount / targetCount;

                List<IntermediateResultPartitionID> consumedPartitions =
                        new ArrayList<>(end - start);

                for (int i = start; i < end; i++) {
                    IntermediateResultPartition partition = intermediateResult.getPartitions()[i];
                    partition.addConsumers(consumerVertexGroup);

                    consumedPartitions.add(partition.getPartitionId());
                }

                ConsumedPartitionGroup consumedPartitionGroup =
                        ConsumedPartitionGroup.fromMultiplePartitions(consumedPartitions);
                executionVertex.addConsumedPartitionGroup(consumedPartitionGroup);
            }
        } else {
            for (int partitionNum = 0; partitionNum < sourceCount; partitionNum++) {

                IntermediateResultPartition partition =
                        intermediateResult.getPartitions()[partitionNum];
                ConsumedPartitionGroup consumerPartitionGroup =
                        ConsumedPartitionGroup.fromSinglePartition(partition.getPartitionId());

                int start = (partitionNum * targetCount + sourceCount - 1) / sourceCount;
                int end = ((partitionNum + 1) * targetCount + sourceCount - 1) / sourceCount;

                List<ExecutionVertexID> consumers = new ArrayList<>(end - start);

                for (int i = start; i < end; i++) {
                    ExecutionVertex executionVertex = taskVertices[i];
                    executionVertex.addConsumedPartitionGroup(consumerPartitionGroup);

                    consumers.add(executionVertex.getID());
                }

                ConsumerVertexGroup consumerVertexGroup =
                        ConsumerVertexGroup.fromMultipleVertices(consumers);
                partition.addConsumers(consumerVertexGroup);
            }
        }
    }
}

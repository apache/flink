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

package org.apache.flink.table.runtime.strategy;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.scheduler.adaptivebatch.AllToAllBlockingResultInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingResultInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeSkewThreshold;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.median;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

public class AdaptiveSkewedJoinOptimizationStrategy
        extends BaseAdaptiveJoinOperatorOptimizationStrategy {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveSkewedJoinOptimizationStrategy.class);

    private static final int LEFT_INPUT_TYPE_NUMBER = 1;
    private static final int RIGHT_INPUT_TYPE_NUMBER = 2;

    private boolean initialized = false;
    private Map<Integer, Map<Integer, long[]>> aggregatedProducedBytesByTypeNumberAndNodeId;

    private OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy
            adaptiveSkewedJoinOptimizationStrategy;
    private long skewedThresholdInBytes;
    private double skewedFactor;

    @Override
    public boolean maybeOptimizeStreamGraph(
            OperatorsFinished operatorsFinished, StreamGraphContext context) throws Exception {
        initialize(context.getStreamGraph().getConfiguration());
        visitDownstreamAdaptiveJoinNode(operatorsFinished, context);

        return true;
    }

    @Override
    void tryOptimizeAdaptiveJoin(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            List<ImmutableStreamEdge> upstreamStreamEdges,
            AdaptiveJoin adaptiveJoin) {
        if (!canPerformOptimization(adaptiveJoinNode)) {
            return;
        }
        for (ImmutableStreamEdge edge : upstreamStreamEdges) {
            BlockingResultInfo resultInfo = getBlockingResultInfo(operatorsFinished, context, edge);
            checkState(resultInfo instanceof AllToAllBlockingResultInfo);
            aggregatedProducedBytesByTypeNumber(
                    adaptiveJoinNode,
                    edge.getTypeNumber(),
                    ((AllToAllBlockingResultInfo) resultInfo).getAggregatedSubpartitionBytes());
        }
        if (context.areAllUpstreamNodesFinished(adaptiveJoinNode)) {
            applyAdaptiveSkewedJoinOptimization(
                    context, adaptiveJoinNode, adaptiveJoin.getJoinType());
            freeNodeStatistic(adaptiveJoinNode.getId());
        }
    }

    private void initialize(ReadableConfig config) {
        if (!initialized) {
            initialized = true;
            aggregatedProducedBytesByTypeNumberAndNodeId = new HashMap<>();
            adaptiveSkewedJoinOptimizationStrategy =
                    config.get(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY);
            skewedFactor =
                    config.get(
                            OptimizerConfigOptions
                                    .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_FACTOR);
            skewedThresholdInBytes =
                    config.get(
                                    OptimizerConfigOptions
                                            .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_SKEWED_THRESHOLD)
                            .getBytes();
        }
    }

    private void aggregatedProducedBytesByTypeNumber(
            ImmutableStreamNode adaptiveJoinNode, int typeNumber, List<Long> subpartitionBytes) {
        Integer streamNodeId = adaptiveJoinNode.getId();

        long[] aggregatedSubpartitionBytes =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .computeIfAbsent(streamNodeId, k -> new HashMap<>())
                        .computeIfAbsent(
                                typeNumber, (ignore) -> new long[subpartitionBytes.size()]);
        checkState(subpartitionBytes.size() == aggregatedSubpartitionBytes.length);
        for (int i = 0; i < subpartitionBytes.size(); i++) {
            aggregatedSubpartitionBytes[i] += subpartitionBytes.get(i);
        }
    }

    private void freeNodeStatistic(Integer nodeId) {
        aggregatedProducedBytesByTypeNumberAndNodeId.remove(nodeId);
    }

    private boolean canPerformOptimization(ImmutableStreamNode adaptiveJoinNode) {
        if (adaptiveSkewedJoinOptimizationStrategy
                == OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.AUTO) {
            return existForwardOutEdge(adaptiveJoinNode.getOutEdges());
        } else if (adaptiveSkewedJoinOptimizationStrategy
                == OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.FORCED) {
            return existExactForwardOutEdge(adaptiveJoinNode.getOutEdges());
        } else {
            return false;
        }
    }

    private void applyAdaptiveSkewedJoinOptimization(
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            FlinkJoinType joinType) {
        long[] leftInputSize =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .get(adaptiveJoinNode.getId())
                        .get(LEFT_INPUT_TYPE_NUMBER);
        long[] rightInputSize =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .get(adaptiveJoinNode.getId())
                        .get(RIGHT_INPUT_TYPE_NUMBER);
        checkArgument(
                leftInputSize != null && rightInputSize != null,
                "Adaptive join node currently supports only two inputs, "
                        + "but received input bytes with left [%s] and right [%s] for stream "
                        + "node id [%s].",
                leftInputSize,
                rightInputSize,
                adaptiveJoinNode.getId());

        long leftSkewedThreshold =
                computeSkewThreshold(median(leftInputSize), skewedFactor, skewedThresholdInBytes);
        long rightSkewedThreshold =
                computeSkewThreshold(median(rightInputSize), skewedFactor, skewedThresholdInBytes);

        boolean isLeftOptimizable = false;
        boolean isRightOptimizable = false;
        switch (joinType) {
            case RIGHT:
                isRightOptimizable = true;
                break;
            case INNER:
                isLeftOptimizable = true;
                isRightOptimizable = true;
                break;
            case LEFT:
            case SEMI:
            case ANTI:
                isLeftOptimizable = true;
                break;
            case FULL:
            default:
                throw new RuntimeException(String.format("Unexpected join type %s.", joinType));
        }

        isLeftOptimizable =
                isLeftOptimizable
                        & existBytesLargerThanThreshold(leftInputSize, leftSkewedThreshold);
        isRightOptimizable =
                isRightOptimizable
                        & existBytesLargerThanThreshold(rightInputSize, rightSkewedThreshold);

        if (isLeftOptimizable) {
            boolean isModificationSucceed =
                    postStreamEdgeModificationRequest(
                            context, adaptiveJoinNode, LEFT_INPUT_TYPE_NUMBER);
            LOG.info(
                    "Apply skewed join optimization {} for left input of node {}.",
                    isModificationSucceed ? "succeeded" : "failed",
                    adaptiveJoinNode.getId());
        }
        if (isRightOptimizable) {
            boolean isModificationSucceed =
                    postStreamEdgeModificationRequest(
                            context, adaptiveJoinNode, RIGHT_INPUT_TYPE_NUMBER);
            LOG.info(
                    "Apply skewed join optimization {} for right input of node {}.",
                    isModificationSucceed ? "succeeded" : "failed",
                    adaptiveJoinNode.getId());
        }
    }

    private static boolean postStreamEdgeModificationRequest(
            StreamGraphContext context, ImmutableStreamNode adaptiveJoinNode, int typeNumber) {
        List<StreamEdgeUpdateRequestInfo> modifiedRequests = new ArrayList<>();
        modifiedRequests.addAll(
                generateModifiedCorrelationRequestInfos(
                        filterEdges(adaptiveJoinNode.getInEdges(), typeNumber)));
        modifiedRequests.addAll(
                generateForwardPartitionerModificationRequestInfos(
                        adaptiveJoinNode.getOutEdges(), context));
        return context.modifyStreamEdge(modifiedRequests);
    }

    private static List<StreamEdgeUpdateRequestInfo> generateModifiedCorrelationRequestInfos(
            List<ImmutableStreamEdge> streamEdges) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge edge : streamEdges) {
            streamEdgeUpdateRequestInfos.add(
                    new StreamEdgeUpdateRequestInfo(
                                    edge.getEdgeId(), edge.getSourceId(), edge.getTargetId())
                            .existIntraInputKeyCorrelation(false));
        }
        return streamEdgeUpdateRequestInfos;
    }

    private static List<StreamEdgeUpdateRequestInfo>
            generateForwardPartitionerModificationRequestInfos(
                    List<ImmutableStreamEdge> streamEdges, StreamGraphContext context) {
        List<ImmutableStreamEdge> forwardEdges =
                streamEdges.stream()
                        .filter(ImmutableStreamEdge::isForwardEdge)
                        .collect(Collectors.toList());
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge edge : forwardEdges) {
            StreamPartitioner<?> newPartitioner;
            StreamPartitioner<?> partitioner =
                    context.getOutputPartitioner(
                            edge.getSourceId(), edge.getTargetId(), edge.getEdgeId());
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                newPartitioner =
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner();
            } else {
                newPartitioner = new RescalePartitioner<>();
            }
            streamEdgeUpdateRequestInfos.add(
                    new StreamEdgeUpdateRequestInfo(
                                    edge.getEdgeId(), edge.getSourceId(), edge.getTargetId())
                            .outputPartitioner(newPartitioner));
        }
        return streamEdgeUpdateRequestInfos;
    }

    private static BlockingResultInfo getBlockingResultInfo(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamEdge edge) {
        List<BlockingResultInfo> resultInfos =
                operatorsFinished.getResultInfoMap().get(edge.getSourceId());
        IntermediateDataSetID intermediateDataSetId =
                context.getConsumedIntermediateDataSetId(edge.getEdgeId());
        for (BlockingResultInfo result : resultInfos) {
            if (result.getResultId() == intermediateDataSetId) {
                return result;
            }
        }
        throw new IllegalArgumentException(
                "No matching BlockingResultInfo found for edge ID: " + edge.getEdgeId());
    }

    private static boolean existBytesLargerThanThreshold(long[] inputBytes, long threshold) {
        for (long byteSize : inputBytes) {
            if (byteSize > threshold) {
                return true;
            }
        }
        return false;
    }

    private static boolean existExactForwardOutEdge(List<ImmutableStreamEdge> edges) {
        return edges.stream().anyMatch(ImmutableStreamEdge::isExactForwardEdge);
    }

    private static boolean existForwardOutEdge(List<ImmutableStreamEdge> edges) {
        return edges.stream().anyMatch(ImmutableStreamEdge::isForwardEdge);
    }

    public static List<ImmutableStreamEdge> filterEdges(
            List<ImmutableStreamEdge> inEdges, int typeNumber) {
        return inEdges.stream()
                .filter(e -> e.getTypeNumber() == typeNumber)
                .collect(Collectors.toList());
    }
}

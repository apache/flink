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

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.computeSkewThreshold;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.median;
import static org.apache.flink.table.runtime.strategy.AdaptiveJoinOptimizationUtils.filterEdges;
import static org.apache.flink.table.runtime.strategy.AdaptiveJoinOptimizationUtils.isBroadcastJoin;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The stream graph optimization strategy of adaptive skewed join. */
public class AdaptiveSkewedJoinOptimizationStrategy
        extends BaseAdaptiveJoinOperatorOptimizationStrategy {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveSkewedJoinOptimizationStrategy.class);

    private Map<Integer, Map<Integer, long[]>> aggregatedProducedBytesByTypeNumberAndNodeId;

    private OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy
            adaptiveSkewedJoinOptimizationStrategy;
    private long skewedThresholdInBytes;
    private double skewedFactor;

    @Override
    public void initialize(StreamGraphContext context) {
        ReadableConfig config = context.getStreamGraph().getConfiguration();
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

    @Override
    public boolean onOperatorsFinished(
            OperatorsFinished operatorsFinished, StreamGraphContext context) throws Exception {
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
        if (!canPerformOptimization(context, adaptiveJoinNode)) {
            freeNodeStatistic(adaptiveJoinNode.getId());
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
        if (context.checkUpstreamNodesFinished(adaptiveJoinNode, null)) {
            applyAdaptiveSkewedJoinOptimization(
                    context, adaptiveJoinNode, adaptiveJoin.getJoinType());
            freeNodeStatistic(adaptiveJoinNode.getId());
        }
    }

    private boolean canPerformOptimization(
            StreamGraphContext context, ImmutableStreamNode adaptiveJoinNode) {
        // For broadcast joins, especially those generated by
        // AdaptiveBroadcastJoinOptimizationStrategy, skip perform optimization to
        // avoid unexpected problems.
        if (isBroadcastJoin(adaptiveJoinNode)) {
            return false;
        }
        if (adaptiveSkewedJoinOptimizationStrategy
                == OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.AUTO) {
            return canPerformOptimizationAutomatic(context, adaptiveJoinNode);
        } else if (adaptiveSkewedJoinOptimizationStrategy
                == OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.FORCED) {
            return canPerformOptimizationForced(context, adaptiveJoinNode);
        } else {
            return false;
        }
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
            if (result.getResultId().equals(intermediateDataSetId)) {
                return result;
            }
        }
        throw new IllegalStateException(
                "No matching BlockingResultInfo found for edge ID: " + edge.getEdgeId());
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

    private void applyAdaptiveSkewedJoinOptimization(
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            FlinkJoinType joinType) {
        long[] leftInputSize =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .get(adaptiveJoinNode.getId())
                        .get(LEFT_INPUT_TYPE_NUMBER);
        checkState(
                leftInputSize != null,
                "Left input bytes of adaptive join [%s] is unknown, which is unexpected.",
                adaptiveJoinNode.toString());
        long[] rightInputSize =
                aggregatedProducedBytesByTypeNumberAndNodeId
                        .get(adaptiveJoinNode.getId())
                        .get(RIGHT_INPUT_TYPE_NUMBER);
        checkState(
                rightInputSize != null,
                "Right input bytes of adaptive join [%s] is unknown, which is unexpected.",
                adaptiveJoinNode.toString());

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
                throw new IllegalStateException(
                        String.format("Unexpected join type %s.", joinType));
        }

        isLeftOptimizable =
                isLeftOptimizable
                        & existBytesLargerThanThreshold(leftInputSize, leftSkewedThreshold);
        isRightOptimizable =
                isRightOptimizable
                        & existBytesLargerThanThreshold(rightInputSize, rightSkewedThreshold);

        if (isLeftOptimizable) {
            boolean isModificationSucceed =
                    tryModifyInputAndOutputEdges(context, adaptiveJoinNode, LEFT_INPUT_TYPE_NUMBER);
            LOG.info(
                    "Apply skewed join optimization {} for left input of node {}.",
                    isModificationSucceed ? "succeeded" : "failed",
                    adaptiveJoinNode);
        }
        if (isRightOptimizable) {
            boolean isModificationSucceed =
                    tryModifyInputAndOutputEdges(
                            context, adaptiveJoinNode, RIGHT_INPUT_TYPE_NUMBER);
            LOG.info(
                    "Apply skewed join optimization {} for right input of node {}.",
                    isModificationSucceed ? "succeeded" : "failed",
                    adaptiveJoinNode);
        }
    }

    private static boolean tryModifyInputAndOutputEdges(
            StreamGraphContext context, ImmutableStreamNode adaptiveJoinNode, int typeNumber) {
        List<StreamEdgeUpdateRequestInfo> modifiedRequests = new ArrayList<>();
        // Modify the IntraInputKeyCorrelation of all input edges with the specified typeNumber to
        // false.
        modifiedRequests.addAll(
                generateCorrelationModificationRequestInfos(
                        filterEdges(adaptiveJoinNode.getInEdges(), typeNumber)));
        // Modify ForwardForConsecutiveHashPartitioner of the output edges to HashPartitioner
        modifiedRequests.addAll(
                generateForwardPartitionerModificationRequestInfos(
                        adaptiveJoinNode.getOutEdges(), context));
        return context.modifyStreamEdge(modifiedRequests);
    }

    private static List<StreamEdgeUpdateRequestInfo> generateCorrelationModificationRequestInfos(
            List<ImmutableStreamEdge> streamEdges) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge edge : streamEdges) {
            streamEdgeUpdateRequestInfos.add(
                    new StreamEdgeUpdateRequestInfo(
                                    edge.getEdgeId(), edge.getSourceId(), edge.getTargetId())
                            .withIntraInputKeyCorrelated(false));
        }
        return streamEdgeUpdateRequestInfos;
    }

    private static List<StreamEdgeUpdateRequestInfo>
            generateForwardPartitionerModificationRequestInfos(
                    List<ImmutableStreamEdge> streamEdges, StreamGraphContext context) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge edge : streamEdges) {
            if (edge.isForwardForConsecutiveHashEdge()) {
                StreamPartitioner<?> partitioner =
                        checkNotNull(
                                context.getOutputPartitioner(
                                        edge.getEdgeId(), edge.getSourceId(), edge.getTargetId()));
                StreamPartitioner<?> newPartitioner =
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner();
                streamEdgeUpdateRequestInfos.add(
                        new StreamEdgeUpdateRequestInfo(
                                        edge.getEdgeId(), edge.getSourceId(), edge.getTargetId())
                                .withOutputPartitioner(newPartitioner));
            }
        }
        return streamEdgeUpdateRequestInfos;
    }

    private void freeNodeStatistic(Integer nodeId) {
        aggregatedProducedBytesByTypeNumberAndNodeId.remove(nodeId);
    }

    private static boolean existBytesLargerThanThreshold(long[] inputBytes, long threshold) {
        for (long byteSize : inputBytes) {
            if (byteSize > threshold) {
                return true;
            }
        }
        return false;
    }
}

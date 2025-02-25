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
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingResultInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.runtime.strategy.AdaptiveJoinOptimizationUtils.filterEdges;
import static org.apache.flink.table.runtime.strategy.AdaptiveJoinOptimizationUtils.isBroadcastJoin;
import static org.apache.flink.table.runtime.strategy.AdaptiveJoinOptimizationUtils.isBroadcastJoinDisabled;
import static org.apache.flink.util.Preconditions.checkState;

/** The stream graph optimization strategy of adaptive broadcast join. */
public class AdaptiveBroadcastJoinOptimizationStrategy
        extends BaseAdaptiveJoinOperatorOptimizationStrategy {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveBroadcastJoinOptimizationStrategy.class);

    private Long broadcastThreshold;

    private Map<Integer, Map<Integer, Long>> aggregatedInputBytesByTypeNumberAndNodeId;
    private Set<Integer> optimizedAdaptiveJoinNodes;

    @Override
    public void initialize(StreamGraphContext context) {
        ReadableConfig config = context.getStreamGraph().getConfiguration();
        broadcastThreshold =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD);
        aggregatedInputBytesByTypeNumberAndNodeId = new HashMap<>();
        optimizedAdaptiveJoinNodes = new HashSet<>();
    }

    @Override
    public boolean onOperatorsFinished(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        visitDownstreamAdaptiveJoinNode(operatorsFinished, context);

        return true;
    }

    @Override
    protected void tryOptimizeAdaptiveJoin(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            List<ImmutableStreamEdge> upstreamStreamEdges,
            AdaptiveJoin adaptiveJoin) {
        if (!canPerformOptimization(adaptiveJoinNode, context)
                || optimizedAdaptiveJoinNodes.contains(adaptiveJoinNode.getId())) {
            return;
        }
        for (ImmutableStreamEdge upstreamEdge : upstreamStreamEdges) {
            IntermediateDataSetID relatedDataSetId =
                    context.getConsumedIntermediateDataSetId(upstreamEdge.getEdgeId());
            long producedBytes =
                    operatorsFinished.getResultInfoMap().get(upstreamEdge.getSourceId()).stream()
                            .filter(
                                    blockingResultInfo ->
                                            relatedDataSetId.equals(
                                                    blockingResultInfo.getResultId()))
                            .mapToLong(BlockingResultInfo::getNumBytesProduced)
                            .sum();
            aggregatedInputBytesByTypeNumber(
                    adaptiveJoinNode, upstreamEdge.getTypeNumber(), producedBytes);
        }

        FlinkJoinType joinType = adaptiveJoin.getJoinType();
        Long leftInputSize = null;
        Long rightInputSize = null;

        // When either input side of the adaptive join meets broadcast requirements, the broadcast
        // optimization can be immediately applied. This allows the opposite side to produce data
        // in a pointwise manner if it has not yet started scheduling, thereby reducing the costs
        // associated with shuffle writes and network overhead. Furthermore, in the future,
        // when multi-input support is available in the runtime, it could enable chaining large
        // input nodes with adaptive join nodes, further minimizing overall overhead.
        if (context.checkUpstreamNodesFinished(adaptiveJoinNode, LEFT_INPUT_TYPE_NUMBER)) {
            leftInputSize =
                    aggregatedInputBytesByTypeNumberAndNodeId
                            .get(adaptiveJoinNode.getId())
                            .get(LEFT_INPUT_TYPE_NUMBER);
            checkState(
                    leftInputSize != null,
                    "Left input bytes of adaptive join [%s] is unknown, which is unexpected.",
                    adaptiveJoinNode.getId());
            boolean leftIsBuild = true;
            if (checkInputSideCanBeBroadcast(joinType, leftIsBuild, leftInputSize)
                    && tryBroadcastOptimization(
                            adaptiveJoinNode, context, adaptiveJoin, leftIsBuild, leftInputSize)) {
                return;
            }
        }
        if (context.checkUpstreamNodesFinished(adaptiveJoinNode, RIGHT_INPUT_TYPE_NUMBER)) {
            rightInputSize =
                    aggregatedInputBytesByTypeNumberAndNodeId
                            .get(adaptiveJoinNode.getId())
                            .get(RIGHT_INPUT_TYPE_NUMBER);
            checkState(
                    rightInputSize != null,
                    "Right input bytes of adaptive join [%s] is unknown, which is unexpected.",
                    adaptiveJoinNode.getId());
            boolean leftIsBuild = false;
            if (checkInputSideCanBeBroadcast(joinType, leftIsBuild, rightInputSize)
                    && tryBroadcastOptimization(
                            adaptiveJoinNode, context, adaptiveJoin, leftIsBuild, rightInputSize)) {
                return;
            }
        }

        // When neither input meets broadcast thresholds, recompute the smaller side to provide
        // better performance for the shuffle join.
        if (leftInputSize != null && rightInputSize != null) {
            LOG.debug(
                    "The size of the specified side of the input data for the join node [{}] "
                            + "is too large to be converted into a broadcast hash join. "
                            + "The Join type: {}, Broadcast threshold: {} bytes, Left input size: "
                            + "{} bytes, Right input size: {} bytes.",
                    adaptiveJoinNode.getId(),
                    joinType,
                    broadcastThreshold,
                    leftInputSize,
                    rightInputSize);
            boolean leftSmallerThanRight = leftInputSize < rightInputSize;
            adaptiveJoin.markAsBroadcastJoin(false, leftSmallerThanRight);
            optimizedAdaptiveJoinNodes.add(adaptiveJoinNode.getId());
            aggregatedInputBytesByTypeNumberAndNodeId.remove(adaptiveJoinNode.getId());
        }
    }

    private boolean tryBroadcastOptimization(
            ImmutableStreamNode adaptiveJoinNode,
            StreamGraphContext context,
            AdaptiveJoin adaptiveJoin,
            boolean leftIsBuild,
            long inputBytes) {
        if (tryModifyStreamEdgesForBroadcastJoin(
                adaptiveJoinNode.getInEdges(), context, leftIsBuild)) {
            LOG.info(
                    "The {} input data size of the join node [{}] is small enough, "
                            + "adaptively convert it to a broadcast hash join. Broadcast "
                            + "threshold bytes: {}, actual input bytes: {}.",
                    leftIsBuild ? "left" : "right",
                    adaptiveJoinNode.getId(),
                    broadcastThreshold,
                    inputBytes);
            adaptiveJoin.markAsBroadcastJoin(true, leftIsBuild);
            optimizedAdaptiveJoinNodes.add(adaptiveJoinNode.getId());
            aggregatedInputBytesByTypeNumberAndNodeId.remove(adaptiveJoinNode.getId());
            return true;
        } else {
            LOG.info(
                    "Modification to stream edges for the join node [{}] failed. Keep the join node as is.",
                    adaptiveJoinNode.getId());
            return false;
        }
    }

    private Boolean checkInputSideCanBeBroadcast(
            FlinkJoinType joinType, boolean isLeftBuild, long producedBytes) {
        if (producedBytes < broadcastThreshold) {
            switch (joinType) {
                case RIGHT:
                    return isLeftBuild;
                case INNER:
                    return true;
                case LEFT:
                case SEMI:
                case ANTI:
                    return !isLeftBuild;
                case FULL:
                default:
                    throw new RuntimeException(String.format("Unexpected join type %s.", joinType));
            }
        }
        return false;
    }

    private boolean canPerformOptimization(
            ImmutableStreamNode adaptiveJoinNode, StreamGraphContext context) {
        if (isBroadcastJoinDisabled(context.getStreamGraph().getConfiguration())
                || isBroadcastJoin(adaptiveJoinNode)) {
            return false;
        }
        return canPerformOptimizationAutomatic(context, adaptiveJoinNode);
    }

    private void aggregatedInputBytesByTypeNumber(
            ImmutableStreamNode adaptiveJoinNode, int typeNumber, long producedBytes) {
        Integer streamNodeId = adaptiveJoinNode.getId();

        aggregatedInputBytesByTypeNumberAndNodeId
                .computeIfAbsent(streamNodeId, k -> new HashMap<>())
                .merge(typeNumber, producedBytes, Long::sum);
    }

    private List<StreamEdgeUpdateRequestInfo> generateStreamEdgeUpdateRequestInfos(
            List<ImmutableStreamEdge> modifiedEdges, StreamPartitioner<?> outputPartitioner) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge streamEdge : modifiedEdges) {
            StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                    new StreamEdgeUpdateRequestInfo(
                                    streamEdge.getEdgeId(),
                                    streamEdge.getSourceId(),
                                    streamEdge.getTargetId())
                            .withOutputPartitioner(outputPartitioner);
            streamEdgeUpdateRequestInfos.add(streamEdgeUpdateRequestInfo);
        }

        return streamEdgeUpdateRequestInfos;
    }

    private boolean tryModifyStreamEdgesForBroadcastJoin(
            List<ImmutableStreamEdge> inEdges, StreamGraphContext context, boolean leftIsBuild) {
        List<StreamEdgeUpdateRequestInfo> modifiedBuildSideEdges =
                generateStreamEdgeUpdateRequestInfos(
                        filterEdges(
                                inEdges,
                                leftIsBuild ? LEFT_INPUT_TYPE_NUMBER : RIGHT_INPUT_TYPE_NUMBER),
                        new BroadcastPartitioner<>());
        List<StreamEdgeUpdateRequestInfo> modifiedProbeSideEdges =
                generateStreamEdgeUpdateRequestInfos(
                        filterEdges(
                                inEdges,
                                leftIsBuild ? RIGHT_INPUT_TYPE_NUMBER : LEFT_INPUT_TYPE_NUMBER),
                        new ForwardForUnspecifiedPartitioner<>());
        modifiedBuildSideEdges.addAll(modifiedProbeSideEdges);

        return context.modifyStreamEdge(modifiedBuildSideEdges);
    }
}

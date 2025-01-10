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
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
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

    @Override
    public void initialize(StreamGraphContext context) {
        ReadableConfig config = context.getStreamGraph().getConfiguration();
        broadcastThreshold =
                config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD);
        aggregatedInputBytesByTypeNumberAndNodeId = new HashMap<>();
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
        if (!canPerformOptimization(
                adaptiveJoinNode, context.getStreamGraph().getConfiguration())) {
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

        // If all upstream nodes have finished, we attempt to optimize the AdaptiveJoin node.
        if (context.areAllUpstreamNodesFinished(adaptiveJoinNode)) {
            Long leftInputSize =
                    aggregatedInputBytesByTypeNumberAndNodeId.get(adaptiveJoinNode.getId()).get(1);
            checkState(
                    leftInputSize != null,
                    "Left input bytes of adaptive join [%s] is unknown, which is unexpected.",
                    adaptiveJoinNode.getId());
            Long rightInputSize =
                    aggregatedInputBytesByTypeNumberAndNodeId.get(adaptiveJoinNode.getId()).get(2);
            checkState(
                    rightInputSize != null,
                    "Right input bytes of adaptive join [%s] is unknown, which is unexpected.",
                    adaptiveJoinNode.getId());

            boolean leftSizeSmallerThanThreshold = leftInputSize <= broadcastThreshold;
            boolean rightSizeSmallerThanThreshold = rightInputSize <= broadcastThreshold;
            boolean leftSmallerThanRight = leftInputSize < rightInputSize;
            FlinkJoinType joinType = adaptiveJoin.getJoinType();
            boolean canBeBroadcast;
            boolean leftIsBuild;
            switch (joinType) {
                case RIGHT:
                    // For a right outer join, if the left side can be broadcast, then the left side
                    // is
                    // always the build side; otherwise, the smaller side is the build side.
                    canBeBroadcast = leftSizeSmallerThanThreshold;
                    leftIsBuild = true;
                    break;
                case INNER:
                    canBeBroadcast = leftSizeSmallerThanThreshold || rightSizeSmallerThanThreshold;
                    leftIsBuild = leftSmallerThanRight;
                    break;
                case LEFT:
                case SEMI:
                case ANTI:
                    // For left outer / semi / anti join, if the right side can be broadcast, then
                    // the
                    // right side is always the build side; otherwise, the smaller side is the build
                    // side.
                    canBeBroadcast = rightSizeSmallerThanThreshold;
                    leftIsBuild = false;
                    break;
                case FULL:
                default:
                    throw new RuntimeException(String.format("Unexpected join type %s.", joinType));
            }

            boolean isBroadcast = false;
            if (canBeBroadcast) {
                isBroadcast =
                        tryModifyStreamEdgesForBroadcastJoin(
                                adaptiveJoinNode.getInEdges(), context, leftIsBuild);

                if (isBroadcast) {
                    LOG.info(
                            "The {} input data size of the join node [{}] is small enough, "
                                    + "adaptively convert it to a broadcast hash join. Broadcast "
                                    + "threshold bytes: {}, left input bytes: {}, right input bytes: {}.",
                            leftIsBuild ? "left" : "right",
                            adaptiveJoinNode.getId(),
                            broadcastThreshold,
                            leftInputSize,
                            rightInputSize);
                }
            }
            adaptiveJoin.markAsBroadcastJoin(
                    isBroadcast, isBroadcast ? leftIsBuild : leftSmallerThanRight);

            aggregatedInputBytesByTypeNumberAndNodeId.remove(adaptiveJoinNode.getId());
        }
    }

    private boolean canPerformOptimization(
            ImmutableStreamNode adaptiveJoinNode, ReadableConfig config) {
        return !isBroadcastJoinDisabled(config) && !isBroadcastJoin(adaptiveJoinNode);
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
                        filterEdges(inEdges, leftIsBuild ? 1 : 2), new BroadcastPartitioner<>());
        List<StreamEdgeUpdateRequestInfo> modifiedProbeSideEdges =
                generateStreamEdgeUpdateRequestInfos(
                        filterEdges(inEdges, leftIsBuild ? 2 : 1), new ForwardPartitioner<>());
        modifiedBuildSideEdges.addAll(modifiedProbeSideEdges);

        return context.modifyStreamEdge(modifiedBuildSideEdges);
    }
}

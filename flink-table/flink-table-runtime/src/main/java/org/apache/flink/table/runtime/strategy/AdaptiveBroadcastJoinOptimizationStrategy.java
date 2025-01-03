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

import org.apache.flink.shaded.guava32.com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** The stream graph optimization strategy of adaptive broadcast join. */
public class AdaptiveBroadcastJoinOptimizationStrategy
        extends BaseAdaptiveJoinOperatorOptimizationStrategy {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdaptiveBroadcastJoinOptimizationStrategy.class);

    private Long broadcastThreshold;

    private Map<Integer, Map<Integer, Long>> aggregatedProducedBytesByTypeNumberAndNodeId;

    @Override
    public boolean onOperatorsFinished(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        initialize(context.getStreamGraph().getConfiguration());
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
        for (ImmutableStreamEdge upstreamEdge : upstreamStreamEdges) {
            IntermediateDataSetID relatedDataSetID =
                    context.getConsumedIntermediateDataSetId(upstreamEdge.getEdgeId());
            long producedBytes =
                    operatorsFinished.getResultInfoMap().get(upstreamEdge.getSourceId()).stream()
                            .filter(
                                    blockingResultInfo ->
                                            blockingResultInfo.getResultId() == relatedDataSetID)
                            .mapToLong(BlockingResultInfo::getNumBytesProduced)
                            .sum();
            aggregatedProducedBytesByTypeNumber(
                    adaptiveJoinNode, upstreamEdge.getTypeNumber(), producedBytes);
        }

        // If all upstream nodes have finished, we attempt to optimize the AdaptiveJoin node.
        if (context.areAllUpstreamNodesFinished(adaptiveJoinNode)) {
            Long leftInputSize =
                    aggregatedProducedBytesByTypeNumberAndNodeId
                            .get(adaptiveJoinNode.getId())
                            .get(1);
            Long rightInputSize =
                    aggregatedProducedBytesByTypeNumberAndNodeId
                            .get(adaptiveJoinNode.getId())
                            .get(2);
            Preconditions.checkArgument(
                    leftInputSize != null && rightInputSize != null,
                    "Adaptive join node currently supports only two inputs, "
                            + "but received input bytes with left [%s] and right [%s] for stream "
                            + "node id [%s].",
                    leftInputSize,
                    rightInputSize,
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
                            "The {} input data size of the join node [{}] is small enough which is {} bytes, "
                                    + "adaptively convert it to a broadcast hash join.",
                            leftIsBuild ? "left" : "right",
                            leftIsBuild ? leftInputSize : rightInputSize,
                            adaptiveJoinNode.getId());
                }
            }
            adaptiveJoin.markAsBroadcastJoin(
                    isBroadcast, isBroadcast ? leftIsBuild : leftSmallerThanRight);

            aggregatedProducedBytesByTypeNumberAndNodeId.remove(adaptiveJoinNode.getId());
        }
    }

    private void aggregatedProducedBytesByTypeNumber(
            ImmutableStreamNode adaptiveJoinNode, int typeNumber, long producedBytes) {
        Integer streamNodeId = adaptiveJoinNode.getId();

        aggregatedProducedBytesByTypeNumberAndNodeId
                .computeIfAbsent(streamNodeId, k -> new HashMap<>())
                .merge(typeNumber, producedBytes, Long::sum);
    }

    private List<ImmutableStreamEdge> filterEdges(
            List<ImmutableStreamEdge> inEdges, int typeNumber) {
        return inEdges.stream()
                .filter(e -> e.getTypeNumber() == typeNumber)
                .collect(Collectors.toList());
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
                            .outputPartitioner(outputPartitioner);
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

    private void initialize(ReadableConfig config) {
        if (aggregatedProducedBytesByTypeNumberAndNodeId == null) {
            broadcastThreshold =
                    config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD);
            aggregatedProducedBytesByTypeNumberAndNodeId = new HashMap<>();
        }
    }
}

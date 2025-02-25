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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.graph.util.StreamNodeUpdateRequestInfo;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The post-processing phase of adaptive join optimization, which must be placed at the end of all
 * adaptive join optimization strategies. This is necessary because certain operations, like
 * 'reorder inputs', can influence how adaptive broadcast join or skewed join determine the left and
 * right sides.
 */
public class PostProcessAdaptiveJoinStrategy extends BaseAdaptiveJoinOperatorOptimizationStrategy {

    private static final Logger LOG =
            LoggerFactory.getLogger(PostProcessAdaptiveJoinStrategy.class);

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
        if (context.checkUpstreamNodesFinished(adaptiveJoinNode, null)) {
            // For hash join, reorder the join node inputs so the build side is read first.
            if (adaptiveJoin.shouldReorderInputs()) {
                if (!context.modifyStreamEdge(
                                generateStreamEdgeUpdateRequestInfosForInputsReordered(
                                        adaptiveJoinNode))
                        || !context.modifyStreamNode(
                                generateStreamNodeUpdateRequestInfosForInputsReordered(
                                        adaptiveJoinNode))) {
                    throw new RuntimeException(
                            "Unexpected error occurs while reordering the inputs "
                                    + "of the adaptive join node, potentially leading to data inaccuracies. "
                                    + "Exceptions will be thrown.");
                } else {
                    LOG.info(
                            "Reordered the inputs of the adaptive join node {}.",
                            adaptiveJoinNode.getId());
                }
            }

            // Generate OperatorFactory for adaptive join operator after inputs are reordered.
            ReadableConfig config = context.getStreamGraph().getConfiguration();
            ClassLoader userClassLoader = context.getStreamGraph().getUserClassLoader();
            adaptiveJoin.genOperatorFactory(userClassLoader, config);
        }
    }

    private static List<StreamEdgeUpdateRequestInfo>
            generateStreamEdgeUpdateRequestInfosForInputsReordered(
                    ImmutableStreamNode adaptiveJoinNode) {
        List<StreamEdgeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();
        for (ImmutableStreamEdge inEdge : adaptiveJoinNode.getInEdges()) {
            StreamEdgeUpdateRequestInfo streamEdgeUpdateRequestInfo =
                    new StreamEdgeUpdateRequestInfo(
                            inEdge.getEdgeId(), inEdge.getSourceId(), inEdge.getTargetId());
            streamEdgeUpdateRequestInfo.withTypeNumber(inEdge.getTypeNumber() == 1 ? 2 : 1);
            streamEdgeUpdateRequestInfos.add(streamEdgeUpdateRequestInfo);
        }
        return streamEdgeUpdateRequestInfos;
    }

    private List<StreamNodeUpdateRequestInfo>
            generateStreamNodeUpdateRequestInfosForInputsReordered(
                    ImmutableStreamNode modifiedNode) {
        List<StreamNodeUpdateRequestInfo> streamEdgeUpdateRequestInfos = new ArrayList<>();

        TypeSerializer<?>[] typeSerializers = modifiedNode.getTypeSerializersIn();
        Preconditions.checkState(
                typeSerializers.length == 2,
                String.format(
                        "Adaptive join currently only supports two "
                                + "inputs, but the join node [%s] has received %s inputs.",
                        modifiedNode.getId(), typeSerializers.length));
        TypeSerializer<?>[] swappedTypeSerializers = new TypeSerializer<?>[2];
        swappedTypeSerializers[0] = typeSerializers[1];
        swappedTypeSerializers[1] = typeSerializers[0];
        StreamNodeUpdateRequestInfo requestInfo =
                new StreamNodeUpdateRequestInfo(modifiedNode.getId())
                        .withTypeSerializersIn(swappedTypeSerializers);
        streamEdgeUpdateRequestInfos.add(requestInfo);

        return streamEdgeUpdateRequestInfos;
    }
}

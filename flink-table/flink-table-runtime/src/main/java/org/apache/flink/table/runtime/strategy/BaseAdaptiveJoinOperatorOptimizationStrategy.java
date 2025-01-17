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

import org.apache.flink.runtime.scheduler.adaptivebatch.OperatorsFinished;
import org.apache.flink.runtime.scheduler.adaptivebatch.StreamGraphOptimizationStrategy;
import org.apache.flink.streaming.api.graph.StreamGraphContext;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamEdge;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamNode;
import org.apache.flink.table.runtime.operators.join.adaptive.AdaptiveJoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** The base stream graph optimization strategy class for adaptive join operator. */
public abstract class BaseAdaptiveJoinOperatorOptimizationStrategy
        implements StreamGraphOptimizationStrategy {

    protected void visitDownstreamAdaptiveJoinNode(
            OperatorsFinished operatorsFinished, StreamGraphContext context) {
        ImmutableStreamGraph streamGraph = context.getStreamGraph();
        List<Integer> finishedStreamNodeIds = operatorsFinished.getFinishedStreamNodeIds();
        Map<ImmutableStreamNode, List<ImmutableStreamEdge>> joinNodesWithInEdges = new HashMap<>();
        for (Integer finishedStreamNodeId : finishedStreamNodeIds) {
            for (ImmutableStreamEdge streamEdge :
                    streamGraph.getStreamNode(finishedStreamNodeId).getOutEdges()) {
                ImmutableStreamNode downstreamNode =
                        streamGraph.getStreamNode(streamEdge.getTargetId());
                if (downstreamNode.getOperatorFactory() instanceof AdaptiveJoin) {
                    joinNodesWithInEdges
                            .computeIfAbsent(downstreamNode, k -> new ArrayList<>())
                            .add(streamEdge);
                }
            }
        }
        for (ImmutableStreamNode joinNode : joinNodesWithInEdges.keySet()) {
            tryOptimizeAdaptiveJoin(
                    operatorsFinished,
                    context,
                    joinNode,
                    joinNodesWithInEdges.get(joinNode),
                    (AdaptiveJoin) joinNode.getOperatorFactory());
        }
    }

    abstract void tryOptimizeAdaptiveJoin(
            OperatorsFinished operatorsFinished,
            StreamGraphContext context,
            ImmutableStreamNode adaptiveJoinNode,
            List<ImmutableStreamEdge> upstreamStreamEdges,
            AdaptiveJoin adaptiveJoin);
}

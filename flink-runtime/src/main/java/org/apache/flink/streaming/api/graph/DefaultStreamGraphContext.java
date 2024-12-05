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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation for {@link StreamGraphContext}. */
@Internal
public class DefaultStreamGraphContext implements StreamGraphContext {
    private final StreamGraph streamGraph;
    private final ImmutableStreamGraph immutableStreamGraph;
    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeIdCache;
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    public DefaultStreamGraphContext(
            StreamGraph streamGraph,
            Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeIdCache,
            Map<Integer, Integer> frozenNodeToStartNodeMap,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches) {
        this.streamGraph = streamGraph;
        this.immutableStreamGraph = new ImmutableStreamGraph(streamGraph);
        this.forwardGroupsByStartNodeIdCache = forwardGroupsByStartNodeIdCache;
        this.frozenNodeToStartNodeMap = frozenNodeToStartNodeMap;
        this.opIntermediateOutputsCaches = opIntermediateOutputsCaches;
    }

    @Override
    public ImmutableStreamGraph getStreamGraph() {
        return immutableStreamGraph;
    }

    @Override
    public boolean modifyStreamEdge(List<StreamEdgeUpdateRequestInfo> requestInfos) {
        // We first verify the legality of all requestInfos to ensure that all requests can be
        // modified atomically.
        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            if (!modifyStreamEdgeValidate(requestInfo)) {
                return false;
            }
        }

        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            Integer sourceNodeId = requestInfo.getSourceId();
            Integer targetNodeId = requestInfo.getTargetId();
            StreamEdge targetEdge =
                    getStreamEdge(sourceNodeId, targetNodeId, requestInfo.getEdgeId());
            StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
            if (newPartitioner != null) {
                modifyOutputPartitioner(targetEdge, newPartitioner);
            }
        }

        return true;
    }

    private boolean modifyStreamEdgeValidate(StreamEdgeUpdateRequestInfo requestInfo) {
        StreamEdge targetEdge =
                getStreamEdge(
                        requestInfo.getSourceId(),
                        requestInfo.getTargetId(),
                        requestInfo.getEdgeId());
        if (targetEdge == null) {
            return false;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (frozenNodeToStartNodeMap.containsKey(targetNodeId)) {
            return false;
        }
        StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
        if (newPartitioner != null) {
            if (targetEdge.getPartitioner().getClass().equals(ForwardPartitioner.class)) {
                return false;
            }
            if (streamGraph.isDynamic()
                    && newPartitioner instanceof ForwardPartitioner
                    && !ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup(
                            forwardGroupsByStartNodeIdCache.get(sourceNodeId),
                            forwardGroupsByStartNodeIdCache.get(targetNodeId))) {
                requestInfo.outputPartitioner(new RescalePartitioner<>());
            }
        }
        return true;
    }

    private void modifyOutputPartitioner(
            StreamEdge targetEdge, StreamPartitioner<?> newPartitioner) {
        if (newPartitioner == null || targetEdge == null) {
            return;
        }
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (newPartitioner instanceof ForwardPartitioner
                && !StreamingJobGraphGenerator.isChainable(targetEdge, streamGraph)) {
            // For non chainable edges, we change ForwardPartitioner to RescalePartitioner to avoid
            // parallelism decider that does not match expectations.
            targetEdge.setPartitioner(new RescalePartitioner<>());
        } else {
            targetEdge.setPartitioner(newPartitioner);
        }
        if (streamGraph.isDynamic() && targetEdge.getPartitioner() instanceof ForwardPartitioner) {
            mergeForwardGroups(sourceNodeId, targetNodeId);
        }
        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(sourceNodeId);
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            output.setPartitioner(targetEdge.getPartitioner());
        }
    }

    private void mergeForwardGroups(Integer sourceNodeId, Integer targetNodeId) {
        StreamNodeForwardGroup sourceForwardGroup =
                forwardGroupsByStartNodeIdCache.get(sourceNodeId);
        StreamNodeForwardGroup targetForwardGroup =
                forwardGroupsByStartNodeIdCache.get(targetNodeId);
        if (sourceForwardGroup == null || targetForwardGroup == null) {
            return;
        }
        checkState(sourceForwardGroup.mergeForwardGroup(targetForwardGroup));
        // Update forwardGroupsByStartNodeIdCache.
        targetForwardGroup
                .getStartNodes()
                .forEach(
                        startNode ->
                                forwardGroupsByStartNodeIdCache.put(
                                        startNode.getId(), sourceForwardGroup));
    }

    private StreamEdge getStreamEdge(Integer sourceId, Integer targetId, String edgeId) {
        for (StreamEdge edge : streamGraph.getStreamEdges(sourceId, targetId)) {
            if (edge.getEdgeId().equals(edgeId)) {
                return edge;
            }
        }
        return null;
    }
}

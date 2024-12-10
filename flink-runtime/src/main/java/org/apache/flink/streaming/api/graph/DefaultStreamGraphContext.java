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
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.streaming.api.graph.util.ImmutableStreamGraph;
import org.apache.flink.streaming.api.graph.util.StreamEdgeUpdateRequestInfo;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil.canTargetMergeIntoSourceForwardGroup;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation for {@link StreamGraphContext}. */
@Internal
public class DefaultStreamGraphContext implements StreamGraphContext {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamGraphContext.class);

    private final StreamGraph streamGraph;
    private final ImmutableStreamGraph immutableStreamGraph;

    // The attributes below are reused from AdaptiveGraphManager as AdaptiveGraphManager also needs
    // to use the modified information to create the job vertex.

    // A modifiable map which records the ids of stream nodes to their forward groups.
    // When stream edge's partitioner is modified to forward, we need get forward groups by source
    // and target node id and merge them.
    private final Map<Integer, StreamNodeForwardGroup> steamNodeIdToForwardGroupMap;
    // A read only map which records the id of stream node which job vertex is created, used to
    // ensure that the stream nodes involved in the modification have not yet created job vertices.
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;
    // A modifiable map which key is the id of stream node which creates the non-chained output, and
    // value is the stream edge connected to the stream node and the non-chained output subscribed
    // by the edge. It is used to verify whether the edge being modified is subscribed to a reused
    // output and ensures that modifications to StreamEdge can be synchronized to NonChainedOutput
    // as they reuse some attributes.
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches;

    public DefaultStreamGraphContext(
            StreamGraph streamGraph,
            Map<Integer, StreamNodeForwardGroup> steamNodeIdToForwardGroupMap,
            Map<Integer, Integer> frozenNodeToStartNodeMap,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputsCaches) {
        this.streamGraph = checkNotNull(streamGraph);
        this.steamNodeIdToForwardGroupMap = checkNotNull(steamNodeIdToForwardGroupMap);
        this.frozenNodeToStartNodeMap = checkNotNull(frozenNodeToStartNodeMap);
        this.opIntermediateOutputsCaches = checkNotNull(opIntermediateOutputsCaches);
        this.immutableStreamGraph = new ImmutableStreamGraph(this.streamGraph);
    }

    @Override
    public ImmutableStreamGraph getStreamGraph() {
        return immutableStreamGraph;
    }

    @Override
    public @Nullable StreamOperatorFactory<?> getOperatorFactory(Integer streamNodeId) {
        return streamGraph.getStreamNode(streamNodeId).getOperatorFactory();
    }

    @Override
    public boolean modifyStreamEdge(List<StreamEdgeUpdateRequestInfo> requestInfos) {
        // We first verify the legality of all requestInfos to ensure that all requests can be
        // modified atomically.
        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            if (!validateStreamEdgeUpdateRequest(requestInfo)) {
                return false;
            }
        }

        for (StreamEdgeUpdateRequestInfo requestInfo : requestInfos) {
            StreamEdge targetEdge =
                    getStreamEdge(
                            requestInfo.getSourceId(),
                            requestInfo.getTargetId(),
                            requestInfo.getEdgeId());
            StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();
            if (newPartitioner != null) {
                modifyOutputPartitioner(targetEdge, newPartitioner);
            }
        }

        return true;
    }

    private boolean validateStreamEdgeUpdateRequest(StreamEdgeUpdateRequestInfo requestInfo) {
        Integer sourceNodeId = requestInfo.getSourceId();
        Integer targetNodeId = requestInfo.getTargetId();

        StreamEdge targetEdge = getStreamEdge(sourceNodeId, targetNodeId, requestInfo.getEdgeId());

        if (targetEdge == null) {
            return false;
        }

        // Modification is not allowed when the subscribing output is reused.
        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(sourceNodeId);
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            Set<StreamEdge> consumerStreamEdges =
                    opIntermediateOutputs.entrySet().stream()
                            .filter(entry -> entry.getValue().equals(output))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toSet());
            if (consumerStreamEdges.size() != 1) {
                LOG.info(
                        "Skip modifying edge {} because the subscribing output is reused.",
                        targetEdge);
                return false;
            }
        }

        if (frozenNodeToStartNodeMap.containsKey(targetNodeId)) {
            LOG.info(
                    "Skip modifying edge {} because the target node with id {} is in frozen list.",
                    targetEdge,
                    targetNodeId);
            return false;
        }

        StreamPartitioner<?> newPartitioner = requestInfo.getOutputPartitioner();

        if (newPartitioner != null) {
            if (targetEdge.getPartitioner().getClass().equals(ForwardPartitioner.class)) {
                LOG.info(
                        "Modification for edge {} is not allowed as the origin partitioner is ForwardPartitioner.",
                        targetEdge);
                return false;
            }
            if (newPartitioner.getClass().equals(ForwardPartitioner.class)
                    && !canTargetMergeIntoSourceForwardGroup(
                            steamNodeIdToForwardGroupMap.get(targetEdge.getSourceId()),
                            steamNodeIdToForwardGroupMap.get(targetEdge.getTargetId()))) {
                LOG.info(
                        "Skip modifying edge {} because forward groups can not be merged.",
                        targetEdge);
                return false;
            }
        }

        return true;
    }

    private void modifyOutputPartitioner(
            StreamEdge targetEdge, StreamPartitioner<?> newPartitioner) {
        if (newPartitioner == null || targetEdge == null) {
            return;
        }
        StreamPartitioner<?> oldPartitioner = targetEdge.getPartitioner();
        targetEdge.setPartitioner(newPartitioner);

        if (targetEdge.getPartitioner() instanceof ForwardPartitioner) {
            tryConvertForwardPartitionerAndMergeForwardGroup(targetEdge);
        }

        // The partitioner in NonChainedOutput derived from the consumer edge, so we need to ensure
        // that any modifications to the partitioner of consumer edge are synchronized with
        // NonChainedOutput.
        Map<StreamEdge, NonChainedOutput> opIntermediateOutputs =
                opIntermediateOutputsCaches.get(targetEdge.getSourceId());
        NonChainedOutput output =
                opIntermediateOutputs != null ? opIntermediateOutputs.get(targetEdge) : null;
        if (output != null) {
            output.setPartitioner(targetEdge.getPartitioner());
        }
        LOG.info(
                "The original partitioner of the edge {} is: {} , requested change to: {} , and finally modified to: {}.",
                targetEdge,
                oldPartitioner,
                newPartitioner,
                targetEdge.getPartitioner());
    }

    private void tryConvertForwardPartitionerAndMergeForwardGroup(StreamEdge targetEdge) {
        checkState(targetEdge.getPartitioner() instanceof ForwardPartitioner);
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (canConvertToForwardPartitioner(targetEdge)) {
            targetEdge.setPartitioner(new ForwardPartitioner<>());
            checkState(mergeForwardGroups(sourceNodeId, targetNodeId));
        } else if (targetEdge.getPartitioner() instanceof ForwardForUnspecifiedPartitioner) {
            targetEdge.setPartitioner(new RescalePartitioner<>());
        } else if (targetEdge.getPartitioner() instanceof ForwardForConsecutiveHashPartitioner) {
            targetEdge.setPartitioner(
                    ((ForwardForConsecutiveHashPartitioner<?>) targetEdge.getPartitioner())
                            .getHashPartitioner());
        } else {
            // For ForwardPartitioner, StreamGraphContext can ensure the success of the merge.
            checkState(mergeForwardGroups(sourceNodeId, targetNodeId));
        }
    }

    private boolean canConvertToForwardPartitioner(StreamEdge targetEdge) {
        Integer sourceNodeId = targetEdge.getSourceId();
        Integer targetNodeId = targetEdge.getTargetId();
        if (targetEdge.getPartitioner() instanceof ForwardForUnspecifiedPartitioner) {
            return !frozenNodeToStartNodeMap.containsKey(sourceNodeId)
                    && StreamingJobGraphGenerator.isChainable(targetEdge, streamGraph, true)
                    && canTargetMergeIntoSourceForwardGroup(
                            steamNodeIdToForwardGroupMap.get(sourceNodeId),
                            steamNodeIdToForwardGroupMap.get(targetNodeId));
        } else if (targetEdge.getPartitioner() instanceof ForwardForConsecutiveHashPartitioner) {
            return canTargetMergeIntoSourceForwardGroup(
                    steamNodeIdToForwardGroupMap.get(sourceNodeId),
                    steamNodeIdToForwardGroupMap.get(targetNodeId));
        } else {
            return false;
        }
    }

    private boolean mergeForwardGroups(Integer sourceNodeId, Integer targetNodeId) {
        StreamNodeForwardGroup sourceForwardGroup = steamNodeIdToForwardGroupMap.get(sourceNodeId);
        StreamNodeForwardGroup forwardGroupToMerge = steamNodeIdToForwardGroupMap.get(targetNodeId);
        if (sourceForwardGroup == null || forwardGroupToMerge == null) {
            return false;
        }
        if (!sourceForwardGroup.mergeForwardGroup(forwardGroupToMerge)) {
            return false;
        }
        // Update steamNodeIdToForwardGroupMap.
        forwardGroupToMerge
                .getVertexIds()
                .forEach(nodeId -> steamNodeIdToForwardGroupMap.put(nodeId, sourceForwardGroup));
        return true;
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

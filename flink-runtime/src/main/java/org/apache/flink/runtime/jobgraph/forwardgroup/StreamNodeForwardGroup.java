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

package org.apache.flink.runtime.jobgraph.forwardgroup;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.VertexGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class StreamNodeForwardGroup {

    private int parallelism = ExecutionConfig.PARALLELISM_DEFAULT;

    private int maxParallelism = JobVertex.MAX_PARALLELISM_DEFAULT;

    private final Set<Integer> startNodeIds = new HashSet<>();

    private final Map<Integer, List<Integer>> chainedNodeIdMap = new HashMap<>();

    public StreamNodeForwardGroup(final Set<StreamNode> startNodes) {
        checkNotNull(startNodes);

        Set<Integer> configuredParallelisms =
                startNodes.stream()
                        .filter(
                                startNode -> {
                                    startNodeIds.add(startNode.getId());
                                    return startNode.getParallelism() > 0;
                                })
                        .map(StreamNode::getParallelism)
                        .collect(Collectors.toSet());

        checkState(configuredParallelisms.size() <= 1);

        if (configuredParallelisms.size() == 1) {
            this.parallelism = configuredParallelisms.iterator().next();
        }

        Set<Integer> configuredMaxParallelisms =
                startNodes.stream()
                        .map(StreamNode::getMaxParallelism)
                        .filter(val -> val > 0)
                        .collect(Collectors.toSet());

        if (!configuredMaxParallelisms.isEmpty()) {
            this.maxParallelism = Collections.min(configuredMaxParallelisms);
            checkState(
                    parallelism == ExecutionConfig.PARALLELISM_DEFAULT
                            || maxParallelism >= parallelism,
                    "There is a start node in the forward group whose maximum parallelism is smaller than the group's parallelism");
        }
    }

    public void setParallelism(int parallelism) {
        checkState(this.parallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        this.parallelism = parallelism;
    }

    public boolean isParallelismDecided() {
        return parallelism > 0;
    }

    public int getParallelism() {
        checkState(isParallelismDecided());
        return parallelism;
    }

    public boolean isMaxParallelismDecided() {
        return maxParallelism > 0;
    }

    public int getMaxParallelism() {
        checkState(isMaxParallelismDecided());
        return maxParallelism;
    }

    public int size() {
        return startNodeIds.size();
    }

    public Set<Integer> getStartNodeIds() {
        return startNodeIds;
    }

    public Map<Integer, List<Integer>> getChainedNodeIdMap() {
        return chainedNodeIdMap;
    }

    public void putChainedNodeIds(Integer startNodeId, List<Integer> chainedNodeIds) {
        this.chainedNodeIdMap.put(startNodeId, chainedNodeIds);
    }

    public void mergeForwardGroup(StreamNodeForwardGroup forwardGroup) {
        if (forwardGroup == null) {
            return;
        }
        this.startNodeIds.addAll(forwardGroup.getStartNodeIds());
        this.chainedNodeIdMap.putAll(forwardGroup.getChainedNodeIdMap());
    }

    public static Map<Integer, StreamNodeForwardGroup> computeForwardGroup(
            final Set<Integer> topologicallySortedStartNodeIds,
            final Function<StreamNode, Set<StreamNode>> forwardProducersRetriever,
            final Function<Integer, List<StreamNode>> chainedNodeRetriever,
            StreamGraph streamGraph) {
        final Map<StreamNode, Set<StreamNode>> nodeToGroup = new IdentityHashMap<>();
        for (Integer startNodeId : topologicallySortedStartNodeIds) {
            StreamNode currentNode = streamGraph.getStreamNode(startNodeId);
            Set<StreamNode> currentGroup = new HashSet<>();
            currentGroup.add(currentNode);
            nodeToGroup.put(currentNode, currentGroup);
            for (StreamNode producerNode : forwardProducersRetriever.apply(currentNode)) {
                final Set<StreamNode> producerGroup = nodeToGroup.get(producerNode);
                if (producerGroup == null) {
                    throw new IllegalStateException(
                            "Producer task "
                                    + producerNode.getId()
                                    + " forward group is null"
                                    + " while calculating forward group for the consumer task "
                                    + currentNode.getId()
                                    + ". This should be a forward group building bug.");
                }
                if (currentGroup != producerGroup) {
                    currentGroup =
                            VertexGroupComputeUtil.mergeVertexGroups(
                                    currentGroup, producerGroup, nodeToGroup);
                }
            }
        }
        final Map<Integer, StreamNodeForwardGroup> result = new HashMap<>();
        for (Set<StreamNode> nodeGroup : VertexGroupComputeUtil.uniqueVertexGroups(nodeToGroup)) {
            if (!nodeGroup.isEmpty()) {
                StreamNodeForwardGroup streamNodeForwardGroup =
                        new StreamNodeForwardGroup(nodeGroup);
                for (Integer startNodeId : streamNodeForwardGroup.getStartNodeIds()) {
                    List<Integer> chainedNodeIds =
                            chainedNodeRetriever.apply(startNodeId).stream()
                                    .map(StreamNode::getId)
                                    .collect(Collectors.toList());
                    streamNodeForwardGroup.putChainedNodeIds(startNodeId, chainedNodeIds);
                    result.put(startNodeId, streamNodeForwardGroup);
                }
            }
        }
        return result;
    }
}

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

package org.apache.flink.streaming.api.graph.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.jobgraph.InputOutputFormatContainer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Helper class to help maintain the information of an operator chain. */
@Internal
public class OperatorChainInfo {
    private final Integer startNodeId;
    private final Map<Integer, List<ChainedOperatorHashInfo>> chainedOperatorHashes;
    private final Map<Integer, ChainedSourceInfo> chainedSources;
    private final Map<Integer, ResourceSpec> chainedMinResources;
    private final Map<Integer, ResourceSpec> chainedPreferredResources;
    private final Map<Integer, String> chainedNames;

    /** The {@link OperatorInfo}s, key is the id of the stream node. */
    private final Map<Integer, OperatorInfo> chainedOperatorInfos;

    private final List<OperatorCoordinator.Provider> coordinatorProviders;
    private final List<StreamNode> chainedNodes;
    private final List<StreamEdge> transitiveOutEdges;
    private final List<StreamEdge> transitiveInEdges;

    private InputOutputFormatContainer inputOutputFormatContainer = null;

    public OperatorChainInfo(int startNodeId) {
        this.startNodeId = startNodeId;
        this.chainedOperatorHashes = new HashMap<>();
        this.coordinatorProviders = new ArrayList<>();
        this.chainedSources = new HashMap<>();
        this.chainedMinResources = new HashMap<>();
        this.chainedPreferredResources = new HashMap<>();
        this.chainedNames = new HashMap<>();
        this.chainedNodes = new ArrayList<>();
        this.transitiveOutEdges = new ArrayList<>();
        this.transitiveInEdges = new ArrayList<>();
        this.chainedOperatorInfos = new HashMap<>();
    }

    public Integer getStartNodeId() {
        return startNodeId;
    }

    public List<ChainedOperatorHashInfo> getChainedOperatorHashes(int startNodeId) {
        return chainedOperatorHashes.get(startNodeId);
    }

    public void addCoordinatorProvider(OperatorCoordinator.Provider coordinator) {
        coordinatorProviders.add(coordinator);
    }

    public List<OperatorCoordinator.Provider> getCoordinatorProviders() {
        return coordinatorProviders;
    }

    public Map<Integer, ChainedSourceInfo> getChainedSources() {
        return chainedSources;
    }

    public OperatorID addNodeToChain(
            int currentNodeId, String operatorName, JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        StreamNode streamNode = streamGraph.getStreamNode(currentNodeId);

        recordChainedNode(streamNode);

        List<ChainedOperatorHashInfo> operatorHashes =
                chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

        byte[] primaryHashBytes = jobVertexBuildContext.getHash(currentNodeId);

        for (byte[] legacyHash : jobVertexBuildContext.getLegacyHashes(currentNodeId)) {
            operatorHashes.add(
                    new ChainedOperatorHashInfo(primaryHashBytes, legacyHash, streamNode));
        }

        streamNode
                .getCoordinatorProvider(operatorName, new OperatorID(primaryHashBytes))
                .map(coordinatorProviders::add);

        return new OperatorID(primaryHashBytes);
    }

    public void setTransitiveOutEdges(final List<StreamEdge> transitiveOutEdges) {
        this.transitiveOutEdges.addAll(transitiveOutEdges);
    }

    public List<StreamEdge> getTransitiveOutEdges() {
        return transitiveOutEdges;
    }

    public void recordChainedNode(StreamNode streamNode) {
        chainedNodes.add(streamNode);
    }

    public OperatorChainInfo newChain(Integer startNodeId) {
        return new OperatorChainInfo(startNodeId);
    }

    public List<StreamNode> getAllChainedNodes() {
        return chainedNodes;
    }

    public boolean hasFormatContainer() {
        return inputOutputFormatContainer != null;
    }

    public InputOutputFormatContainer getOrCreateFormatContainer() {
        if (inputOutputFormatContainer == null) {
            inputOutputFormatContainer =
                    new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader());
        }
        return inputOutputFormatContainer;
    }

    public void addChainedSource(StreamNode sourceNode, ChainedSourceInfo chainedSourceInfo) {
        recordChainedNode(sourceNode);
        chainedSources.put(sourceNode.getId(), chainedSourceInfo);
    }

    public void addChainedMinResources(Integer sourceNodeId, ResourceSpec resourceSpec) {
        chainedMinResources.put(sourceNodeId, resourceSpec);
    }

    public ResourceSpec getChainedMinResources(Integer sourceNodeId) {
        return chainedMinResources.get(sourceNodeId);
    }

    public void addChainedPreferredResources(Integer sourceNodeId, ResourceSpec resourceSpec) {
        chainedPreferredResources.put(sourceNodeId, resourceSpec);
    }

    public ResourceSpec getChainedPreferredResources(Integer sourceNodeId) {
        return chainedPreferredResources.get(sourceNodeId);
    }

    public String getChainedName(Integer streamNodeId) {
        return chainedNames.get(streamNodeId);
    }

    public Map<Integer, String> getChainedNames() {
        return chainedNames;
    }

    public void addChainedName(Integer streamNodeId, String chainedName) {
        this.chainedNames.put(streamNodeId, chainedName);
    }

    public void addTransitiveInEdge(StreamEdge streamEdge) {
        transitiveInEdges.add(streamEdge);
    }

    public List<StreamEdge> getTransitiveInEdges() {
        return transitiveInEdges;
    }

    public OperatorInfo getOperatorInfo(Integer nodeId) {
        return chainedOperatorInfos.get(nodeId);
    }

    public OperatorInfo createAndGetOperatorInfo(Integer nodeId, OperatorID operatorId) {
        OperatorInfo operatorInfo = new OperatorInfo(operatorId);
        chainedOperatorInfos.put(nodeId, operatorInfo);
        return operatorInfo;
    }

    public Map<Integer, OperatorInfo> getOperatorInfos() {
        return Collections.unmodifiableMap(chainedOperatorInfos);
    }
}

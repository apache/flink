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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.SerializedValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Helper class encapsulates all necessary information and configurations required during the
 * construction of job vertices.
 */
@Internal
public class JobVertexBuildContext {

    private final StreamGraph streamGraph;

    /**
     * The {@link OperatorChainInfo}s, key is the start node id of the chain. It should be ordered,
     * as the implementation of the incremental generator relies on its order to create the
     * JobVertex.
     */
    private final Map<Integer, OperatorChainInfo> chainInfosInOrder;

    /** The {@link OperatorInfo}s, key is the id of the stream node. */
    private final Map<Integer, OperatorInfo> operatorInfos;

    // This map's key represents the starting node id of each chain. Note that this includes not
    // only the usual head node of the chain but also the ids of chain sources which are used by
    // multi-input.
    private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

    // The created JobVertex, the key is start node id.  It records the order in which the JobVertex
    // is created, and some functions depend on it.
    private final Map<Integer, JobVertex> jobVerticesInOrder;

    // Futures for the serialization of operator coordinators.
    private final Map<
                    JobVertexID,
                    List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            coordinatorSerializationFuturesPerJobVertex;

    // The order of StreamEdge connected to other vertices should be consistent with the order in
    // which JobEdge was created.
    private final List<StreamEdge> physicalEdgesInOrder;

    // We use AtomicBoolean to track the existence of HybridResultPartition during the incremental
    // JobGraph generation process introduced by AdaptiveGraphManager. It is essential to globally
    // monitor changes to this variable, thus necessitating the use of a Boolean object instead of a
    // primitive boolean.
    private final AtomicBoolean hasHybridResultPartition;

    private final Map<Integer, byte[]> hashes;

    private final List<Map<Integer, byte[]>> legacyHashes;

    public JobVertexBuildContext(
            StreamGraph streamGraph,
            AtomicBoolean hasHybridResultPartition,
            Map<Integer, byte[]> hashes,
            List<Map<Integer, byte[]>> legacyHashes) {
        this.streamGraph = streamGraph;
        this.hashes = hashes;
        this.legacyHashes = legacyHashes;
        this.chainInfosInOrder = new LinkedHashMap<>();
        this.jobVerticesInOrder = new LinkedHashMap<>();
        this.physicalEdgesInOrder = new ArrayList<>();
        this.hasHybridResultPartition = hasHybridResultPartition;
        this.coordinatorSerializationFuturesPerJobVertex = new HashMap<>();
        this.chainedConfigs = new HashMap<>();
        this.operatorInfos = new HashMap<>();
    }

    public void addChainInfo(Integer startNodeId, OperatorChainInfo chainInfo) {
        chainInfosInOrder.put(startNodeId, chainInfo);
    }

    public OperatorChainInfo getChainInfo(Integer startNodeId) {
        return chainInfosInOrder.get(startNodeId);
    }

    public Map<Integer, OperatorChainInfo> getChainInfosInOrder() {
        return chainInfosInOrder;
    }

    public OperatorInfo getOperatorInfo(Integer nodeId) {
        return operatorInfos.get(nodeId);
    }

    public OperatorInfo createAndGetOperatorInfo(Integer nodeId) {
        OperatorInfo operatorInfo = new OperatorInfo();
        operatorInfos.put(nodeId, operatorInfo);
        return operatorInfo;
    }

    public Map<Integer, OperatorInfo> getOperatorInfos() {
        return operatorInfos;
    }

    public StreamGraph getStreamGraph() {
        return streamGraph;
    }

    public boolean hasHybridResultPartition() {
        return hasHybridResultPartition.get();
    }

    public void setHasHybridResultPartition(boolean hasHybridResultPartition) {
        this.hasHybridResultPartition.set(hasHybridResultPartition);
    }

    public void addPhysicalEdgesInOrder(StreamEdge edge) {
        physicalEdgesInOrder.add(edge);
    }

    public List<StreamEdge> getPhysicalEdgesInOrder() {
        return physicalEdgesInOrder;
    }

    public void addJobVertex(Integer startNodeId, JobVertex jobVertex) {
        jobVerticesInOrder.put(startNodeId, jobVertex);
    }

    public Map<Integer, JobVertex> getJobVerticesInOrder() {
        return jobVerticesInOrder;
    }

    public JobVertex getJobVertex(Integer startNodeId) {
        return jobVerticesInOrder.get(startNodeId);
    }

    public void putCoordinatorSerializationFutures(
            JobVertexID vertexId,
            List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                    serializationFutures) {
        coordinatorSerializationFuturesPerJobVertex.put(vertexId, serializationFutures);
    }

    public Map<JobVertexID, List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
            getCoordinatorSerializationFuturesPerJobVertex() {
        return coordinatorSerializationFuturesPerJobVertex;
    }

    public Map<Integer, Map<Integer, StreamConfig>> getChainedConfigs() {
        return chainedConfigs;
    }

    public Map<Integer, StreamConfig> getOrCreateChainedConfig(Integer streamNodeId) {
        return chainedConfigs.computeIfAbsent(streamNodeId, key -> new HashMap<>());
    }

    public byte[] getHash(Integer streamNodeId) {
        return hashes.get(streamNodeId);
    }

    public List<byte[]> getLegacyHashes(Integer streamNodeId) {
        List<byte[]> hashes = new ArrayList<>();
        for (Map<Integer, byte[]> legacyHash : legacyHashes) {
            hashes.add(legacyHash.get(streamNodeId));
        }
        return hashes;
    }
}

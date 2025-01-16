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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createAndInitializeJobGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChain;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createSourceChainInfo;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainableSource;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.serializeOperatorCoordinatorsAndStreamConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setAllOperatorNonChainedOutputsConfigs;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharingAndCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

/** Default implementation for {@link AdaptiveGraphGenerator}. */
@Internal
public class AdaptiveGraphManager
        implements AdaptiveGraphGenerator, StreamGraphContext.StreamGraphUpdateListener {

    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;

    private final StreamGraphHasher defaultStreamGraphHasher;

    private final List<StreamGraphHasher> legacyStreamGraphHasher;

    private final Executor serializationExecutor;

    private final AtomicInteger vertexIndexId;

    private final StreamGraphContext streamGraphContext;

    private final Map<Integer, byte[]> hashes;

    private final List<Map<Integer, byte[]>> legacyHashes;

    // Records the ids of stream nodes of which the job vertices are already created.
    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    // When the downstream vertex is not created, we need to cache the output.
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> intermediateOutputsCaches;

    // Records the id of the stream node that produces the IntermediateDataSet.
    private final Map<IntermediateDataSetID, Integer> intermediateDataSetIdToProducerMap;

    // Records the id of the IntermediateDataSet to the stream edges that subscribe to it.
    private final Map<IntermediateDataSetID, List<StreamEdge>>
            intermediateDataSetIdToOutputEdgesMap;

    private final Map<String, IntermediateDataSet> consumerEdgeIdToIntermediateDataSetMap =
            new HashMap<>();

    // Records the ids of stream nodes in the StreamNodeForwardGroup.
    // When stream edge's partitioner is modified to forward, we need get forward groups by source
    // and target node id.
    private final Map<Integer, StreamNodeForwardGroup> steamNodeIdToForwardGroupMap;

    // Records the chain info that is not ready to create the job vertex, the key is the start node
    // in this chain.
    private final Map<Integer, OperatorChainInfo> pendingChainEntryPoints;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, List<Integer>> jobVertexToChainedStreamNodeIdsMap;

    // We need cache all job vertices to create JobEdge for downstream vertex.
    private final Map<Integer, JobVertex> startNodeToJobVertexMap;

    private final Map<Integer, JobVertexID> streamNodeIdsToJobVertexMap;

    // Records the ID of the job vertex that has completed execution.
    private final Set<JobVertexID> finishedJobVertices;

    // Records the ID of the stream nodes that has completed execution.
    private final Set<Integer> finishedStreamNodeIds;

    private final AtomicBoolean hasHybridResultPartition;

    private final SlotSharingGroup defaultSlotSharingGroup;

    private String streamGraphJson;

    public AdaptiveGraphManager(
            ClassLoader userClassloader, StreamGraph streamGraph, Executor serializationExecutor) {
        preValidate(streamGraph, userClassloader);
        this.streamGraph = streamGraph;
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);

        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHasher = Collections.singletonList(new StreamGraphUserHashHasher());

        this.hashes = new HashMap<>();
        this.legacyHashes = Collections.singletonList(new HashMap<>());

        this.startNodeToJobVertexMap = new LinkedHashMap<>();
        this.pendingChainEntryPoints = new TreeMap<>();

        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.intermediateOutputsCaches = new HashMap<>();
        this.intermediateDataSetIdToProducerMap = new HashMap<>();
        this.intermediateDataSetIdToOutputEdgesMap = new HashMap<>();
        this.steamNodeIdToForwardGroupMap = new HashMap<>();

        this.vertexIndexId = new AtomicInteger(0);

        this.hasHybridResultPartition = new AtomicBoolean(false);

        this.jobVertexToStartNodeMap = new HashMap<>();
        this.jobVertexToChainedStreamNodeIdsMap = new HashMap<>();
        this.streamNodeIdsToJobVertexMap = new HashMap<>();

        this.finishedJobVertices = new HashSet<>();
        this.finishedStreamNodeIds = new HashSet<>();

        this.streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        steamNodeIdToForwardGroupMap,
                        frozenNodeToStartNodeMap,
                        intermediateOutputsCaches,
                        consumerEdgeIdToIntermediateDataSetMap,
                        finishedStreamNodeIds,
                        userClassloader,
                        this);

        this.jobGraph = createAndInitializeJobGraph(streamGraph, streamGraph.getJobID());

        this.defaultSlotSharingGroup = new SlotSharingGroup();

        initialization();
    }

    @Override
    public JobGraph getJobGraph() {
        return this.jobGraph;
    }

    @Override
    public StreamGraphContext getStreamGraphContext() {
        return streamGraphContext;
    }

    @Override
    public List<JobVertex> onJobVertexFinished(JobVertexID finishedJobVertexId) {
        this.finishedJobVertices.add(finishedJobVertexId);
        List<StreamNode> streamNodes = new ArrayList<>();
        for (StreamEdge outEdge : getOutputEdgesByVertexId(finishedJobVertexId)) {
            streamNodes.add(streamGraph.getStreamNode(outEdge.getTargetId()));
        }
        return createJobVerticesAndUpdateGraph(streamNodes);
    }

    public void addFinishedStreamNodeIds(List<Integer> finishedStreamNodeIds) {
        this.finishedStreamNodeIds.addAll(finishedStreamNodeIds);
    }

    /**
     * Retrieves the StreamNodeForwardGroup which provides a stream node level ForwardGroup.
     *
     * @param jobVertexId The ID of the JobVertex.
     * @return An instance of {@link StreamNodeForwardGroup}.
     */
    public StreamNodeForwardGroup getStreamNodeForwardGroupByVertexId(JobVertexID jobVertexId) {
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        return steamNodeIdToForwardGroupMap.get(startNodeId);
    }

    /**
     * Retrieves the number of operators that have not yet been converted to job vertex.
     *
     * @return The number of unconverted operators.
     */
    public int getPendingOperatorsCount() {
        return streamGraph.getStreamNodes().size() - frozenNodeToStartNodeMap.size();
    }

    /**
     * Retrieves the IDs of stream nodes that belong to the given job vertex.
     *
     * @param jobVertexId The ID of the JobVertex.
     * @return A list of IDs of stream nodes that belong to the job vertex.
     */
    public List<Integer> getStreamNodeIdsByJobVertexId(JobVertexID jobVertexId) {
        return jobVertexToChainedStreamNodeIdsMap.get(jobVertexId);
    }

    /**
     * Retrieves the ID of the stream node that produces the IntermediateDataSet.
     *
     * @param intermediateDataSetId The ID of the IntermediateDataSet.
     * @return The ID of the stream node that produces the IntermediateDataSet.
     */
    public Integer getProducerStreamNodeId(IntermediateDataSetID intermediateDataSetId) {
        return intermediateDataSetIdToProducerMap.get(intermediateDataSetId);
    }

    /**
     * Retrieves the stream edges that subscribe to the IntermediateDataSet.
     *
     * @param intermediateDataSetId the ID of the IntermediateDataSet
     * @return the stream edges that subscribe to the IntermediateDataSet
     */
    public List<StreamEdge> getOutputStreamEdges(IntermediateDataSetID intermediateDataSetId) {
        return Collections.unmodifiableList(
                intermediateDataSetIdToOutputEdgesMap.get(intermediateDataSetId));
    }

    public Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (isNodeFrozen(streamNodeId)) {
            Integer startNodeId = getStartNodeId(streamNodeId);
            return Optional.of(startNodeToJobVertexMap.get(startNodeId).getID());
        }
        return Optional.empty();
    }

    private List<StreamEdge> getOutputEdgesByVertexId(JobVertexID jobVertexId) {
        JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);
        List<StreamEdge> outputEdges = new ArrayList<>();
        for (IntermediateDataSet result : jobVertex.getProducedDataSets()) {
            outputEdges.addAll(intermediateDataSetIdToOutputEdgesMap.get(result.getId()));
        }
        return outputEdges;
    }

    private void initialization() {
        List<StreamNode> sourceNodes = new ArrayList<>();
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            sourceNodes.add(streamGraph.getStreamNode(sourceNodeId));
        }
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary();
        }
        createJobVerticesAndUpdateGraph(sourceNodes);
    }

    private List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes) {
        final JobVertexBuildContext jobVertexBuildContext =
                new JobVertexBuildContext(
                        jobGraph,
                        streamGraph,
                        hasHybridResultPartition,
                        hashes,
                        legacyHashes,
                        defaultSlotSharingGroup);

        createOperatorChainInfos(streamNodes, jobVertexBuildContext);

        recordCreatedJobVerticesInfo(jobVertexBuildContext);

        generateConfigForJobVertices(jobVertexBuildContext);

        generateStreamGraphJson();

        return new ArrayList<>(jobVertexBuildContext.getJobVerticesInOrder().values());
    }

    private void generateConfigForJobVertices(JobVertexBuildContext jobVertexBuildContext) {
        // this may be used by uncreated down stream vertex.
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();

        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        connectToFinishedUpStreamVertex(jobVertexBuildContext);

        setPhysicalEdges(jobVertexBuildContext);

        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);

        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);

        setSlotSharingAndCoLocation(jobVertexBuildContext);

        setManagedMemoryFraction(jobVertexBuildContext);

        addVertexIndexPrefixInVertexName(jobVertexBuildContext, vertexIndexId);

        setVertexDescription(jobVertexBuildContext);

        serializeOperatorCoordinatorsAndStreamConfig(serializationExecutor, jobVertexBuildContext);
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        jobVertexBuildContext
                .getJobVerticesInOrder()
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId, opIntermediateOutputs, jobVertexBuildContext));
    }

    /**
     * Sets non-chained outputs config for vertex.
     *
     * <p>Unlike {@link StreamingJobGraphGenerator #setVertexNonChainedOutputsConfig}, the
     * downstream job vertex of the specific vertex in this method may not be created. In this case,
     * when the downstream vertex has been created, a connection to the downstream will be created,
     * otherwise an intermediate data set will be cached for it to create connection later.
     *
     * @param startNodeId the start node id for the specific vertex
     * @param opIntermediateOutputs the intermediate outputs for stream nodes
     * @param jobVertexBuildContext job vertex build context
     */
    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {

        OperatorChainInfo chainInfo = jobVertexBuildContext.getChainInfo(startNodeId);
        StreamConfig config = chainInfo.getOperatorInfo(startNodeId).getVertexConfig();
        List<StreamEdge> transitiveOutEdges = chainInfo.getTransitiveOutEdges();
        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();

        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            // When a downstream vertex has been created, a connection to the downstream will be
            // created, otherwise only an IntermediateDataSet will be created for it.
            if (jobVertexBuildContext.getJobVerticesInOrder().containsKey(edge.getTargetId())) {
                connect(startNodeId, edge, output, startNodeToJobVertexMap, jobVertexBuildContext);
            } else {
                JobVertex jobVertex =
                        jobVertexBuildContext.getJobVerticesInOrder().get(startNodeId);
                IntermediateDataSet dataSet =
                        jobVertex.getOrCreateResultDataSet(
                                output.getDataSetId(), output.getPartitionType());
                DistributionPattern distributionPattern =
                        edge.getPartitioner().isPointwise()
                                ? DistributionPattern.POINTWISE
                                : DistributionPattern.ALL_TO_ALL;
                dataSet.configure(
                        distributionPattern,
                        edge.getPartitioner().isBroadcast(),
                        edge.getPartitioner().getClass().equals(ForwardPartitioner.class));
                dataSet.increaseNumJobEdgesToCreate();

                intermediateDataSetIdToOutputEdgesMap
                        .computeIfAbsent(dataSet.getId(), ignored -> new ArrayList<>())
                        .add(edge);
                consumerEdgeIdToIntermediateDataSetMap.put(edge.getEdgeId(), dataSet);
                // we cache the output here for downstream vertex to create jobEdge.
                intermediateOutputsCaches
                        .computeIfAbsent(edge.getSourceId(), k -> new HashMap<>())
                        .put(edge, output);
            }
            intermediateDataSetIdToProducerMap.put(output.getDataSetId(), edge.getSourceId());
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    /**
     * Responds to connect to the upstream job vertex that has completed execution.
     *
     * @param jobVertexBuildContext the job vertex build context.
     */
    private void connectToFinishedUpStreamVertex(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamEdge> transitiveInEdges = chainInfo.getTransitiveInEdges();
            for (StreamEdge transitiveInEdge : transitiveInEdges) {
                NonChainedOutput output =
                        intermediateOutputsCaches
                                .get(transitiveInEdge.getSourceId())
                                .get(transitiveInEdge);
                Integer sourceStartNodeId = getStartNodeId(transitiveInEdge.getSourceId());
                connect(
                        sourceStartNodeId,
                        transitiveInEdge,
                        output,
                        startNodeToJobVertexMap,
                        jobVertexBuildContext);
            }
        }
    }

    private void recordCreatedJobVerticesInfo(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = jobVertexBuildContext.getJobVertex(chainInfo.getStartNodeId());
            startNodeToJobVertexMap.put(chainInfo.getStartNodeId(), jobVertex);
            jobVertexToStartNodeMap.put(jobVertex.getID(), chainInfo.getStartNodeId());
            chainInfo
                    .getAllChainedNodes()
                    .forEach(
                            node -> {
                                frozenNodeToStartNodeMap.put(
                                        node.getId(), chainInfo.getStartNodeId());
                                jobVertexToChainedStreamNodeIdsMap
                                        .computeIfAbsent(
                                                jobVertex.getID(), key -> new ArrayList<>())
                                        .add(node.getId());
                                streamNodeIdsToJobVertexMap.put(node.getId(), jobVertex.getID());
                            });
        }
    }

    private void createOperatorChainInfos(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildAndGetChainEntryPoints(streamNodes, jobVertexBuildContext);

        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        for (OperatorChainInfo info : initialEntryPoints) {
            // We use generateHashesByStreamNodeId to subscribe the visited stream node id and
            // generate hashes for it.
            createChain(
                    info.getStartNodeId(),
                    1,
                    info,
                    chainEntryPoints,
                    false,
                    serializationExecutor,
                    jobVertexBuildContext,
                    this::generateHashesByStreamNodeId);

            // We need to record in-edges connected to the finished job vertex and create the
            // connection later.
            StreamNode startNode = streamGraph.getStreamNode(info.getStartNodeId());
            for (StreamEdge inEdge : startNode.getInEdges()) {
                if (isNodeFrozen(inEdge.getSourceId())) {
                    info.addTransitiveInEdge(inEdge);
                }
            }
        }
    }

    private Map<Integer, OperatorChainInfo> buildAndGetChainEntryPoints(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        Collection<Integer> sourceNodeIds = streamGraph.getSourceIDs();
        for (StreamNode streamNode : streamNodes) {
            int streamNodeId = streamNode.getId();
            if (sourceNodeIds.contains(streamNodeId)
                    && isChainableSource(streamNode, streamGraph)) {
                generateHashesByStreamNodeId(streamNodeId);
                createSourceChainInfo(streamNode, pendingChainEntryPoints, jobVertexBuildContext);
            } else {
                pendingChainEntryPoints.computeIfAbsent(
                        streamNodeId, ignored -> new OperatorChainInfo(streamNodeId));
            }
        }
        return getChainEntryPointsReadyForJobVertex();
    }

    private Map<Integer, OperatorChainInfo> getChainEntryPointsReadyForJobVertex() {
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();
        Iterator<Map.Entry<Integer, OperatorChainInfo>> iterator =
                pendingChainEntryPoints.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, OperatorChainInfo> entry = iterator.next();
            Integer startNodeId = entry.getKey();
            OperatorChainInfo chainInfo = entry.getValue();
            if (!isReadyToCreateJobVertex(chainInfo)) {
                continue;
            }
            chainEntryPoints.put(startNodeId, chainInfo);
            iterator.remove();
        }
        return chainEntryPoints;
    }

    /**
     * Calculates the forward group and reset the parallelism of all nodes in the same forward group
     * to make them the same.
     *
     * <p>Note: Currently, the construction of the Stream Node Forward Group is based on the
     * precondition that the {@link ForwardPartitioner} will not be changed to any other type of
     * partitioner. If this precondition is broken, this method needs to be re-implemented.
     */
    private void setVertexParallelismsForDynamicGraphIfNecessary() {
        // Reset parallelism for chained stream nodes whose parallelism is not configured.
        List<StreamNode> topologicallySortedStreamNodes =
                streamGraph.getStreamNodesSortedTopologicallyFromSources();

        topologicallySortedStreamNodes.forEach(
                streamNode ->
                        streamNode
                                .getOutEdges()
                                .forEach(this::tryConvertPartitionerForChainableEdge));

        topologicallySortedStreamNodes.forEach(
                streamNode -> {
                    if (!streamNode.isParallelismConfigured()
                            && streamGraph.isAutoParallelismEnabled()) {
                        streamNode.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT, false);
                    }
                });

        // The value are stream nodes of upstream that connect target node by forward edge.
        final Map<StreamNode, Set<StreamNode>> forwardProducersByConsumerNodeId = new HashMap<>();

        topologicallySortedStreamNodes.forEach(
                streamNode -> {
                    Set<StreamNode> forwardConsumers =
                            streamNode.getOutEdges().stream()
                                    .filter(
                                            edge ->
                                                    edge.getPartitioner()
                                                            .getClass()
                                                            .equals(ForwardPartitioner.class))
                                    .map(StreamEdge::getTargetId)
                                    .map(streamGraph::getStreamNode)
                                    .collect(Collectors.toSet());
                    for (StreamNode forwardConsumer : forwardConsumers) {
                        forwardProducersByConsumerNodeId.compute(
                                forwardConsumer,
                                (ignored, producers) -> {
                                    if (producers == null) {
                                        producers = new HashSet<>();
                                    }
                                    producers.add(streamNode);
                                    return producers;
                                });
                    }
                });

        this.steamNodeIdToForwardGroupMap.putAll(
                ForwardGroupComputeUtil.computeStreamNodeForwardGroup(
                        topologicallySortedStreamNodes,
                        startNode ->
                                forwardProducersByConsumerNodeId.getOrDefault(
                                        startNode, Collections.emptySet())));

        topologicallySortedStreamNodes.forEach(
                streamNode -> {
                    StreamNodeForwardGroup forwardGroup =
                            steamNodeIdToForwardGroupMap.get(streamNode.getId());
                    // set parallelism for vertices in forward group.
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        streamNode.setParallelism(forwardGroup.getParallelism(), true);
                    }
                    if (forwardGroup != null && forwardGroup.isMaxParallelismDecided()) {
                        streamNode.setMaxParallelism(forwardGroup.getMaxParallelism());
                    }
                });
    }

    private void tryConvertPartitionerForChainableEdge(StreamEdge edge) {
        StreamPartitioner<?> partitioner = edge.getPartitioner();
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner
                || partitioner instanceof ForwardForUnspecifiedPartitioner) {
            if ((streamGraph.getSourceIDs().contains(edge.getSourceId())
                            && isChainableSource(
                                    streamGraph.getStreamNode(edge.getSourceId()), streamGraph))
                    || isChainable(edge, streamGraph)) {
                edge.setPartitioner(new ForwardPartitioner<>());
                // Currently, there is no intra input key correlation for edge with
                // ForwardForUnspecifiedPartitioner, and we need to modify it to false.
                if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                    edge.setIntraInputKeyCorrelated(false);
                }
            }
        }
    }

    private void generateHashesByStreamNodeId(Integer streamNodeId) {
        // Generate deterministic hashes for the nodes in order to identify them across
        // submission if they didn't change.
        if (hashes.containsKey(streamNodeId)) {
            return;
        }
        for (int i = 0; i < legacyStreamGraphHasher.size(); ++i) {
            legacyStreamGraphHasher
                    .get(i)
                    .generateHashesByStreamNodeId(streamNodeId, streamGraph, legacyHashes.get(i));
        }
        Preconditions.checkState(
                defaultStreamGraphHasher.generateHashesByStreamNodeId(
                        streamNodeId, streamGraph, hashes),
                "Failed to generate hash for streamNode with ID '%s'",
                streamNodeId);
    }

    /**
     * Determines whether the chain info is ready to create job vertex based on the following rules:
     *
     * <ul>
     *   <li>The hashes of all upstream nodes of the start node have been generated.
     *   <li>The vertices of all upstream non-source-chained nodes have been created and executed
     *       finished.
     * </ul>
     *
     * @param chainInfo the chain info.
     * @return true if ready, false otherwise.
     */
    private boolean isReadyToCreateJobVertex(OperatorChainInfo chainInfo) {
        Integer startNodeId = chainInfo.getStartNodeId();
        if (isNodeFrozen(startNodeId)) {
            return false;
        }
        StreamNode startNode = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge inEdges : startNode.getInEdges()) {
            Integer sourceNodeId = inEdges.getSourceId();
            if (!hashes.containsKey(sourceNodeId)) {
                return false;
            }
            if (chainInfo.getChainedSources().containsKey(sourceNodeId)) {
                continue;
            }
            Optional<JobVertexID> upstreamJobVertex = findVertexByStreamNodeId(sourceNodeId);
            if (upstreamJobVertex.isEmpty()
                    || !finishedJobVertices.contains(upstreamJobVertex.get())) {
                return false;
            }
        }
        return true;
    }

    private boolean isNodeFrozen(Integer streamNodeId) {
        return frozenNodeToStartNodeMap.containsKey(streamNodeId);
    }

    private Integer getStartNodeId(Integer streamNodeId) {
        return frozenNodeToStartNodeMap.get(streamNodeId);
    }

    private void generateStreamGraphJson() {
        streamGraphJson =
                JsonPlanGenerator.generateStreamGraphJson(streamGraph, streamNodeIdsToJobVertexMap);
    }

    public String getStreamGraphJson() {
        return streamGraphJson;
    }

    @Override
    public void onStreamGraphUpdated() {
        generateStreamGraphJson();
    }
}

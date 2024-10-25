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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.StreamNodeForwardGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.streaming.api.graph.util.ChainedOperatorHashInfo;
import org.apache.flink.streaming.api.graph.util.ChainedSourceInfo;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.api.graph.util.OperatorInfo;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.addVertexIndexPrefixInVertexName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.configureCheckpointing;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.connect;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedMinResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedName;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createChainedPreferredResources;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainable;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.isChainableInput;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.markSupportingConcurrentExecutionAttempts;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.preValidate;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.serializeOperatorCoordinatorsAndStreamConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setAllOperatorNonChainedOutputsConfigs;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setManagedMemoryFraction;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorChainedOutputsConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setOperatorConfig;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setPhysicalEdges;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setSlotSharingAndCoLocation;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setVertexDescription;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.tryConvertPartitionerForDynamicGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.validateHybridShuffleExecuteInBatchMode;

/** Default implementation for {@link AdaptiveGraphGenerator}. */
@Internal
public class AdaptiveGraphManager implements AdaptiveGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveGraphManager.class);

    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;

    private final StreamGraphHasher defaultStreamGraphHasher;

    private final List<StreamGraphHasher> legacyStreamGraphHasher;

    private final Executor serializationExecutor;

    private final AtomicInteger vertexIndexId;

    private final StreamGraphContext streamGraphContext;

    private final Map<Integer, Integer> frozenNodeToStartNodeMap;

    private final Map<Integer, byte[]> hashes;

    private final List<Map<Integer, byte[]>> legacyHashes;

    // When the downstream vertex is not created, we need to cache the output.
    private final Map<Integer, Map<StreamEdge, NonChainedOutput>> intermediateOutputsCaches;

    private final Map<IntermediateDataSetID, Integer> intermediateDataSetIdToProducerMap;

    private final Map<Integer, StreamNodeForwardGroup> forwardGroupsByEndpointNodeIdCache;

    // We need to cache all jobVertices to create JobEdge for downstream vertex.
    private final Map<Integer, JobVertex> jobVertices;

    private final Map<Integer, OperatorChainInfo> pendingSourceChainInfos;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, Integer> jobVertexToStartNodeMap;

    // The value is the stream node ids belonging to that job vertex.
    private final Map<JobVertexID, List<Integer>> jobVertexToChainedStreamNodeIdsMap;

    private final Set<JobVertexID> finishedJobVertices;

    private final AtomicBoolean hasHybridResultPartition;

    @VisibleForTesting
    public AdaptiveGraphManager(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            Executor serializationExecutor,
            @Nullable JobID jobId) {
        preValidate(streamGraph, userClassloader);
        this.streamGraph = streamGraph;
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);

        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHasher = Collections.singletonList(new StreamGraphUserHashHasher());

        this.hashes = new HashMap<>();
        this.legacyHashes = Collections.singletonList(new HashMap<>());

        this.jobVertices = new LinkedHashMap<>();
        this.pendingSourceChainInfos = new TreeMap<>();

        this.frozenNodeToStartNodeMap = new HashMap<>();
        this.intermediateOutputsCaches = new HashMap<>();
        this.intermediateDataSetIdToProducerMap = new HashMap<>();
        this.forwardGroupsByEndpointNodeIdCache = new HashMap<>();

        this.vertexIndexId = new AtomicInteger(0);

        this.hasHybridResultPartition = new AtomicBoolean(false);

        this.jobVertexToStartNodeMap = new HashMap<>();
        this.jobVertexToChainedStreamNodeIdsMap = new HashMap<>();

        this.finishedJobVertices = new HashSet<>();

        this.streamGraphContext =
                new DefaultStreamGraphContext(
                        streamGraph,
                        forwardGroupsByEndpointNodeIdCache,
                        frozenNodeToStartNodeMap,
                        intermediateOutputsCaches);

        this.jobGraph = new JobGraph(jobId, streamGraph.getJobName());

        initializeJobGraph();
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

    /**
     * Retrieves the StreamNodeForwardGroup which provides a stream node level ForwardGroup.
     *
     * @param jobVertexId The ID of the JobVertex.
     * @return An instance of {@link StreamNodeForwardGroup}.
     */
    public StreamNodeForwardGroup getStreamNodeForwardGroupByVertexId(JobVertexID jobVertexId) {
        Integer startNodeId = jobVertexToStartNodeMap.get(jobVertexId);
        return forwardGroupsByEndpointNodeIdCache.get(startNodeId);
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
     * @param intermediateDataSetID The ID of the IntermediateDataSet.
     * @return The ID of the stream node that produces the IntermediateDataSet.
     */
    public Integer getProducerStreamNodeId(IntermediateDataSetID intermediateDataSetID) {
        return intermediateDataSetIdToProducerMap.get(intermediateDataSetID);
    }

    private Optional<JobVertexID> findVertexByStreamNodeId(int streamNodeId) {
        if (frozenNodeToStartNodeMap.containsKey(streamNodeId)) {
            Integer startNodeId = frozenNodeToStartNodeMap.get(streamNodeId);
            return Optional.of(jobVertices.get(startNodeId).getID());
        }
        return Optional.empty();
    }

    private List<StreamEdge> getOutputEdgesByVertexId(JobVertexID jobVertexId) {
        JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);
        List<StreamEdge> outputEdges = new ArrayList<>();
        for (IntermediateDataSet result : jobVertex.getProducedDataSets()) {
            outputEdges.addAll(result.getOutputStreamEdges());
        }
        return outputEdges;
    }

    private void initializeJobGraph() {
        this.jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
        this.jobGraph.setJobType(streamGraph.getJobType());
        this.jobGraph.setDynamic(streamGraph.isDynamic());
        this.jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());

        // set the ExecutionConfig last when it has been finalized.
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }
        this.jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        configureCheckpointing(streamGraph, jobGraph);

        List<StreamNode> sourceNodes = new ArrayList<>();
        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            sourceNodes.add(streamGraph.getStreamNode(sourceNodeId));
        }
        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary(sourceNodes);
        }
        createJobVerticesAndUpdateGraph(sourceNodes);
    }

    private List<JobVertex> createJobVerticesAndUpdateGraph(List<StreamNode> streamNodes) {
        final JobVertexBuildContext jobVertexBuildContext =
                new JobVertexBuildContext(
                        streamGraph, hasHybridResultPartition, hashes, legacyHashes);

        createOperatorChainInfos(streamNodes, jobVertexBuildContext);

        createJobVertices(jobVertexBuildContext);

        generateConfigForJobVertices(jobVertexBuildContext);

        return new ArrayList<>(jobVertexBuildContext.getJobVerticesInOrder().values());
    }

    private void generateConfigForJobVertices(JobVertexBuildContext jobVertexBuildContext) {
        jobVertexBuildContext
                .getChainInfosInOrder()
                .values()
                .forEach(chainInfo -> initStreamConfigs(chainInfo, jobVertexBuildContext));
        generateAllOutputConfigs(jobVertexBuildContext);
        finalizeConfig(jobVertexBuildContext);
    }

    private void initStreamConfigs(
            OperatorChainInfo chainInfo, JobVertexBuildContext jobVertexBuildContext) {
        int startNodeId = chainInfo.getStartNodeId();
        List<StreamNode> chainedNodes = chainInfo.getAllChainedNodes();
        Map<Integer, ChainedSourceInfo> chainedSources = chainInfo.getChainedSources();

        int chainIndex = 1;
        for (StreamNode currentNode : chainedNodes) {
            int currentNodeId = currentNode.getId();
            boolean isChainedSource = chainedSources.containsKey(currentNodeId);
            OperatorInfo currentOperatorInfo = jobVertexBuildContext.getOperatorInfo(currentNodeId);
            StreamConfig config;
            if (isChainedSource) {
                config = chainInfo.getChainedSources().get(currentNodeId).getOperatorConfig();
            } else if (currentNodeId == startNodeId) {
                config =
                        new StreamConfig(
                                jobVertexBuildContext
                                        .getJobVertex(currentNodeId)
                                        .getConfiguration());
            } else {
                config = new StreamConfig(new Configuration());
            }

            Attribute currentNodeAttribute = currentNode.getAttribute();
            config.setAttribute(currentNodeAttribute);

            setOperatorConfig(
                    currentNodeId, config, chainInfo.getChainedSources(), jobVertexBuildContext);
            setOperatorChainedOutputsConfig(
                    config, currentOperatorInfo.getChainableOutputs(), jobVertexBuildContext);

            if (isChainedSource) {
                config.setChainIndex(0); // sources are always first
            } else {
                config.setChainIndex(chainIndex++);
                // We only need to set the name for the non-source chain configurations, as the
                // name of chained sources have already been set when created.
                config.setOperatorName(currentNode.getOperatorName());
            }

            if (currentNodeId == startNodeId) {
                config.setChainStart();
            } else {
                jobVertexBuildContext
                        .getOrCreateChainedConfig(startNodeId)
                        .put(currentNodeId, config);
            }

            config.setOperatorID(new OperatorID(hashes.get(currentNodeId)));

            if (currentOperatorInfo.getChainableOutputs().isEmpty()) {
                config.setChainEnd();
            }
        }
        StreamConfig vertexConfig =
                new StreamConfig(
                        jobVertexBuildContext.getJobVertex(startNodeId).getConfiguration());
        vertexConfig.setTransitiveChainedTaskConfigs(
                jobVertexBuildContext.getChainedConfigs().get(startNodeId));
    }

    private void generateAllOutputConfigs(JobVertexBuildContext jobVertexBuildContext) {
        // this may be used by uncreated down stream vertex.
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();

        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);

        connectNonChainedInput(jobVertexBuildContext);

        // The order of physicalEdges should be consistent with the order in which JobEdge was
        // created.
        setPhysicalEdges(jobVertexBuildContext);
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

    private void finalizeConfig(JobVertexBuildContext jobVertexBuildContext) {
        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);

        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);

        // When generating in a single step, there may be differences between the results and the
        // full image generation. We consider this difference to be normal because they do not need
        // to be in the same shared group.
        setSlotSharingAndCoLocation(jobGraph, jobVertexBuildContext);

        setManagedMemoryFraction(jobVertexBuildContext);

        addVertexIndexPrefixInVertexName(jobVertexBuildContext, vertexIndexId, jobGraph);

        setVertexDescription(jobVertexBuildContext);

        serializeOperatorCoordinatorsAndStreamConfig(
                jobGraph, serializationExecutor, jobVertexBuildContext);
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {

        StreamConfig config = jobVertexBuildContext.getOperatorInfo(startNodeId).getVertexConfig();
        List<StreamEdge> transitiveOutEdges =
                jobVertexBuildContext.getChainInfo(startNodeId).getTransitiveOutEdges();

        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            // When a downstream vertex has been created, a connection to the downstream will be
            // created, otherwise only an IntermediateDataSet will be created for it.
            if (jobVertexBuildContext.getJobVerticesInOrder().containsKey(edge.getTargetId())) {
                connect(startNodeId, edge, output, jobVertices, jobVertexBuildContext);
            } else {
                JobVertex jobVertex =
                        jobVertexBuildContext.getJobVerticesInOrder().get(startNodeId);
                IntermediateDataSet dataSet =
                        jobVertex.getOrCreateResultDataSet(
                                output.getDataSetId(), output.getPartitionType());
                dataSet.addOutputStreamEdge(edge);
                // we cache the output here for downstream vertex to create jobEdge.
                intermediateOutputsCaches
                        .computeIfAbsent(edge.getSourceId(), k -> new HashMap<>())
                        .put(edge, output);
            }
            intermediateDataSetIdToProducerMap.put(output.getDataSetId(), edge.getSourceId());
        }
        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    // Create JobEdge with the completed upstream jobVertex.
    private void connectNonChainedInput(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            List<StreamEdge> streamEdges = chainInfo.getTransitiveInEdges();
            for (StreamEdge edge : streamEdges) {
                NonChainedOutput output =
                        intermediateOutputsCaches.get(edge.getSourceId()).get(edge);
                Integer sourceStartNodeId = frozenNodeToStartNodeMap.get(edge.getSourceId());
                connect(sourceStartNodeId, edge, output, jobVertices, jobVertexBuildContext);
            }
        }
    }

    private void createJobVertices(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, OperatorChainInfo> chainInfos = jobVertexBuildContext.getChainInfosInOrder();
        for (OperatorChainInfo chainInfo : chainInfos.values()) {
            JobVertex jobVertex = createJobVertex(chainInfo, jobVertexBuildContext);
            jobVertexBuildContext.addJobVertex(chainInfo.getStartNodeId(), jobVertex);
            jobVertices.put(chainInfo.getStartNodeId(), jobVertex);
            jobGraph.addVertex(jobVertex);
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
                            });
        }
    }

    private JobVertex createJobVertex(
            OperatorChainInfo chainInfo, JobVertexBuildContext jobVertexBuildContext) {
        JobVertex jobVertex;
        Integer streamNodeId = chainInfo.getStartNodeId();
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = hashes.get(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<ChainedOperatorHashInfo> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIdPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (ChainedOperatorHashInfo chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorId =
                        chainedOperator.getUserDefinedOperatorId() == null
                                ? null
                                : new OperatorID(chainedOperator.getUserDefinedOperatorId());
                operatorIdPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.getGeneratedOperatorId()),
                                userDefinedOperatorId,
                                chainedOperator.getStreamNode().getOperatorName(),
                                chainedOperator.getStreamNode().getTransformationUID()));
            }
        }

        if (chainInfo.hasFormatContainer()) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIdPairs);
            chainInfo
                    .getOrCreateFormatContainer()
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex =
                    new JobVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIdPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider :
                chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    chainInfo.getChainedName(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            jobVertexBuildContext.putCoordinatorSerializationFutures(
                    jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                chainInfo.getChainedMinResources(streamNodeId),
                chainInfo.getChainedPreferredResources(streamNodeId));

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return jobVertex;
    }

    private void createOperatorChainInfos(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildAndGetChainEntryPoints(streamNodes, jobVertexBuildContext);
        final List<OperatorChainInfo> chainInfos = new ArrayList<>(chainEntryPoints.values());
        for (OperatorChainInfo info : chainInfos) {
            generateOperatorChainInfo(
                    info.getStartNodeId(), info, chainEntryPoints, jobVertexBuildContext);
        }
    }

    private Map<Integer, OperatorChainInfo> buildAndGetChainEntryPoints(
            List<StreamNode> streamNodes, JobVertexBuildContext jobVertexBuildContext) {
        for (StreamNode streamNode : streamNodes) {
            buildChainEntryPoint(streamNode);
        }
        return getChainEntryPoints(jobVertexBuildContext);
    }

    private void buildChainEntryPoint(StreamNode streamNode) {
        int streamNodeId = streamNode.getId();
        if (!isSourceChainable(streamNode)) {
            pendingSourceChainInfos.computeIfAbsent(
                    streamNodeId,
                    ignored -> new OperatorChainInfo(streamNodeId, new HashMap<>(), streamGraph));
        } else {
            generateHashesByStreamNodeId(streamNodeId);

            final StreamEdge sourceOutEdge = streamNode.getOutEdges().get(0);
            final int startNodeId = sourceOutEdge.getTargetId();
            final SourceOperatorFactory<?> sourceOpFact =
                    (SourceOperatorFactory<?>) streamNode.getOperatorFactory();
            Preconditions.checkNotNull(sourceOpFact);

            final OperatorCoordinator.Provider coordinatorProvider =
                    sourceOpFact.getCoordinatorProvider(
                            streamNode.getOperatorName(),
                            new OperatorID(hashes.get(streamNode.getId())));
            final StreamConfig operatorConfig = new StreamConfig(new Configuration());
            final StreamConfig.SourceInputConfig inputConfig =
                    new StreamConfig.SourceInputConfig(sourceOutEdge);
            operatorConfig.setOperatorName(streamNode.getOperatorName());

            OperatorChainInfo chainInfo =
                    pendingSourceChainInfos.computeIfAbsent(
                            startNodeId,
                            ignored ->
                                    new OperatorChainInfo(
                                            startNodeId, new HashMap<>(), streamGraph));

            chainInfo.addChainedSource(
                    streamNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));
            chainInfo.recordChainedNode(streamNodeId);
            chainInfo.addCoordinatorProvider(coordinatorProvider);
        }
    }

    private Map<Integer, OperatorChainInfo> getChainEntryPoints(
            JobVertexBuildContext jobVertexBuildContext) {

        final Map<Integer, OperatorChainInfo> chainEntryPoints = new TreeMap<>();
        Iterator<Map.Entry<Integer, OperatorChainInfo>> iterator =
                pendingSourceChainInfos.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Integer, OperatorChainInfo> entry = iterator.next();
            Integer startNodeId = entry.getKey();
            OperatorChainInfo chainInfo = entry.getValue();
            if (!isReadyToCreateJobVertex(chainInfo)) {
                continue;
            }
            chainEntryPoints.put(startNodeId, chainInfo);
            for (Integer sourceNodeId : chainInfo.getChainedSources().keySet()) {
                StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);
                StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                // we cache the chainable outputs here, and set the chained config later.
                jobVertexBuildContext
                        .createAndGetOperatorInfo(sourceNodeId)
                        .addChainableOutputs(Collections.singletonList(sourceOutEdge));
            }
            iterator.remove();
        }
        return chainEntryPoints;
    }

    private List<StreamEdge> generateOperatorChainInfo(
            final Integer currentNodeId,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints,
            final JobVertexBuildContext jobVertexBuildContext) {

        Integer startNodeId = chainInfo.getStartNodeId();

        if (jobVertexBuildContext.getChainInfosInOrder().containsKey(startNodeId)) {
            return new ArrayList<>();
        }

        List<StreamEdge> transitiveOutEdges = new ArrayList<>();
        List<StreamEdge> chainableOutputs = new ArrayList<>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<>();

        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

        generateHashesByStreamNodeId(currentNodeId);

        Attribute currentNodeAttribute = currentNode.getAttribute();
        boolean isNoOutputUntilEndOfInput =
                currentNode.isOutputOnlyAfterEndOfStream()
                        || currentNodeAttribute.isNoOutputUntilEndOfInput();
        if (isNoOutputUntilEndOfInput) {
            currentNodeAttribute.setNoOutputUntilEndOfInput(true);
        }

        // We need to record the chained nodes here to ensure the order is from source to end.
        OperatorID currentOperatorId =
                chainInfo.addNodeToChain(
                        currentNodeId,
                        streamGraph.getStreamNode(currentNodeId).getOperatorName(),
                        jobVertexBuildContext);

        for (StreamEdge outEdge : currentNode.getOutEdges()) {
            if (isChainable(outEdge, streamGraph)) {
                chainableOutputs.add(outEdge);
            } else {
                nonChainableOutputs.add(outEdge);
            }
        }

        for (StreamEdge inEdge : currentNode.getInEdges()) {
            // inEdge exist in generated jobVertex.
            if (frozenNodeToStartNodeMap.containsKey(inEdge.getSourceId())) {
                chainInfo.addTransitiveInEdge(inEdge);
            }
        }

        for (StreamEdge chainable : chainableOutputs) {
            // Mark downstream nodes in the same chain as outputBlocking.
            StreamNode targetNode = streamGraph.getStreamNode(chainable.getTargetId());
            Attribute targetNodeAttribute = targetNode.getAttribute();
            if (isNoOutputUntilEndOfInput) {
                if (targetNodeAttribute != null) {
                    targetNodeAttribute.setNoOutputUntilEndOfInput(true);
                }
            }
            transitiveOutEdges.addAll(
                    generateOperatorChainInfo(
                            chainable.getTargetId(),
                            chainInfo,
                            chainEntryPoints,
                            jobVertexBuildContext));
            // Mark upstream nodes in the same chain as outputBlocking.
            if (targetNodeAttribute != null && targetNodeAttribute.isNoOutputUntilEndOfInput()) {
                currentNodeAttribute.setNoOutputUntilEndOfInput(true);
            }
        }

        transitiveOutEdges.addAll(nonChainableOutputs);

        chainInfo.addChainedName(
                currentNodeId,
                createChainedName(
                        currentNodeId,
                        chainableOutputs,
                        Optional.ofNullable(chainEntryPoints.get(currentNodeId)),
                        chainInfo.getChainedNames(),
                        jobVertexBuildContext));

        chainInfo.addChainedMinResources(
                currentNodeId,
                createChainedMinResources(
                        currentNodeId, chainableOutputs, chainInfo, jobVertexBuildContext));

        chainInfo.addChainedPreferredResources(
                currentNodeId,
                createChainedPreferredResources(
                        currentNodeId, chainableOutputs, chainInfo, jobVertexBuildContext));

        if (currentNode.getInputFormat() != null) {
            chainInfo
                    .getOrCreateFormatContainer()
                    .addInputFormat(currentOperatorId, currentNode.getInputFormat());
        }

        if (currentNode.getOutputFormat() != null) {
            chainInfo
                    .getOrCreateFormatContainer()
                    .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
        }

        tryConvertPartitionerForDynamicGraph(
                chainableOutputs, nonChainableOutputs, jobVertexBuildContext);

        OperatorInfo operatorInfo = jobVertexBuildContext.createAndGetOperatorInfo(currentNodeId);

        // we cache the non-chainable outputs here, and set the non-chained config later.
        operatorInfo.addNonChainableOutputs(nonChainableOutputs);

        // we cache the chainable outputs here, and set the chained config later.
        operatorInfo.addChainableOutputs(chainableOutputs);

        if (currentNodeId.equals(startNodeId)) {
            chainInfo.setTransitiveOutEdges(transitiveOutEdges);
            jobVertexBuildContext.addChainInfo(startNodeId, chainInfo);
        }

        return transitiveOutEdges;
    }

    private void setVertexParallelismsForDynamicGraphIfNecessary(List<StreamNode> streamNodes) {
        List<StreamEdge> chainableOutputs = new ArrayList<>();

        Map<Integer, List<StreamEdge>> transitiveOutEdgesMap = new HashMap<>();
        Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap =
                new LinkedHashMap<>();
        Set<Integer> finishedChain = new HashSet<>();

        List<StreamNode> enterPoints =
                getEnterPoints(streamNodes, topologicallySortedChainedStreamNodesMap);
        for (StreamNode streamNode : enterPoints) {
            traverseFullGraph(
                    streamNode.getId(),
                    streamNode.getId(),
                    chainableOutputs,
                    transitiveOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    finishedChain);
        }
        computeForwardGroupAndSetNodeParallelisms(
                transitiveOutEdgesMap, topologicallySortedChainedStreamNodesMap, chainableOutputs);
    }

    private List<StreamNode> getEnterPoints(
            List<StreamNode> sourceNodes, Map<StreamNode, List<StreamNode>> chainedStreamNodesMap) {
        List<StreamNode> enterPoints = new ArrayList<>();
        for (StreamNode sourceNode : sourceNodes) {
            if (isSourceChainable(sourceNode)) {
                StreamEdge outEdge = sourceNode.getOutEdges().get(0);
                StreamNode startNode = streamGraph.getStreamNode(outEdge.getTargetId());
                chainedStreamNodesMap
                        .computeIfAbsent(startNode, k -> new ArrayList<>())
                        .add(sourceNode);
                enterPoints.add(startNode);
            } else {
                chainedStreamNodesMap.computeIfAbsent(sourceNode, k -> new ArrayList<>());
                enterPoints.add(sourceNode);
            }
        }
        return enterPoints;
    }

    private void traverseFullGraph(
            Integer startNodeId,
            Integer currentNodeId,
            List<StreamEdge> allChainableOutputs,
            Map<Integer, List<StreamEdge>> transitiveOutEdgesMap,
            Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap,
            Set<Integer> finishedChain) {
        if (finishedChain.contains(startNodeId)) {
            return;
        }
        List<StreamEdge> chainableOutputs = new ArrayList<>();
        List<StreamEdge> nonChainableOutputs = new ArrayList<>();
        StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
        StreamNode startNode = streamGraph.getStreamNode(startNodeId);
        for (StreamEdge streamEdge : currentNode.getOutEdges()) {
            if (isChainable(streamEdge, streamGraph)) {
                chainableOutputs.add(streamEdge);
            } else {
                nonChainableOutputs.add(streamEdge);
                topologicallySortedChainedStreamNodesMap.computeIfAbsent(
                        streamGraph.getStreamNode(streamEdge.getTargetId()),
                        k -> new ArrayList<>());
            }
        }

        allChainableOutputs.addAll(chainableOutputs);

        transitiveOutEdgesMap
                .computeIfAbsent(startNodeId, k -> new ArrayList<>())
                .addAll(nonChainableOutputs);

        topologicallySortedChainedStreamNodesMap.get(startNode).add(currentNode);

        for (StreamEdge chainable : chainableOutputs) {
            traverseFullGraph(
                    startNodeId,
                    chainable.getTargetId(),
                    allChainableOutputs,
                    transitiveOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    finishedChain);
        }
        for (StreamEdge nonChainable : nonChainableOutputs) {
            traverseFullGraph(
                    nonChainable.getTargetId(),
                    nonChainable.getTargetId(),
                    allChainableOutputs,
                    transitiveOutEdgesMap,
                    topologicallySortedChainedStreamNodesMap,
                    finishedChain);
        }
        if (currentNodeId.equals(startNodeId)) {
            finishedChain.add(startNodeId);
        }
    }

    private void computeForwardGroupAndSetNodeParallelisms(
            Map<Integer, List<StreamEdge>> transitiveOutEdgesMap,
            Map<StreamNode, List<StreamNode>> topologicallySortedChainedStreamNodesMap,
            List<StreamEdge> chainableOutputs) {
        // reset parallelism for chained stream nodes whose parallelism is not configured.
        for (List<StreamNode> chainedStreamNodes :
                topologicallySortedChainedStreamNodesMap.values()) {
            boolean isParallelismConfigured =
                    chainedStreamNodes.stream().anyMatch(StreamNode::isParallelismConfigured);
            if (!isParallelismConfigured && streamGraph.isAutoParallelismEnabled()) {
                chainedStreamNodes.forEach(
                        n -> n.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT, false));
            }
        }

        final Map<StreamNode, Set<StreamNode>> forwardProducersByStartNode = new LinkedHashMap<>();

        for (StreamNode startNode : topologicallySortedChainedStreamNodesMap.keySet()) {
            int startNodeId = startNode.getId();
            Set<StreamNode> forwardConsumers =
                    transitiveOutEdgesMap.get(startNodeId).stream()
                            .filter(
                                    edge -> {
                                        StreamPartitioner<?> partitioner = edge.getPartitioner();
                                        if (partitioner
                                                        instanceof
                                                        ForwardForConsecutiveHashPartitioner
                                                || partitioner
                                                        instanceof
                                                        ForwardForUnspecifiedPartitioner) {
                                            return chainableOutputs.contains(edge);
                                        }
                                        return partitioner instanceof ForwardPartitioner;
                                    })
                            .map(StreamEdge::getTargetId)
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toSet());
            for (StreamNode forwardConsumer : forwardConsumers) {
                forwardProducersByStartNode
                        .computeIfAbsent(forwardConsumer, ignored -> new HashSet<>())
                        .add(streamGraph.getStreamNode(startNodeId));
            }
        }

        final Map<Integer, StreamNodeForwardGroup> forwardGroupsByStartNodeId =
                ForwardGroupComputeUtil.computeStreamNodeForwardGroup(
                        topologicallySortedChainedStreamNodesMap,
                        startNode ->
                                forwardProducersByStartNode.getOrDefault(
                                        startNode, Collections.emptySet()));

        forwardGroupsByStartNodeId.forEach(
                (startNodeId, forwardGroup) -> {
                    this.forwardGroupsByEndpointNodeIdCache.put(startNodeId, forwardGroup);
                    transitiveOutEdgesMap
                            .get(startNodeId)
                            .forEach(
                                    streamEdge -> {
                                        this.forwardGroupsByEndpointNodeIdCache.put(
                                                streamEdge.getSourceId(), forwardGroup);
                                    });
                });

        topologicallySortedChainedStreamNodesMap.forEach(this::setNodeParallelism);
    }

    private void setNodeParallelism(StreamNode startNode, List<StreamNode> chainedStreamNodes) {
        StreamNodeForwardGroup streamNodeForwardGroup =
                forwardGroupsByEndpointNodeIdCache.get(startNode.getId());
        // set parallelism for vertices in forward group.
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setParallelism(
                                    streamNodeForwardGroup.getParallelism(), true));
        }
        if (streamNodeForwardGroup != null && streamNodeForwardGroup.isMaxParallelismDecided()) {
            chainedStreamNodes.forEach(
                    streamNode ->
                            streamNode.setMaxParallelism(
                                    streamNodeForwardGroup.getMaxParallelism()));
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

    private boolean isReadyToCreateJobVertex(OperatorChainInfo chainInfo) {
        Integer startNodeId = chainInfo.getStartNodeId();
        if (frozenNodeToStartNodeMap.containsKey(startNodeId)) {
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

    private boolean isSourceChainable(StreamNode sourceNode) {
        if (!sourceNode.getInEdges().isEmpty()
                || sourceNode.getOperatorFactory() == null
                || !(sourceNode.getOperatorFactory() instanceof SourceOperatorFactory)
                || sourceNode.getOutEdges().size() != 1) {
            return false;
        }
        final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
        final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
        final ChainingStrategy targetChainingStrategy =
                Preconditions.checkNotNull(target.getOperatorFactory()).getChainingStrategy();
        return targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                && isChainableInput(sourceOutEdge, streamGraph);
    }
}

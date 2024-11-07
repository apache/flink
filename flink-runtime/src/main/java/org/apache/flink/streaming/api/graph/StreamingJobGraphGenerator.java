/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.util.ChainedOperatorHashInfo;
import org.apache.flink.streaming.api.graph.util.ChainedSourceInfo;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.api.graph.util.OperatorInfo;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return new StreamingJobGraphGenerator(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        streamGraph.getJobID(),
                        Runnable::run)
                .createJobGraph();
    }

    public static JobGraph createJobGraph(
            ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
        // TODO Currently, we construct a new thread pool for the compilation of each job. In the
        // future, we may refactor the job submission framework and make it reusable across jobs.
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        try {
            return new StreamingJobGraphGenerator(
                            userClassLoader, streamGraph, jobID, serializationExecutor)
                    .createJobGraph();
        } finally {
            serializationExecutor.shutdown();
        }
    }

    // ------------------------------------------------------------------------

    private final ClassLoader userClassloader;
    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;
    private final Collection<Integer> builtVertices;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private final Executor serializationExecutor;

    // We save all the context needed to create the JobVertex in this structure.
    private final JobVertexBuildContext jobVertexBuildContext;

    private StreamingJobGraphGenerator(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            @Nullable JobID jobID,
            Executor serializationExecutor) {
        this.userClassloader = userClassloader;
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

        this.builtVertices = new HashSet<>();
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        jobGraph = new JobGraph(jobID, streamGraph.getJobName());

        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        final Map<Integer, byte[]> hashes =
                defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

        // Generate legacy version hashes for backwards compatibility
        final List<Map<Integer, byte[]>> legacyHashes =
                new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }

        this.jobVertexBuildContext =
                new JobVertexBuildContext(
                        streamGraph, new AtomicBoolean(false), hashes, legacyHashes);
    }

    private JobGraph createJobGraph() {
        preValidate(streamGraph, userClassloader);
        jobGraph.setJobType(streamGraph.getJobType());
        jobGraph.setDynamic(streamGraph.isDynamic());

        jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        setChaining();

        if (jobGraph.isDynamic()) {
            setVertexParallelismsForDynamicGraphIfNecessary();
        }

        // Note that we set all the non-chainable outputs configuration here because the
        // "setVertexParallelismsForDynamicGraphIfNecessary" may affect the parallelism of job
        // vertices and partition-reuse
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs =
                new HashMap<>();
        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);
        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);

        setPhysicalEdges(jobVertexBuildContext);

        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);

        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);

        setSlotSharingAndCoLocation(jobGraph, jobVertexBuildContext);

        setManagedMemoryFraction(jobVertexBuildContext);

        configureCheckpointing(streamGraph, jobGraph);

        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());

        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                streamGraph.getUserArtifacts().entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        streamGraph.getUserJarBlobKeys().forEach(jobGraph::addUserJarBlobKey);
        jobGraph.setClasspaths(streamGraph.getClasspath());

        // set the ExecutionConfig last when it has been finalized
        try {
            jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
        } catch (IOException e) {
            throw new IllegalConfigurationException(
                    "Could not serialize the ExecutionConfig."
                            + "This indicates that non-serializable types (like custom serializers) were registered");
        }
        jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());

        addVertexIndexPrefixInVertexName(jobVertexBuildContext, new AtomicInteger(0), jobGraph);

        setVertexDescription(jobVertexBuildContext);

        // Wait for the serialization of operator coordinators and stream config.
        serializeOperatorCoordinatorsAndStreamConfig(
                jobGraph, serializationExecutor, jobVertexBuildContext);

        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        return jobGraph;
    }

    public static void serializeOperatorCoordinatorsAndStreamConfig(
            JobGraph jobGraph,
            Executor serializationExecutor,
            JobVertexBuildContext jobVertexBuildContext) {
        try {
            FutureUtils.combineAll(
                            jobVertexBuildContext.getOperatorInfos().values().stream()
                                    .map(
                                            operatorInfo ->
                                                    operatorInfo
                                                            .getVertexConfig()
                                                            .triggerSerializationAndReturnFuture(
                                                                    serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();

            waitForSerializationFuturesAndUpdateJobVertices(jobGraph, jobVertexBuildContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }
    }

    /**
     * Creates an instance of {@link OperatorID} based on the provided operator unique identifier
     * (UID).
     *
     * @param operatorUid the unique identifier of the operator, used to generate the hash
     * @return a new {@link OperatorID} instance generated from the specified operator UID
     */
    public static OperatorID generateOperatorID(String operatorUid) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(operatorUid));
    }

    private static void waitForSerializationFuturesAndUpdateJobVertices(
            JobGraph jobGraph, JobVertexBuildContext jobVertexBuildContext)
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex :
                        jobVertexBuildContext
                                .getCoordinatorSerializationFuturesPerJobVertex()
                                .entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex = jobGraph.findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    public static void addVertexIndexPrefixInVertexName(
            JobVertexBuildContext jobVertexBuildContext,
            AtomicInteger vertexIndexId,
            JobGraph jobGraph) {
        if (!jobVertexBuildContext.getStreamGraph().isVertexNameIncludeIndexPrefix()) {
            return;
        }
        Set<JobVertexID> jobVertexIds =
                jobVertexBuildContext.getJobVerticesInOrder().values().stream()
                        .map(JobVertex::getID)
                        .collect(Collectors.toSet());
        // JobVertexBuildContext only contains incrementally generated jobVertex instances. The
        // setName method needs to set the names based on the topological order of all jobVertices,
        // so it relies on the job graph to compute a difference set.
        jobGraph.getVerticesSortedTopologicallyFromSources()
                .forEach(
                        vertex -> {
                            if (jobVertexIds.contains(vertex.getID())) {
                                vertex.setName(
                                        String.format(
                                                "[vertex-%d]%s",
                                                vertexIndexId.getAndIncrement(), vertex.getName()));
                            }
                        });
    }

    public static void setVertexDescription(JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        for (Map.Entry<Integer, JobVertex> headOpAndJobVertex : jobVertices.entrySet()) {
            Integer headOpId = headOpAndJobVertex.getKey();
            JobVertex vertex = headOpAndJobVertex.getValue();
            StringBuilder builder = new StringBuilder();
            switch (streamGraph.getVertexDescriptionMode()) {
                case CASCADING:
                    buildCascadingDescription(builder, headOpId, headOpId, jobVertexBuildContext);
                    break;
                case TREE:
                    buildTreeDescription(
                            builder, headOpId, headOpId, "", true, jobVertexBuildContext);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Description mode %s not supported",
                                    streamGraph.getVertexDescriptionMode()));
            }
            vertex.setOperatorPrettyName(builder.toString());
        }
    }

    private static void buildCascadingDescription(
            StringBuilder builder,
            int headOpId,
            int currentOpId,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamNode node = jobVertexBuildContext.getStreamGraph().getStreamNode(currentOpId);
        builder.append(getDescriptionWithChainedSourcesInfo(node, jobVertexBuildContext));

        LinkedList<Integer> chainedOutput =
                getChainedOutputNodes(headOpId, node, jobVertexBuildContext);
        if (chainedOutput.isEmpty()) {
            return;
        }
        builder.append(" -> ");

        boolean multiOutput = chainedOutput.size() > 1;
        if (multiOutput) {
            builder.append("(");
        }
        while (true) {
            Integer outputId = chainedOutput.pollFirst();
            buildCascadingDescription(builder, headOpId, outputId, jobVertexBuildContext);
            if (chainedOutput.isEmpty()) {
                break;
            }
            builder.append(" , ");
        }
        if (multiOutput) {
            builder.append(")");
        }
    }

    private static LinkedList<Integer> getChainedOutputNodes(
            int headOpId, StreamNode node, JobVertexBuildContext jobVertexBuildContext) {
        LinkedList<Integer> chainedOutput = new LinkedList<>();
        Map<Integer, Map<Integer, StreamConfig>> chainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        if (chainedConfigs.containsKey(headOpId)) {
            for (StreamEdge edge : node.getOutEdges()) {
                int targetId = edge.getTargetId();
                if (chainedConfigs.get(headOpId).containsKey(targetId)) {
                    chainedOutput.add(targetId);
                }
            }
        }
        return chainedOutput;
    }

    private static void buildTreeDescription(
            StringBuilder builder,
            int headOpId,
            int currentOpId,
            String prefix,
            boolean isLast,
            JobVertexBuildContext jobVertexBuildContext) {
        // Replace the '-' in prefix of current node with ' ', keep ':'
        // HeadNode
        // :- Node1
        // :  :- Child1
        // :  +- Child2
        // +- Node2
        //    :- Child3
        //    +- Child4
        String currentNodePrefix = "";
        String childPrefix = "";
        if (currentOpId != headOpId) {
            if (isLast) {
                currentNodePrefix = prefix + "+- ";
                childPrefix = prefix + "   ";
            } else {
                currentNodePrefix = prefix + ":- ";
                childPrefix = prefix + ":  ";
            }
        }

        StreamNode node = jobVertexBuildContext.getStreamGraph().getStreamNode(currentOpId);
        builder.append(currentNodePrefix);
        builder.append(getDescriptionWithChainedSourcesInfo(node, jobVertexBuildContext));
        builder.append("\n");

        LinkedList<Integer> chainedOutput =
                getChainedOutputNodes(headOpId, node, jobVertexBuildContext);
        while (!chainedOutput.isEmpty()) {
            Integer outputId = chainedOutput.pollFirst();
            buildTreeDescription(
                    builder,
                    headOpId,
                    outputId,
                    childPrefix,
                    chainedOutput.isEmpty(),
                    jobVertexBuildContext);
        }
    }

    private static String getDescriptionWithChainedSourcesInfo(
            StreamNode node, JobVertexBuildContext jobVertexBuildContext) {

        List<StreamNode> chainedSources;
        final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        if (!chainedConfigs.containsKey(node.getId())) {
            // node is not head operator of a vertex
            chainedSources = Collections.emptyList();
        } else {
            chainedSources =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .filter(
                                    id ->
                                            streamGraph.getSourceIDs().contains(id)
                                                    && chainedConfigs
                                                            .get(node.getId())
                                                            .containsKey(id))
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
        }
        return chainedSources.isEmpty()
                ? node.getOperatorDescription()
                : String.format(
                        "%s [%s]",
                        node.getOperatorDescription(),
                        chainedSources.stream()
                                .map(StreamNode::getOperatorDescription)
                                .collect(Collectors.joining(", ")));
    }

    @SuppressWarnings("deprecation")
    public static void preValidate(StreamGraph streamGraph, ClassLoader userClassloader) {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative()
                    && checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, "
                                + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()
                    && streamGraph.getStreamNodes().stream()
                            .anyMatch(StreamingJobGraphGenerator::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, "
                                + "as rescaling is not guaranteed to work correctly."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass =
                            operatorFactory.getStreamOperatorClass(userClassloader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:"
                                        + operatorClass.getName());
                    }
                }
            }
        }

        if (checkpointConfig.isUnalignedCheckpointsEnabled()
                && streamGraph.getCheckpointingMode() != CheckpointingMode.EXACTLY_ONCE) {
            LOG.warn("Unaligned checkpoints can only be used with checkpointing mode EXACTLY_ONCE");
            checkpointConfig.enableUnalignedCheckpoints(false);
        }
    }

    private static boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream()
                .anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }

    public static void setPhysicalEdges(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder =
                new HashMap<Integer, List<StreamEdge>>();

        for (StreamEdge edge : jobVertexBuildContext.getPhysicalEdgesInOrder()) {
            int target = edge.getTargetId();

            List<StreamEdge> inEdges =
                    physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

            inEdges.add(edge);
        }

        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();

            jobVertexBuildContext
                    .getOperatorInfo(vertex)
                    .getVertexConfig()
                    .setInPhysicalEdges(edgeList);
        }
    }

    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs() {

        final Map<Integer, ChainedSourceInfo> chainedSources = new HashMap<>();
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);

            if (sourceNode.getOperatorFactory() != null
                    && sourceNode.getOperatorFactory() instanceof SourceOperatorFactory
                    && sourceNode.getOutEdges().size() == 1) {
                // as long as only NAry ops support this chaining, we need to skip the other parts
                final StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);
                final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
                final ChainingStrategy targetChainingStrategy =
                        Preconditions.checkNotNull(target.getOperatorFactory())
                                .getChainingStrategy();

                if (targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                        && isChainableInput(sourceOutEdge, streamGraph)) {
                    final OperatorInfo operatorInfo =
                            jobVertexBuildContext.createAndGetOperatorInfo(sourceNodeId);
                    final OperatorID opId =
                            new OperatorID(jobVertexBuildContext.getHash(sourceNodeId));
                    final StreamConfig.SourceInputConfig inputConfig =
                            new StreamConfig.SourceInputConfig(sourceOutEdge);
                    final StreamConfig operatorConfig = new StreamConfig(new Configuration());
                    setOperatorConfig(
                            sourceNodeId,
                            operatorConfig,
                            Collections.emptyMap(),
                            jobVertexBuildContext);
                    setOperatorChainedOutputsConfig(
                            operatorConfig, Collections.emptyList(), jobVertexBuildContext);
                    // we cache the non-chainable outputs here, and set the non-chained config later
                    operatorInfo.addNonChainableOutputs(Collections.emptyList());

                    operatorConfig.setChainIndex(0); // sources are always first
                    operatorConfig.setOperatorID(opId);
                    operatorConfig.setOperatorName(sourceNode.getOperatorName());
                    chainedSources.put(
                            sourceNodeId, new ChainedSourceInfo(operatorConfig, inputConfig));

                    final SourceOperatorFactory<?> sourceOpFact =
                            (SourceOperatorFactory<?>) sourceNode.getOperatorFactory();
                    final OperatorCoordinator.Provider coord =
                            sourceOpFact.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

                    final OperatorChainInfo chainInfo =
                            chainEntryPoints.computeIfAbsent(
                                    sourceOutEdge.getTargetId(),
                                    (k) ->
                                            new OperatorChainInfo(
                                                    sourceOutEdge.getTargetId(),
                                                    chainedSources,
                                                    streamGraph));
                    chainInfo.addCoordinatorProvider(coord);
                    chainInfo.recordChainedNode(sourceNodeId);
                    continue;
                }
            }

            chainEntryPoints.put(
                    sourceNodeId, new OperatorChainInfo(sourceNodeId, chainedSources, streamGraph));
        }

        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    private void setChaining() {
        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        final Map<Integer, OperatorChainInfo> chainEntryPoints =
                buildChainedInputsAndGetHeadInputs();
        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        // iterate over a copy of the values, because this map gets concurrently modified
        for (OperatorChainInfo info : initialEntryPoints) {
            createChain(
                    info.getStartNodeId(),
                    1, // operators start at position 1 because 0 is for chained source inputs
                    info,
                    chainEntryPoints);
        }
    }

    private List<StreamEdge> createChain(
            final Integer currentNodeId,
            final int chainIndex,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints) {

        Integer startNodeId = chainInfo.getStartNodeId();
        if (!builtVertices.contains(startNodeId)) {

            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
            Attribute currentNodeAttribute = currentNode.getAttribute();
            boolean isNoOutputUntilEndOfInput =
                    currentNode.isOutputOnlyAfterEndOfStream()
                            || currentNodeAttribute.isNoOutputUntilEndOfInput();
            if (isNoOutputUntilEndOfInput) {
                currentNodeAttribute.setNoOutputUntilEndOfInput(true);
            }

            for (StreamEdge outEdge : currentNode.getOutEdges()) {
                if (isChainable(outEdge, streamGraph)) {
                    chainableOutputs.add(outEdge);
                } else {
                    nonChainableOutputs.add(outEdge);
                }
            }

            for (StreamEdge chainable : chainableOutputs) {
                StreamNode targetNode = streamGraph.getStreamNode(chainable.getTargetId());
                Attribute targetNodeAttribute = targetNode.getAttribute();
                if (isNoOutputUntilEndOfInput) {
                    if (targetNodeAttribute != null) {
                        targetNodeAttribute.setNoOutputUntilEndOfInput(true);
                    }
                }
                transitiveOutEdges.addAll(
                        createChain(
                                chainable.getTargetId(),
                                chainIndex + 1,
                                chainInfo,
                                chainEntryPoints));
                // Mark upstream nodes in the same chain as outputBlocking
                if (targetNodeAttribute != null
                        && targetNodeAttribute.isNoOutputUntilEndOfInput()) {
                    currentNodeAttribute.setNoOutputUntilEndOfInput(true);
                }
            }

            for (StreamEdge nonChainable : nonChainableOutputs) {
                transitiveOutEdges.add(nonChainable);
                createChain(
                        nonChainable.getTargetId(),
                        1, // operators start at position 1 because 0 is for chained source inputs
                        chainEntryPoints.computeIfAbsent(
                                nonChainable.getTargetId(),
                                (k) -> chainInfo.newChain(nonChainable.getTargetId())),
                        chainEntryPoints);
            }

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

            OperatorID currentOperatorId =
                    chainInfo.addNodeToChain(
                            currentNodeId,
                            streamGraph.getStreamNode(currentNodeId).getOperatorName(),
                            jobVertexBuildContext);

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
            OperatorInfo operatorInfo =
                    jobVertexBuildContext.createAndGetOperatorInfo(currentNodeId);

            StreamConfig config =
                    currentNodeId.equals(startNodeId)
                            ? createJobVertex(startNodeId, chainInfo)
                            : new StreamConfig(new Configuration());

            tryConvertPartitionerForDynamicGraph(
                    chainableOutputs, nonChainableOutputs, jobVertexBuildContext);
            config.setAttribute(currentNodeAttribute);
            setOperatorConfig(
                    currentNodeId, config, chainInfo.getChainedSources(), jobVertexBuildContext);

            setOperatorChainedOutputsConfig(config, chainableOutputs, jobVertexBuildContext);
            // we cache the non-chainable outputs here, and set the non-chained config later
            operatorInfo.addNonChainableOutputs(nonChainableOutputs);

            if (currentNodeId.equals(startNodeId)) {
                chainInfo.setTransitiveOutEdges(transitiveOutEdges);
                jobVertexBuildContext.addChainInfo(startNodeId, chainInfo);

                config.setChainStart();
                config.setChainIndex(chainIndex);
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
                config.setTransitiveChainedTaskConfigs(
                        jobVertexBuildContext.getChainedConfigs().get(startNodeId));
            } else {
                config.setChainIndex(chainIndex);
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                config.setOperatorName(node.getOperatorName());
                jobVertexBuildContext
                        .getOrCreateChainedConfig(startNodeId)
                        .put(currentNodeId, config);
            }

            config.setOperatorID(currentOperatorId);

            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }
            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    /**
     * This method is used to reset or set job vertices' parallelism for dynamic graph:
     *
     * <p>1. Reset parallelism for job vertices whose parallelism is not configured.
     *
     * <p>2. Set parallelism and maxParallelism for job vertices in forward group, to ensure the
     * parallelism and maxParallelism of vertices in the same forward group to be the same; set the
     * parallelism at early stage if possible, to avoid invalid partition reuse.
     */
    private void setVertexParallelismsForDynamicGraphIfNecessary() {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        // Note that the jobVertices are reverse topological order
        final List<JobVertex> topologicalOrderVertices =
                IterableUtils.toStream(jobVertices.values()).collect(Collectors.toList());
        Collections.reverse(topologicalOrderVertices);

        // reset parallelism for job vertices whose parallelism is not configured
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    final OperatorChainInfo chainInfo =
                            jobVertexBuildContext.getChainInfo(startNodeId);
                    if (!jobVertex.isParallelismConfigured()
                            && streamGraph.isAutoParallelismEnabled()) {
                        jobVertex.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
                        chainInfo
                                .getAllChainedNodes()
                                .forEach(
                                        n ->
                                                n.setParallelism(
                                                        ExecutionConfig.PARALLELISM_DEFAULT,
                                                        false));
                    }
                });

        final Map<JobVertex, Set<JobVertex>> forwardProducersByJobVertex = new HashMap<>();
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    Set<JobVertex> forwardConsumers =
                            jobVertexBuildContext.getChainInfo(startNodeId).getTransitiveOutEdges()
                                    .stream()
                                    .filter(
                                            edge ->
                                                    edge.getPartitioner()
                                                            instanceof ForwardPartitioner)
                                    .map(StreamEdge::getTargetId)
                                    .map(jobVertices::get)
                                    .collect(Collectors.toSet());

                    for (JobVertex forwardConsumer : forwardConsumers) {
                        forwardProducersByJobVertex.compute(
                                forwardConsumer,
                                (ignored, producers) -> {
                                    if (producers == null) {
                                        producers = new HashSet<>();
                                    }
                                    producers.add(jobVertex);
                                    return producers;
                                });
                    }
                });

        // compute forward groups
        final Map<JobVertexID, ForwardGroup> forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroups(
                        topologicalOrderVertices,
                        jobVertex ->
                                forwardProducersByJobVertex.getOrDefault(
                                        jobVertex, Collections.emptySet()));

        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    // set parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        jobVertex.setParallelism(forwardGroup.getParallelism());
                        jobVertex.setParallelismConfigured(true);
                        jobVertexBuildContext
                                .getChainInfo(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setParallelism(
                                                        forwardGroup.getParallelism(), true));
                    }

                    // set max parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isMaxParallelismDecided()) {
                        jobVertex.setMaxParallelism(forwardGroup.getMaxParallelism());
                        jobVertexBuildContext
                                .getChainInfo(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setMaxParallelism(
                                                        forwardGroup.getMaxParallelism()));
                    }
                });
    }

    private static void checkAndReplaceReusableHybridPartitionType(
            NonChainedOutput reusableOutput) {
        if (reusableOutput.getPartitionType() == ResultPartitionType.HYBRID_SELECTIVE) {
            // for can be reused hybrid output, it can be optimized to always use full
            // spilling strategy to significantly reduce shuffle data writing cost.
            reusableOutput.setPartitionType(ResultPartitionType.HYBRID_FULL);
            LOG.info(
                    "{} result partition has been replaced by {} result partition to support partition reuse,"
                            + " which will reduce shuffle data writing cost.",
                    reusableOutput.getPartitionType().name(),
                    ResultPartitionType.HYBRID_FULL.name());
        }
    }

    public static String createChainedName(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            Optional<OperatorChainInfo> operatorChainInfo,
            Map<Integer, String> chainedNames,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        List<ChainedSourceInfo> chainedSourceInfos =
                operatorChainInfo
                        .map(
                                chainInfo ->
                                        getChainedSourcesByVertexId(
                                                vertexID, chainInfo, streamGraph))
                        .orElse(Collections.emptyList());
        final String operatorName =
                nameWithChainedSourcesInfo(
                        streamGraph.getStreamNode(vertexID).getOperatorName(), chainedSourceInfos);
        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }
    }

    private static List<ChainedSourceInfo> getChainedSourcesByVertexId(
            Integer vertexId, OperatorChainInfo chainInfo, StreamGraph streamGraph) {
        return streamGraph.getStreamNode(vertexId).getInEdges().stream()
                .map(inEdge -> chainInfo.getChainedSources().get(inEdge.getSourceId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static ResourceSpec createChainedMinResources(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            OperatorChainInfo operatorChainInfo,
            JobVertexBuildContext jobVertexBuildContext) {
        ResourceSpec minResources =
                jobVertexBuildContext.getStreamGraph().getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources =
                    minResources.merge(
                            operatorChainInfo.getChainedMinResources(chainable.getTargetId()));
        }
        return minResources;
    }

    public static ResourceSpec createChainedPreferredResources(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            OperatorChainInfo operatorChainInfo,
            JobVertexBuildContext jobVertexBuildContext) {
        ResourceSpec preferredResources =
                jobVertexBuildContext
                        .getStreamGraph()
                        .getStreamNode(vertexID)
                        .getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources =
                    preferredResources.merge(
                            operatorChainInfo.getChainedPreferredResources(
                                    chainable.getTargetId()));
        }
        return preferredResources;
    }

    private StreamConfig createJobVertex(Integer streamNodeId, OperatorChainInfo chainInfo) {

        JobVertex jobVertex;
        StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

        byte[] hash = jobVertexBuildContext.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<ChainedOperatorHashInfo> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (ChainedOperatorHashInfo chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID =
                        chainedOperator.getUserDefinedOperatorId() == null
                                ? null
                                : new OperatorID(chainedOperator.getUserDefinedOperatorId());
                operatorIDPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.getGeneratedOperatorId()),
                                userDefinedOperatorID,
                                chainedOperator.getStreamNode().getOperatorName(),
                                chainedOperator.getStreamNode().getTransformationUID()));
            }
        }

        if (chainInfo.hasFormatContainer()) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIDPairs);
            chainInfo
                    .getOrCreateFormatContainer()
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex =
                    new JobVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIDPairs);
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

        jobVertexBuildContext.addJobVertex(streamNodeId, jobVertex);
        builtVertices.add(streamNodeId);
        jobGraph.addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return new StreamConfig(jobVertex.getConfiguration());
    }

    public static void setOperatorConfig(
            Integer vertexId,
            StreamConfig config,
            Map<Integer, ChainedSourceInfo> chainedSources,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        OperatorInfo operatorInfo = jobVertexBuildContext.getOperatorInfo(vertexId);
        StreamNode vertex = streamGraph.getStreamNode(vertexId);

        config.setVertexID(vertexId);

        // build the inputs as a combination of source and network inputs
        final List<StreamEdge> inEdges = vertex.getInEdges();
        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();

        final StreamConfig.InputConfig[] inputConfigs =
                new StreamConfig.InputConfig[inputSerializers.length];

        int inputGateCount = 0;
        for (final StreamEdge inEdge : inEdges) {
            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex =
                    inEdge.getTypeNumber() == 0
                            ? 0 // single input operator
                            : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException(
                            "Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                jobVertexBuildContext
                        .getOrCreateChainedConfig(vertexId)
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement =
                            vertex.getInputRequirements()
                                    .getOrDefault(
                                            inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] =
                            new StreamConfig.NetworkInputConfig(
                                    inputSerializers[inputIndex],
                                    inputGateCount++,
                                    inputRequirement);
                }
            }
        }

        // set the input config of the vertex if it consumes from cached intermediate dataset.
        if (vertex.getConsumeClusterDatasetId() != null) {
            config.setNumberOfNetworkInputs(1);
            inputConfigs[0] = new StreamConfig.NetworkInputConfig(inputSerializers[0], 0);
        }

        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        config.setStreamOperatorFactory(vertex.getOperatorFactory());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setSerializedStateBackend(
                streamGraph.getCheckpointingSettings().getDefaultStateBackend(),
                Boolean.TRUE.equals(
                        streamGraph
                                .getCheckpointingSettings()
                                .isStateBackendUseManagedMemory()
                                .getAsBoolean()));
        config.setSerializedCheckpointStorage(
                streamGraph.getCheckpointingSettings().getDefaultCheckpointStorage());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.setCheckpointingEnabled(checkpointCfg.isCheckpointingEnabled());
        config.getConfiguration()
                .set(
                        CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                        streamGraph.isEnableCheckpointsAfterTasksFinish());
        config.setCheckpointMode(StreamGraph.getCheckpointingMode(checkpointCfg));
        config.setUnalignedCheckpointsEnabled(checkpointCfg.isUnalignedCheckpointsEnabled());
        config.setUnalignedCheckpointsSplittableTimersEnabled(
                checkpointCfg.isUnalignedCheckpointsInterruptibleTimersEnabled());
        config.setAlignedCheckpointTimeout(checkpointCfg.getAlignedCheckpointTimeout());
        config.setMaxSubtasksPerChannelStateFile(checkpointCfg.getMaxSubtasksPerChannelStateFile());
        config.setMaxConcurrentCheckpoints(checkpointCfg.getMaxConcurrentCheckpoints());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        config.setStateKeySerializer(vertex.getStateKeySerializer());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class)
                || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexId));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexId));
        }

        operatorInfo.setVertexConfig(config);
    }

    public static void setOperatorChainedOutputsConfig(
            StreamConfig config,
            List<StreamEdge> chainableOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        jobVertexBuildContext
                                                .getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
            }
        }
        config.setChainedOutputs(chainableOutputs);
    }

    private static void setOperatorNonChainedOutputsConfig(
            Integer vertexId,
            StreamConfig config,
            List<StreamEdge> nonChainableOutputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            JobVertexBuildContext jobVertexBuildContext) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        jobVertexBuildContext
                                                .getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
            }
        }

        List<NonChainedOutput> deduplicatedOutputs =
                mayReuseNonChainedOutputs(
                        vertexId,
                        nonChainableOutputs,
                        outputsConsumedByEdge,
                        jobVertexBuildContext);
        config.setNumberOfOutputs(deduplicatedOutputs.size());
        config.setOperatorNonChainedOutputs(deduplicatedOutputs);
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            StreamConfig config,
            List<StreamEdge> transitiveOutEdges,
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {

        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            connect(
                    startNodeId,
                    edge,
                    output,
                    jobVertexBuildContext.getJobVerticesInOrder(),
                    jobVertexBuildContext);
        }

        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    public static void setAllOperatorNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        // set non chainable output config
        jobVertexBuildContext
                .getOperatorInfos()
                .forEach(
                        (vertexId, operatorInfo) -> {
                            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge =
                                    opIntermediateOutputs.computeIfAbsent(
                                            vertexId, ignored -> new HashMap<>());
                            setOperatorNonChainedOutputsConfig(
                                    vertexId,
                                    operatorInfo.getVertexConfig(),
                                    operatorInfo.getNonChainableOutputs(),
                                    outputsConsumedByEdge,
                                    jobVertexBuildContext);
                        });
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        jobVertexBuildContext
                .getJobVerticesInOrder()
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId,
                                        jobVertexBuildContext
                                                .getOperatorInfo(startNodeId)
                                                .getVertexConfig(),
                                        jobVertexBuildContext
                                                .getChainInfo(startNodeId)
                                                .getTransitiveOutEdges(),
                                        opIntermediateOutputs));
    }

    private static List<NonChainedOutput> mayReuseNonChainedOutputs(
            int vertexId,
            List<StreamEdge> consumerEdges,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            JobVertexBuildContext jobVertexBuildContext) {
        if (consumerEdges.isEmpty()) {
            return new ArrayList<>();
        }
        List<NonChainedOutput> outputs = new ArrayList<>(consumerEdges.size());
        for (StreamEdge consumerEdge : consumerEdges) {
            checkState(vertexId == consumerEdge.getSourceId(), "Vertex id must be the same.");
            ResultPartitionType partitionType =
                    getResultPartitionType(consumerEdge, jobVertexBuildContext);
            IntermediateDataSetID dataSetId = new IntermediateDataSetID();

            boolean isPersistentDataSet =
                    isPersistentIntermediateDataset(partitionType, consumerEdge);
            if (isPersistentDataSet) {
                partitionType = ResultPartitionType.BLOCKING_PERSISTENT;
                dataSetId = consumerEdge.getIntermediateDatasetIdToProduce();
            }

            if (partitionType.isHybridResultPartition()) {
                jobVertexBuildContext.setHasHybridResultPartition(true);
                if (consumerEdge.getPartitioner().isBroadcast()
                        && partitionType == ResultPartitionType.HYBRID_SELECTIVE) {
                    // for broadcast result partition, it can be optimized to always use full
                    // spilling strategy to significantly reduce shuffle data writing cost.
                    LOG.info(
                            "{} result partition has been replaced by {} result partition to support "
                                    + "broadcast optimization, which will reduce shuffle data writing cost.",
                            partitionType.name(),
                            ResultPartitionType.HYBRID_FULL.name());
                    partitionType = ResultPartitionType.HYBRID_FULL;
                }
            }

            createOrReuseOutput(
                    outputs,
                    outputsConsumedByEdge,
                    consumerEdge,
                    isPersistentDataSet,
                    dataSetId,
                    partitionType,
                    jobVertexBuildContext.getStreamGraph());
        }
        return outputs;
    }

    private static void createOrReuseOutput(
            List<NonChainedOutput> outputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            StreamEdge consumerEdge,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            ResultPartitionType partitionType,
            StreamGraph streamGraph) {
        int consumerParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getParallelism();
        int consumerMaxParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getMaxParallelism();
        NonChainedOutput reusableOutput = null;
        if (isPartitionTypeCanBeReuse(partitionType)) {
            for (NonChainedOutput outputCandidate : outputsConsumedByEdge.values()) {
                // Reusing the same output can improve performance. The target output can be reused
                // if meeting the following conditions:
                // 1. all is hybrid partition or are same re-consumable partition.
                // 2. have the same partitioner, consumer parallelism, persistentDataSetId,
                // outputTag.
                if (allHybridOrSameReconsumablePartitionType(
                                outputCandidate.getPartitionType(), partitionType)
                        && consumerParallelism == outputCandidate.getConsumerParallelism()
                        && consumerMaxParallelism == outputCandidate.getConsumerMaxParallelism()
                        && Objects.equals(
                                outputCandidate.getPersistentDataSetId(),
                                consumerEdge.getIntermediateDatasetIdToProduce())
                        && Objects.equals(
                                outputCandidate.getOutputTag(), consumerEdge.getOutputTag())
                        && Objects.equals(
                                consumerEdge.getPartitioner(), outputCandidate.getPartitioner())) {
                    reusableOutput = outputCandidate;
                    outputsConsumedByEdge.put(consumerEdge, reusableOutput);
                    checkAndReplaceReusableHybridPartitionType(reusableOutput);
                    break;
                }
            }
        }
        if (reusableOutput == null) {
            NonChainedOutput output =
                    new NonChainedOutput(
                            consumerEdge.supportsUnalignedCheckpoints(),
                            consumerEdge.getSourceId(),
                            consumerParallelism,
                            consumerMaxParallelism,
                            consumerEdge.getBufferTimeout(),
                            isPersistentDataSet,
                            dataSetId,
                            consumerEdge.getOutputTag(),
                            consumerEdge.getPartitioner(),
                            partitionType);
            outputs.add(output);
            outputsConsumedByEdge.put(consumerEdge, output);
        }
    }

    private static boolean isPartitionTypeCanBeReuse(ResultPartitionType partitionType) {
        // for non-hybrid partition, partition reuse only works when its re-consumable.
        // for hybrid selective partition, it still has the opportunity to be converted to
        // hybrid full partition to support partition reuse.
        return partitionType.isReconsumable() || partitionType.isHybridResultPartition();
    }

    private static boolean allHybridOrSameReconsumablePartitionType(
            ResultPartitionType partitionType1, ResultPartitionType partitionType2) {
        return (partitionType1.isReconsumable() && partitionType1 == partitionType2)
                || (partitionType1.isHybridResultPartition()
                        && partitionType2.isHybridResultPartition());
    }

    public static void tryConvertPartitionerForDynamicGraph(
            List<StreamEdge> chainableOutputs,
            List<StreamEdge> nonChainableOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        for (StreamEdge edge : chainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner
                    || partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        String.format(
                                "%s should only be used in dynamic graph.",
                                partitioner.getClass().getSimpleName()));
                edge.setPartitioner(new ForwardPartitioner<>());
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForConsecutiveHashPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner());
            } else if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForUnspecifiedPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(new RescalePartitioner<>());
            }
        }
    }

    public static void connect(
            Integer headOfChain,
            StreamEdge edge,
            NonChainedOutput output,
            Map<Integer, JobVertex> jobVertices,
            JobVertexBuildContext jobVertexBuildContext) {

        jobVertexBuildContext.addPhysicalEdgesInOrder(edge);

        Integer downStreamVertexID = edge.getTargetId();

        JobVertex headVertex = jobVertices.get(headOfChain);
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);

        StreamPartitioner<?> partitioner = output.getPartitioner();
        ResultPartitionType resultPartitionType = output.getPartitionType();

        checkBufferTimeout(resultPartitionType, edge);

        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast());
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
        jobEdge.setForward(partitioner instanceof ForwardPartitioner);
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CONNECTED: {} - {} -> {}",
                    partitioner.getClass().getSimpleName(),
                    headOfChain,
                    downStreamVertexID);
        }
    }

    private static boolean isPersistentIntermediateDataset(
            ResultPartitionType resultPartitionType, StreamEdge edge) {
        return resultPartitionType.isBlockingOrBlockingPersistentResultPartition()
                && edge.getIntermediateDatasetIdToProduce() != null;
    }

    private static void checkBufferTimeout(ResultPartitionType type, StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (!type.canBePipelinedConsumed()
                && bufferTimeout != ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "only canBePipelinedConsumed partition support buffer timeout "
                            + bufferTimeout
                            + " for src operator in edge "
                            + edge
                            + ". \nPlease either disable buffer timeout (via -1) or use the canBePipelinedConsumed partition.");
        }
    }

    private static ResultPartitionType getResultPartitionType(
            StreamEdge edge, JobVertexBuildContext jobVertexBuildContext) {
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case BATCH:
                return ResultPartitionType.BLOCKING;
            case HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            case UNDEFINED:
                return determineUndefinedResultPartitionType(edge, jobVertexBuildContext);
            default:
                throw new UnsupportedOperationException(
                        "Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }
    }

    public static ResultPartitionType determineUndefinedResultPartitionType(
            StreamEdge edge, JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        Attribute sourceNodeAttribute =
                streamGraph.getStreamNode(edge.getSourceId()).getAttribute();
        if (sourceNodeAttribute.isNoOutputUntilEndOfInput()) {
            edge.setBufferTimeout(ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
            return ResultPartitionType.BLOCKING;
        }

        StreamPartitioner<?> partitioner = edge.getPartitioner();
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            case ALL_EDGES_HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case ALL_EDGES_HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            default:
                throw new RuntimeException(
                        "Unrecognized global data exchange mode "
                                + streamGraph.getGlobalStreamExchangeMode());
        }
    }

    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph);
    }

    public static boolean isChainableInput(StreamEdge edge, StreamGraph streamGraph) {
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        if (!(streamGraph.isChainingEnabled()
                && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
                && areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
                && arePartitionerAndExchangeModeChainable(
                        edge.getPartitioner(), edge.getExchangeMode(), streamGraph.isDynamic()))) {

            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(
            StreamNode upStreamVertex, StreamNode downStreamVertex, StreamGraph streamGraph) {
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this
        // point
        if (downStreamOperator instanceof YieldingOperatorFactory
                && getHeadOperator(upStreamVertex, streamGraph).isLegacySource()) {
            return false;
        }

        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;

        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }

        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + downStreamOperator.getChainingStrategy());
        }

        // Only vertices with the same parallelism can be chained.
        isChainable &= upStreamVertex.getParallelism() == downStreamVertex.getParallelism();

        if (!streamGraph.isChainingOfOperatorsWithDifferentMaxParallelismEnabled()) {
            isChainable &=
                    upStreamVertex.getMaxParallelism() == downStreamVertex.getMaxParallelism();
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(
            StreamNode upStreamVertex, StreamGraph streamGraph) {
        if (streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex) == null) {
            if (upStreamVertex.getInEdges().size() == 1
                    && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
                StreamOperatorFactory<?> headOperator =
                        getHeadOperator(
                                streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)),
                                streamGraph);
                streamGraph.cacheHeadOperatorForNode(upStreamVertex, headOperator);
            } else {
                Preconditions.checkNotNull(upStreamVertex.getOperatorFactory());
                streamGraph.cacheHeadOperatorForNode(
                        upStreamVertex, upStreamVertex.getOperatorFactory());
            }
        }

        return streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex);
    }

    public static void markSupportingConcurrentExecutionAttempts(
            JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final JobVertex jobVertex = entry.getValue();
            final Set<Integer> vertexOperators = new HashSet<>();
            vertexOperators.add(entry.getKey());
            final Map<Integer, StreamConfig> vertexChainedConfigs =
                    jobVertexBuildContext.getChainedConfigs().get(entry.getKey());
            if (vertexChainedConfigs != null) {
                vertexOperators.addAll(vertexChainedConfigs.keySet());
            }

            // disable supportConcurrentExecutionAttempts of job vertex if there is any stream node
            // does not support it
            boolean supportConcurrentExecutionAttempts = true;
            for (int nodeId : vertexOperators) {
                final StreamNode streamNode = streamGraph.getStreamNode(nodeId);
                if (!streamNode.isSupportsConcurrentExecutionAttempts()) {
                    supportConcurrentExecutionAttempts = false;
                    break;
                }
            }
            jobVertex.setSupportsConcurrentExecutionAttempts(supportConcurrentExecutionAttempts);
        }
    }

    public static void setSlotSharingAndCoLocation(
            JobGraph jobGraph, JobVertexBuildContext jobVertexBuildContext) {
        setSlotSharing(jobGraph, jobVertexBuildContext);
        setCoLocation(jobVertexBuildContext);
    }

    private static void setSlotSharing(
            JobGraph jobGraph, JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups =
                buildVertexRegionSlotSharingGroups(jobGraph, jobVertexBuildContext);
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey =
                    streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup =
                        checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                checkState(
                        !jobVertexBuildContext.hasHybridResultPartition(),
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

                effectiveSlotSharingGroup =
                        specifiedSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupKey,
                                k -> {
                                    SlotSharingGroup ssg = new SlotSharingGroup();
                                    streamGraph
                                            .getSlotSharingGroupResource(k)
                                            .ifPresent(ssg::setResourceProfile);
                                    return ssg;
                                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    public static void validateHybridShuffleExecuteInBatchMode(
            JobVertexBuildContext jobVertexBuildContext) {
        if (jobVertexBuildContext.hasHybridResultPartition()) {
            checkState(
                    jobVertexBuildContext.getStreamGraph().getJobType() == JobType.BATCH,
                    "hybrid shuffle mode only supports batch job, please set %s to %s",
                    ExecutionOptions.RUNTIME_MODE.key(),
                    RuntimeExecutionMode.BATCH.name());
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private static Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups(
            JobGraph jobGraph, JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup =
                streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        final Iterable<DefaultLogicalPipelinedRegion> regions =
                DefaultLogicalTopology.fromJobGraph(jobGraph).getAllPipelinedRegions();
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private static void setCoLocation(JobVertexBuildContext jobVertexBuildContext) {
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups =
                new HashMap<>();
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException(
                            "Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint =
                        coLocationGroups.computeIfAbsent(
                                coLocationGroupKey,
                                k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException(
                            "Cannot co-locate operators from different slot sharing groups");
                }

                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    public static void setManagedMemoryFraction(JobVertexBuildContext jobVertexBuildContext) {

        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(
                    jobVertexSlotSharingGroup != null,
                    "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(
                    jobVertexBuildContext
                            .getChainedConfigs()
                            .getOrDefault(headOperatorId, Collections.emptyMap())
                            .keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(
                    slotSharingGroup, vertexHeadOperators, vertexOperators, jobVertexBuildContext);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(
            final SlotSharingGroup slotSharingGroup,
            final Map<JobVertexID, Integer> vertexHeadOperators,
            final Map<JobVertexID, Set<Integer>> vertexOperators,
            final JobVertexBuildContext jobVertexBuildContext) {
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        final Set<Integer> groupOperatorIds =
                slotSharingGroup.getJobVertexIds().stream()
                        .flatMap((vid) -> vertexOperators.get(vid).stream())
                        .collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        streamGraph.getStreamNode(oid)
                                                .getManagedMemoryOperatorScopeUseCaseWeights()
                                                .entrySet().stream())
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getKey,
                                        Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        streamGraph.getStreamNode(oid)
                                                .getManagedMemorySlotScopeUseCases().stream())
                        .collect(Collectors.toSet());

        for (JobVertexID jobVertexID : slotSharingGroup.getJobVertexIds()) {
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig =
                        jobVertexBuildContext.getOperatorInfo(operatorNodeId).getVertexConfig();
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights =
                        streamGraph
                                .getStreamNode(operatorNodeId)
                                .getManagedMemoryOperatorScopeUseCaseWeights();
                final Set<ManagedMemoryUseCase> slotScopeUseCases =
                        streamGraph
                                .getStreamNode(operatorNodeId)
                                .getManagedMemorySlotScopeUseCases();
                setManagedMemoryFractionForOperator(
                        operatorScopeUseCaseWeights,
                        slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights,
                        groupSlotScopeUseCases,
                        operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            final StreamConfig vertexConfig =
                    jobVertexBuildContext.getOperatorInfo(headOperatorNodeId).getVertexConfig();
            vertexConfig.setTransitiveChainedTaskConfigs(
                    vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(
            final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            final Set<ManagedMemoryUseCase> slotScopeUseCases,
            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase,
                    operatorWeight > 0
                            ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight)
                            : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }

    public static void configureCheckpointing(StreamGraph streamGraph, JobGraph jobGraph) {
        jobGraph.setSnapshotSettings(streamGraph.getCheckpointingSettings());
    }

    private static String nameWithChainedSourcesInfo(
            String operatorName, Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty()
                ? operatorName
                : String.format(
                        "%s [%s]",
                        operatorName,
                        chainedSourceInfos.stream()
                                .map(
                                        chainedSourceInfo ->
                                                chainedSourceInfo
                                                        .getOperatorConfig()
                                                        .getOperatorName())
                                .collect(Collectors.joining(", ")));
    }
}

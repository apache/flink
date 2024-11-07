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
import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.api.checkpoint.WithMasterCheckpointHook;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.lineage.LineageGraph;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.UdfStreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class representing the streaming topology. It contains all the information necessary to build the
 * jobgraph for the execution.
 */
@Internal
public class StreamGraph implements Pipeline, ExecutionPlan {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(StreamGraph.class);

    private long initialClientHeartbeatTimeout;

    private String jobName;

    private JobID jobId;

    private final Configuration jobConfiguration;
    private transient ExecutionConfig executionConfig;
    private final CheckpointConfig checkpointConfig;
    private SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();

    private GlobalStreamExchangeMode globalExchangeMode;

    private boolean enableCheckpointsAfterTasksFinish;

    /** Flag to indicate whether to put all vertices into the same slot sharing group by default. */
    private boolean allVerticesInSameSlotSharingGroupByDefault = true;

    private transient Map<Integer, StreamNode> streamNodes;
    private Set<Integer> sources;
    private Set<Integer> sinks;
    private transient Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
    private transient Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
            virtualPartitionNodes;

    protected Map<Integer, String> vertexIDtoBrokerID;
    protected Map<Integer, Long> vertexIDtoLoopTimeout;
    private transient StateBackend stateBackend;
    private transient CheckpointStorage checkpointStorage;
    private InternalTimeServiceManager.Provider timerServiceProvider;
    private transient LineageGraph lineageGraph;
    private JobType jobType = JobType.STREAMING;
    private Map<String, ResourceProfile> slotSharingGroupResources;
    private PipelineOptions.VertexDescriptionMode descriptionMode =
            PipelineOptions.VertexDescriptionMode.TREE;
    private boolean vertexNameIncludeIndexPrefix = false;

    private final List<JobStatusHook> jobStatusHooks = new ArrayList<>();

    private boolean dynamic;

    private boolean autoParallelismEnabled;

    private final Map<StreamNode, StreamOperatorFactory<?>> nodeToHeadOperatorCache =
            new HashMap<>();

    private JobCheckpointingSettings checkpointingSettings;

    /** Set of blob keys identifying the JAR files required to run this job. */
    private final List<PermanentBlobKey> userJarBlobKeys = new ArrayList<>();

    /** Set of custom files required to run this job. */
    private final Map<String, DistributedCache.DistributedCacheEntry> userArtifacts =
            new HashMap<>();

    /** List of classpath required to run this job. */
    private List<URL> classpath = Collections.emptyList();

    /** Set of JAR files required to run this job. */
    private final List<Path> userJars = new ArrayList<>();

    private boolean isEmpty;

    private UserDefinedObjectsHolder userDefinedObjectsHolder;

    private final Map<Integer, ResourceSpec> streamNodeMinResources = new HashMap<>();

    public StreamGraph(
            Configuration jobConfiguration,
            ExecutionConfig executionConfig,
            CheckpointConfig checkpointConfig,
            SavepointRestoreSettings savepointRestoreSettings) {
        this.jobConfiguration = new Configuration(checkNotNull(jobConfiguration));
        this.executionConfig = checkNotNull(executionConfig);
        this.checkpointConfig = checkNotNull(checkpointConfig);
        this.savepointRestoreSettings = checkNotNull(savepointRestoreSettings);
        this.jobId = new JobID();
        this.jobName = "(unnamed job)";

        // create an empty new stream graph.
        clear();
    }

    /** Remove all registered nodes etc. */
    public void clear() {
        streamNodes = new HashMap<>();
        virtualSideOutputNodes = new HashMap<>();
        virtualPartitionNodes = new HashMap<>();
        vertexIDtoBrokerID = new HashMap<>();
        vertexIDtoLoopTimeout = new HashMap<>();
        sources = new HashSet<>();
        sinks = new HashSet<>();
        slotSharingGroupResources = new HashMap<>();
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public CheckpointConfig getCheckpointConfig() {
        return checkpointConfig;
    }

    void cacheHeadOperatorForNode(StreamNode node, StreamOperatorFactory<?> headOperator) {
        nodeToHeadOperatorCache.put(node, headOperator);
    }

    StreamOperatorFactory<?> getHeadOperatorForNodeFromCache(StreamNode node) {
        return nodeToHeadOperatorCache.get(node);
    }

    public CheckpointingMode getCheckpointingMode() {
        return getCheckpointingMode(this.checkpointConfig);
    }

    public static CheckpointingMode getCheckpointingMode(CheckpointConfig checkpointConfig) {
        CheckpointingMode checkpointingMode = checkpointConfig.getCheckpointingConsistencyMode();

        checkArgument(
                checkpointingMode == CheckpointingMode.EXACTLY_ONCE
                        || checkpointingMode == CheckpointingMode.AT_LEAST_ONCE,
                "Unexpected checkpointing mode.");

        if (checkpointConfig.isCheckpointingEnabled()) {
            return checkpointingMode;
        } else {
            // the "at-least-once" input handler is slightly cheaper (in the absence of
            // checkpoints), so we use that one if checkpointing is not enabled
            return CheckpointingMode.AT_LEAST_ONCE;
        }
    }

    /**
     * Adds the path of a JAR file required to run the job on a task manager.
     *
     * @param jar path of the JAR file required to run the job on a task manager
     */
    public void addJar(Path jar) {
        if (jar == null) {
            throw new IllegalArgumentException();
        }

        if (!userJars.contains(jar)) {
            userJars.add(jar);
        }
    }

    /**
     * Gets the list of assigned user jar paths.
     *
     * @return The list of assigned user jar paths
     */
    @Override
    public List<Path> getUserJars() {
        return userJars;
    }

    public void createJobCheckpointingSettings() {
        this.checkpointingSettings = createJobCheckpointingSettingsInternal();
    }

    private JobCheckpointingSettings createJobCheckpointingSettingsInternal() {
        CheckpointConfig cfg = getCheckpointConfig();

        long interval = cfg.getCheckpointInterval();
        if (interval < MINIMAL_CHECKPOINT_TIME) {
            interval = CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
        }

        //  --- configure options ---

        CheckpointRetentionPolicy retentionAfterTermination;
        if (cfg.isExternalizedCheckpointsEnabled()) {
            ExternalizedCheckpointRetention cleanup = cfg.getExternalizedCheckpointRetention();
            // Sanity check
            if (cleanup == null) {
                throw new IllegalStateException(
                        "Externalized checkpoints enabled, but no cleanup mode configured.");
            }
            retentionAfterTermination =
                    cleanup.deleteOnCancellation()
                            ? CheckpointRetentionPolicy.RETAIN_ON_FAILURE
                            : CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION;
        } else {
            retentionAfterTermination = CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION;
        }

        //  --- configure the master-side checkpoint hooks ---

        final ArrayList<MasterTriggerRestoreHook.Factory> hooks = new ArrayList<>();

        for (StreamNode node : getStreamNodes()) {
            if (node.getOperatorFactory() != null
                    && node.getOperatorFactory() instanceof UdfStreamOperatorFactory) {
                Function f =
                        ((UdfStreamOperatorFactory<?>) node.getOperatorFactory()).getUserFunction();

                if (f instanceof WithMasterCheckpointHook) {
                    hooks.add(
                            new FunctionMasterCheckpointHookFactory(
                                    (WithMasterCheckpointHook<?>) f));
                }
            }
        }

        // because the hooks can have user-defined code, they need to be stored as
        // eagerly serialized values
        final SerializedValue<MasterTriggerRestoreHook.Factory[]> serializedHooks;
        if (hooks.isEmpty()) {
            serializedHooks = null;
        } else {
            try {
                MasterTriggerRestoreHook.Factory[] asArray =
                        hooks.toArray(new MasterTriggerRestoreHook.Factory[0]);
                serializedHooks = new SerializedValue<>(asArray);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Trigger/restore hook is not serializable", e);
            }
        }

        // because the state backend can have user-defined code, it needs to be stored as
        // eagerly serialized value
        SerializedValue<StateBackend> serializedStateBackend;
        if (this.stateBackend == null) {
            serializedStateBackend = null;
        } else {
            try {
                serializedStateBackend = new SerializedValue<>(this.stateBackend);
            } catch (IOException e) {
                throw new FlinkRuntimeException("State backend is not serializable", e);
            }
        }

        // because the checkpoint storage can have user-defined code, it needs to be stored as
        // eagerly serialized value
        SerializedValue<CheckpointStorage> serializedCheckpointStorage;
        if (this.checkpointStorage == null) {
            serializedCheckpointStorage = null;
        } else {
            try {
                serializedCheckpointStorage = new SerializedValue<>(this.checkpointStorage);
            } catch (IOException e) {
                throw new FlinkRuntimeException("Checkpoint storage is not serializable", e);
            }
        }

        //  --- done, put it all together ---

        return new JobCheckpointingSettings(
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(interval)
                        .setCheckpointIntervalDuringBacklog(
                                cfg.getCheckpointIntervalDuringBacklog())
                        .setCheckpointTimeout(cfg.getCheckpointTimeout())
                        .setMinPauseBetweenCheckpoints(cfg.getMinPauseBetweenCheckpoints())
                        .setMaxConcurrentCheckpoints(cfg.getMaxConcurrentCheckpoints())
                        .setCheckpointRetentionPolicy(retentionAfterTermination)
                        .setExactlyOnce(getCheckpointingMode() == CheckpointingMode.EXACTLY_ONCE)
                        .setTolerableCheckpointFailureNumber(
                                cfg.getTolerableCheckpointFailureNumber())
                        .setUnalignedCheckpointsEnabled(cfg.isUnalignedCheckpointsEnabled())
                        .setCheckpointIdOfIgnoredInFlightData(
                                cfg.getCheckpointIdOfIgnoredInFlightData())
                        .setAlignedCheckpointTimeout(cfg.getAlignedCheckpointTimeout().toMillis())
                        .setEnableCheckpointsAfterTasksFinish(isEnableCheckpointsAfterTasksFinish())
                        .build(),
                serializedStateBackend,
                getJobConfiguration()
                        .getOptional(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG)
                        .map(TernaryBoolean::fromBoolean)
                        .orElse(TernaryBoolean.UNDEFINED),
                serializedCheckpointStorage,
                serializedHooks,
                Optional.ofNullable(stateBackend)
                        .map(StateBackend::useManagedMemory)
                        .map(TernaryBoolean::fromBoolean)
                        .orElse(TernaryBoolean.UNDEFINED));
    }

    @Override
    public void setSavepointRestoreSettings(SavepointRestoreSettings savepointRestoreSettings) {
        this.savepointRestoreSettings = savepointRestoreSettings;
    }

    @Override
    public SerializedValue<ExecutionConfig> getSerializedExecutionConfig() {
        checkNotNull(userDefinedObjectsHolder);
        return userDefinedObjectsHolder.serializedExecutionConfig;
    }

    @Override
    public SavepointRestoreSettings getSavepointRestoreSettings() {
        return savepointRestoreSettings;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public LineageGraph getLineageGraph() {
        return lineageGraph;
    }

    public void setLineageGraph(LineageGraph lineageGraph) {
        this.lineageGraph = lineageGraph;
    }

    public void setStateBackend(StateBackend backend) {
        this.stateBackend = backend;
    }

    @VisibleForTesting
    public StateBackend getStateBackend() {
        return this.stateBackend;
    }

    public void setCheckpointStorage(CheckpointStorage checkpointStorage) {
        this.checkpointStorage = checkpointStorage;
    }

    public InternalTimeServiceManager.Provider getTimerServiceProvider() {
        return timerServiceProvider;
    }

    public void setTimerServiceProvider(InternalTimeServiceManager.Provider timerServiceProvider) {
        this.timerServiceProvider = checkNotNull(timerServiceProvider);
    }

    public GlobalStreamExchangeMode getGlobalStreamExchangeMode() {
        return globalExchangeMode;
    }

    public void setGlobalStreamExchangeMode(GlobalStreamExchangeMode globalExchangeMode) {
        this.globalExchangeMode = globalExchangeMode;
    }

    public void setSlotSharingGroupResource(
            Map<String, ResourceProfile> slotSharingGroupResources) {
        this.slotSharingGroupResources.putAll(slotSharingGroupResources);
    }

    public Optional<ResourceProfile> getSlotSharingGroupResource(String groupId) {
        return Optional.ofNullable(slotSharingGroupResources.get(groupId));
    }

    public boolean hasFineGrainedResource() {
        return slotSharingGroupResources.values().stream()
                .anyMatch(resourceProfile -> !resourceProfile.equals(ResourceProfile.UNKNOWN));
    }

    /**
     * Set whether to put all vertices into the same slot sharing group by default.
     *
     * @param allVerticesInSameSlotSharingGroupByDefault indicates whether to put all vertices into
     *     the same slot sharing group by default.
     */
    public void setAllVerticesInSameSlotSharingGroupByDefault(
            boolean allVerticesInSameSlotSharingGroupByDefault) {
        this.allVerticesInSameSlotSharingGroupByDefault =
                allVerticesInSameSlotSharingGroupByDefault;
    }

    /**
     * Gets whether to put all vertices into the same slot sharing group by default.
     *
     * @return whether to put all vertices into the same slot sharing group by default.
     */
    public boolean isAllVerticesInSameSlotSharingGroupByDefault() {
        return allVerticesInSameSlotSharingGroupByDefault;
    }

    public boolean isEnableCheckpointsAfterTasksFinish() {
        return enableCheckpointsAfterTasksFinish;
    }

    public void setEnableCheckpointsAfterTasksFinish(boolean enableCheckpointsAfterTasksFinish) {
        this.enableCheckpointsAfterTasksFinish = enableCheckpointsAfterTasksFinish;
    }

    // Checkpointing

    public boolean isChainingEnabled() {
        return jobConfiguration.get(PipelineOptions.OPERATOR_CHAINING);
    }

    public boolean isChainingOfOperatorsWithDifferentMaxParallelismEnabled() {
        return jobConfiguration.get(
                PipelineOptions.OPERATOR_CHAINING_CHAIN_OPERATORS_WITH_DIFFERENT_MAX_PARALLELISM);
    }

    public boolean isIterative() {
        return !vertexIDtoLoopTimeout.isEmpty();
    }

    public <IN, OUT> void addSource(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            SourceOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName,
                SourceOperatorStreamTask.class);
        sources.add(vertexID);
    }

    public <IN, OUT> void addLegacySource(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName);
        sources.add(vertexID);
    }

    public <IN, OUT> void addSink(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName);
        if (operatorFactory instanceof OutputFormatOperatorFactory) {
            setOutputFormat(
                    vertexID, ((OutputFormatOperatorFactory) operatorFactory).getOutputFormat());
        }
        sinks.add(vertexID);
    }

    public <IN, OUT> void addOperator(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {
        Class<? extends TaskInvokable> invokableClass =
                operatorFactory.isStreamSource()
                        ? SourceStreamTask.class
                        : OneInputStreamTask.class;
        addOperator(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                operatorFactory,
                inTypeInfo,
                outTypeInfo,
                operatorName,
                invokableClass);
    }

    private <IN, OUT> void addOperator(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeInformation<IN> inTypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName,
            Class<? extends TaskInvokable> invokableClass) {

        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                invokableClass,
                operatorFactory,
                operatorName);
        setSerializers(vertexID, createSerializer(inTypeInfo), null, createSerializer(outTypeInfo));

        if (operatorFactory.isOutputTypeConfigurable() && outTypeInfo != null) {
            // sets the output type which must be know at StreamGraph creation time
            operatorFactory.setOutputType(outTypeInfo, executionConfig);
        }

        if (operatorFactory.isInputTypeConfigurable()) {
            operatorFactory.setInputType(inTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Vertex: {}", vertexID);
        }
    }

    public <IN1, IN2, OUT> void addCoOperator(
            Integer vertexID,
            String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> taskOperatorFactory,
            TypeInformation<IN1> in1TypeInfo,
            TypeInformation<IN2> in2TypeInfo,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {

        Class<? extends TaskInvokable> vertexClass = TwoInputStreamTask.class;

        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                vertexClass,
                taskOperatorFactory,
                operatorName);

        TypeSerializer<OUT> outSerializer = createSerializer(outTypeInfo);

        setSerializers(
                vertexID,
                in1TypeInfo.createSerializer(executionConfig.getSerializerConfig()),
                in2TypeInfo.createSerializer(executionConfig.getSerializerConfig()),
                outSerializer);

        if (taskOperatorFactory.isOutputTypeConfigurable()) {
            // sets the output type which must be known at StreamGraph creation time
            taskOperatorFactory.setOutputType(outTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("CO-TASK: {}", vertexID);
        }
    }

    public <OUT> void addMultipleInputOperator(
            Integer vertexID,
            String slotSharingGroup,
            @Nullable String coLocationGroup,
            StreamOperatorFactory<OUT> operatorFactory,
            List<TypeInformation<?>> inTypeInfos,
            TypeInformation<OUT> outTypeInfo,
            String operatorName) {

        Class<? extends TaskInvokable> vertexClass = MultipleInputStreamTask.class;

        addNode(
                vertexID,
                slotSharingGroup,
                coLocationGroup,
                vertexClass,
                operatorFactory,
                operatorName);

        setSerializers(vertexID, inTypeInfos, createSerializer(outTypeInfo));

        if (operatorFactory.isOutputTypeConfigurable()) {
            // sets the output type which must be known at StreamGraph creation time
            operatorFactory.setOutputType(outTypeInfo, executionConfig);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("CO-TASK: {}", vertexID);
        }
    }

    protected StreamNode addNode(
            Integer vertexID,
            @Nullable String slotSharingGroup,
            @Nullable String coLocationGroup,
            Class<? extends TaskInvokable> vertexClass,
            @Nullable StreamOperatorFactory<?> operatorFactory,
            String operatorName) {

        if (streamNodes.containsKey(vertexID)) {
            throw new RuntimeException("Duplicate vertexID " + vertexID);
        }

        StreamNode vertex =
                new StreamNode(
                        vertexID,
                        slotSharingGroup,
                        coLocationGroup,
                        operatorFactory,
                        operatorName,
                        vertexClass);

        streamNodes.put(vertexID, vertex);
        isEmpty = false;

        return vertex;
    }

    /**
     * Adds a new virtual node that is used to connect a downstream vertex to only the outputs with
     * the selected side-output {@link OutputTag}.
     *
     * @param originalId ID of the node that should be connected to.
     * @param virtualId ID of the virtual node.
     * @param outputTag The selected side-output {@code OutputTag}.
     */
    public void addVirtualSideOutputNode(
            Integer originalId, Integer virtualId, OutputTag outputTag) {

        if (virtualSideOutputNodes.containsKey(virtualId)) {
            throw new IllegalStateException("Already has virtual output node with id " + virtualId);
        }

        // verify that we don't already have a virtual node for the given originalId/outputTag
        // combination with a different TypeInformation. This would indicate that someone is trying
        // to read a side output from an operation with a different type for the same side output
        // id.

        for (Tuple2<Integer, OutputTag> tag : virtualSideOutputNodes.values()) {
            if (!tag.f0.equals(originalId)) {
                // different source operator
                continue;
            }

            if (tag.f1.getId().equals(outputTag.getId())
                    && !tag.f1.getTypeInfo().equals(outputTag.getTypeInfo())) {
                throw new IllegalArgumentException(
                        "Trying to add a side output for the same "
                                + "side-output id with a different type. This is not allowed. Side-output ID: "
                                + tag.f1.getId());
            }
        }

        virtualSideOutputNodes.put(virtualId, new Tuple2<>(originalId, outputTag));
    }

    /**
     * Adds a new virtual node that is used to connect a downstream vertex to an input with a
     * certain partitioning.
     *
     * <p>When adding an edge from the virtual node to a downstream node the connection will be made
     * to the original node, but with the partitioning given here.
     *
     * @param originalId ID of the node that should be connected to.
     * @param virtualId ID of the virtual node.
     * @param partitioner The partitioner
     */
    public void addVirtualPartitionNode(
            Integer originalId,
            Integer virtualId,
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode) {

        if (virtualPartitionNodes.containsKey(virtualId)) {
            throw new IllegalStateException(
                    "Already has virtual partition node with id " + virtualId);
        }

        virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, exchangeMode));
    }

    /** Determines the slot sharing group of an operation across virtual nodes. */
    public String getSlotSharingGroup(Integer id) {
        if (virtualSideOutputNodes.containsKey(id)) {
            Integer mappedId = virtualSideOutputNodes.get(id).f0;
            return getSlotSharingGroup(mappedId);
        } else if (virtualPartitionNodes.containsKey(id)) {
            Integer mappedId = virtualPartitionNodes.get(id).f0;
            return getSlotSharingGroup(mappedId);
        } else {
            StreamNode node = getStreamNode(id);
            return node.getSlotSharingGroup();
        }
    }

    public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
        addEdge(upStreamVertexID, downStreamVertexID, typeNumber, null);
    }

    public void addEdge(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            IntermediateDataSetID intermediateDataSetId) {
        addEdgeInternal(
                upStreamVertexID,
                downStreamVertexID,
                typeNumber,
                null,
                new ArrayList<String>(),
                null,
                null,
                intermediateDataSetId);
    }

    private void addEdgeInternal(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            StreamPartitioner<?> partitioner,
            List<String> outputNames,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            IntermediateDataSetID intermediateDataSetId) {

        if (virtualSideOutputNodes.containsKey(upStreamVertexID)) {
            int virtualId = upStreamVertexID;
            upStreamVertexID = virtualSideOutputNodes.get(virtualId).f0;
            if (outputTag == null) {
                outputTag = virtualSideOutputNodes.get(virtualId).f1;
            }
            addEdgeInternal(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    null,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
        } else if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
            int virtualId = upStreamVertexID;
            upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
            if (partitioner == null) {
                partitioner = virtualPartitionNodes.get(virtualId).f1;
            }
            exchangeMode = virtualPartitionNodes.get(virtualId).f2;
            addEdgeInternal(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    outputNames,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
        } else {
            createActualEdge(
                    upStreamVertexID,
                    downStreamVertexID,
                    typeNumber,
                    partitioner,
                    outputTag,
                    exchangeMode,
                    intermediateDataSetId);
        }
    }

    private void createActualEdge(
            Integer upStreamVertexID,
            Integer downStreamVertexID,
            int typeNumber,
            StreamPartitioner<?> partitioner,
            OutputTag outputTag,
            StreamExchangeMode exchangeMode,
            IntermediateDataSetID intermediateDataSetId) {
        StreamNode upstreamNode = getStreamNode(upStreamVertexID);
        StreamNode downstreamNode = getStreamNode(downStreamVertexID);

        // If no partitioner was specified and the parallelism of upstream and downstream
        // operator matches use forward partitioning, use rebalance otherwise.
        if (partitioner == null
                && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
            partitioner =
                    dynamic ? new ForwardForUnspecifiedPartitioner<>() : new ForwardPartitioner<>();
        } else if (partitioner == null) {
            partitioner = new RebalancePartitioner<Object>();
        }

        if (partitioner instanceof ForwardPartitioner) {
            if (upstreamNode.getParallelism() != downstreamNode.getParallelism()) {
                if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                    partitioner =
                            ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                    .getHashPartitioner();
                } else {
                    throw new UnsupportedOperationException(
                            "Forward partitioning does not allow "
                                    + "change of parallelism. Upstream operation: "
                                    + upstreamNode
                                    + " parallelism: "
                                    + upstreamNode.getParallelism()
                                    + ", downstream operation: "
                                    + downstreamNode
                                    + " parallelism: "
                                    + downstreamNode.getParallelism()
                                    + " You must use another partitioning strategy, such as broadcast, rebalance, shuffle or global.");
                }
            }
        }

        if (exchangeMode == null) {
            exchangeMode = StreamExchangeMode.UNDEFINED;
        }

        /**
         * Just make sure that {@link StreamEdge} connecting same nodes (for example as a result of
         * self unioning a {@link DataStream}) are distinct and unique. Otherwise it would be
         * difficult on the {@link StreamTask} to assign {@link RecordWriter}s to correct {@link
         * StreamEdge}.
         */
        int uniqueId = getStreamEdges(upstreamNode.getId(), downstreamNode.getId()).size();

        StreamEdge edge =
                new StreamEdge(
                        upstreamNode,
                        downstreamNode,
                        typeNumber,
                        partitioner,
                        outputTag,
                        exchangeMode,
                        uniqueId,
                        intermediateDataSetId);

        getStreamNode(edge.getSourceId()).addOutEdge(edge);
        getStreamNode(edge.getTargetId()).addInEdge(edge);
    }

    public void setParallelism(Integer vertexID, int parallelism) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setParallelism(parallelism);
        }
    }

    @Override
    public boolean isDynamic() {
        return dynamic;
    }

    @Override
    public JobCheckpointingSettings getCheckpointingSettings() {
        if (checkpointingSettings == null) {
            createJobCheckpointingSettings();
        }

        return checkpointingSettings;
    }

    @Override
    public boolean isEmpty() {
        return streamNodes == null ? isEmpty : streamNodes.isEmpty();
    }

    public void setParallelism(Integer vertexId, int parallelism, boolean parallelismConfigured) {
        if (getStreamNode(vertexId) != null) {
            getStreamNode(vertexId).setParallelism(parallelism, parallelismConfigured);
        }
    }

    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

    public void setMaxParallelism(int vertexID, int maxParallelism) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setMaxParallelism(maxParallelism);
        }
    }

    public void setResources(
            int vertexID, ResourceSpec minResources, ResourceSpec preferredResources) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setResources(minResources, preferredResources);
            streamNodeMinResources.put(vertexID, minResources);
        }
    }

    public void setManagedMemoryUseCaseWeights(
            int vertexID,
            Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            Set<ManagedMemoryUseCase> slotScopeUseCases) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID)
                    .setManagedMemoryUseCaseWeights(operatorScopeUseCaseWeights, slotScopeUseCases);
        }
    }

    public void setOneInputStateKey(
            Integer vertexID, KeySelector<?, ?> keySelector, TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelector);
        node.setStateKeySerializer(keySerializer);
    }

    public void setTwoInputStateKey(
            Integer vertexID,
            KeySelector<?, ?> keySelector1,
            KeySelector<?, ?> keySelector2,
            TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelector1, keySelector2);
        node.setStateKeySerializer(keySerializer);
    }

    public void setMultipleInputStateKey(
            Integer vertexID,
            List<KeySelector<?, ?>> keySelectors,
            TypeSerializer<?> keySerializer) {
        StreamNode node = getStreamNode(vertexID);
        node.setStatePartitioners(keySelectors.stream().toArray(KeySelector[]::new));
        node.setStateKeySerializer(keySerializer);
    }

    public void setBufferTimeout(Integer vertexID, long bufferTimeout) {
        if (getStreamNode(vertexID) != null) {
            getStreamNode(vertexID).setBufferTimeout(bufferTimeout);
        }
    }

    public void setSerializers(
            Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
        StreamNode vertex = getStreamNode(vertexID);
        vertex.setSerializersIn(in1, in2);
        vertex.setSerializerOut(out);
    }

    private <OUT> void setSerializers(
            Integer vertexID, List<TypeInformation<?>> inTypeInfos, TypeSerializer<OUT> out) {

        StreamNode vertex = getStreamNode(vertexID);

        vertex.setSerializersIn(
                inTypeInfos.stream()
                        .map(
                                typeInfo ->
                                        typeInfo.createSerializer(
                                                executionConfig.getSerializerConfig()))
                        .toArray(TypeSerializer[]::new));
        vertex.setSerializerOut(out);
    }

    public void setInputFormat(Integer vertexID, InputFormat<?, ?> inputFormat) {
        getStreamNode(vertexID).setInputFormat(inputFormat);
    }

    public void setOutputFormat(Integer vertexID, OutputFormat<?> outputFormat) {
        getStreamNode(vertexID).setOutputFormat(outputFormat);
    }

    public void setTransformationUID(Integer nodeId, String transformationId) {
        StreamNode node = streamNodes.get(nodeId);
        if (node != null) {
            node.setTransformationUID(transformationId);
        }
    }

    void setTransformationUserHash(Integer nodeId, String nodeHash) {
        StreamNode node = streamNodes.get(nodeId);
        if (node != null) {
            node.setUserHash(nodeHash);
        }
    }

    public StreamNode getStreamNode(Integer vertexID) {
        return streamNodes.get(vertexID);
    }

    protected Collection<? extends Integer> getVertexIDs() {
        return streamNodes.keySet();
    }

    @VisibleForTesting
    public List<StreamEdge> getStreamEdges(int sourceId) {
        return getStreamNode(sourceId).getOutEdges();
    }

    @VisibleForTesting
    public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {
        List<StreamEdge> result = new ArrayList<>();
        for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
            if (edge.getTargetId() == targetId) {
                result.add(edge);
            }
        }
        return result;
    }

    @VisibleForTesting
    @Deprecated
    public List<StreamEdge> getStreamEdgesOrThrow(int sourceId, int targetId) {
        List<StreamEdge> result = getStreamEdges(sourceId, targetId);
        if (result.isEmpty()) {
            throw new RuntimeException(
                    "No such edge in stream graph: " + sourceId + " -> " + targetId);
        }
        return result;
    }

    public Collection<Integer> getSourceIDs() {
        return sources;
    }

    public Collection<Integer> getSinkIDs() {
        return sinks;
    }

    public Collection<StreamNode> getStreamNodes() {
        return streamNodes.values();
    }

    public String getBrokerID(Integer vertexID) {
        return vertexIDtoBrokerID.get(vertexID);
    }

    public long getLoopTimeout(Integer vertexID) {
        return vertexIDtoLoopTimeout.get(vertexID);
    }

    public StreamNode getSourceVertex(StreamEdge edge) {
        return streamNodes.get(edge.getSourceId());
    }

    public StreamNode getTargetVertex(StreamEdge edge) {
        return streamNodes.get(edge.getTargetId());
    }

    /** Gets the assembled {@link JobGraph} with a random {@link JobID}. */
    @VisibleForTesting
    public JobGraph getJobGraph() {
        return getJobGraph(Thread.currentThread().getContextClassLoader(), jobId);
    }

    public JobGraph getJobGraph(ClassLoader userClassLoader) {
        return getJobGraph(userClassLoader, jobId);
    }

    /** Gets the assembled {@link JobGraph} with a specified {@link JobID}. */
    public JobGraph getJobGraph(ClassLoader userClassLoader, @Nullable JobID jobID) {
        return StreamingJobGraphGenerator.createJobGraph(userClassLoader, this, jobID);
    }

    public String getStreamingPlanAsJSON() {
        try {
            return new JSONGenerator(this).getJSON();
        } catch (Exception e) {
            throw new RuntimeException("JSON plan creation failed", e);
        }
    }

    private <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInfo) {
        return typeInfo != null && !(typeInfo instanceof MissingTypeInfo)
                ? typeInfo.createSerializer(executionConfig.getSerializerConfig())
                : null;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    @Override
    public String getName() {
        return jobName;
    }

    @Override
    public JobType getJobType() {
        return jobType;
    }

    public boolean isAutoParallelismEnabled() {
        return autoParallelismEnabled;
    }

    public void setAutoParallelismEnabled(boolean autoParallelismEnabled) {
        this.autoParallelismEnabled = autoParallelismEnabled;
    }

    public PipelineOptions.VertexDescriptionMode getVertexDescriptionMode() {
        return descriptionMode;
    }

    public void setVertexDescriptionMode(PipelineOptions.VertexDescriptionMode mode) {
        this.descriptionMode = mode;
    }

    public void setVertexNameIncludeIndexPrefix(boolean includePrefix) {
        this.vertexNameIncludeIndexPrefix = includePrefix;
    }

    public boolean isVertexNameIncludeIndexPrefix() {
        return this.vertexNameIncludeIndexPrefix;
    }

    /** Registers the JobStatusHook. */
    public void registerJobStatusHook(JobStatusHook hook) {
        checkNotNull(hook, "Registering a null JobStatusHook is not allowed. ");
        if (!jobStatusHooks.contains(hook)) {
            this.jobStatusHooks.add(hook);
        }
    }

    public List<JobStatusHook> getJobStatusHooks() {
        return this.jobStatusHooks;
    }

    public void setSupportsConcurrentExecutionAttempts(
            Integer vertexId, boolean supportsConcurrentExecutionAttempts) {
        final StreamNode streamNode = getStreamNode(vertexId);
        if (streamNode != null) {
            streamNode.setSupportsConcurrentExecutionAttempts(supportsConcurrentExecutionAttempts);
        }
    }

    public void setAttribute(Integer vertexId, Attribute attribute) {
        if (getStreamNode(vertexId) != null) {
            getStreamNode(vertexId).setAttribute(attribute);
        }
    }

    public void setJobId(JobID jobId) {
        this.jobId = jobId;
    }

    @Override
    public JobID getJobID() {
        return jobId;
    }

    /**
     * Sets the classpath required to run the job on a task manager.
     *
     * @param paths paths of the directories/JAR files required to run the job on a task manager
     */
    public void setClasspath(List<URL> paths) {
        classpath = paths;
    }

    public List<URL> getClasspath() {
        return classpath;
    }

    /**
     * Adds the given jar files to the {@link JobGraph} via {@link JobGraph#addJar}.
     *
     * @param jarFilesToAttach a list of the {@link URL URLs} of the jar files to attach to the
     *     jobgraph.
     * @throws RuntimeException if a jar URL is not valid.
     */
    public void addJars(final List<URL> jarFilesToAttach) {
        for (URL jar : jarFilesToAttach) {
            try {
                addJar(new Path(jar.toURI()));
            } catch (URISyntaxException e) {
                throw new RuntimeException("URL is invalid. This should not happen.", e);
            }
        }
    }

    /**
     * Returns a list of BLOB keys referring to the JAR files required to run this job.
     *
     * @return list of BLOB keys referring to the JAR files required to run this job
     */
    @Override
    public List<PermanentBlobKey> getUserJarBlobKeys() {
        return this.userJarBlobKeys;
    }

    @Override
    public List<URL> getClasspaths() {
        return classpath;
    }

    public void addUserArtifact(String name, DistributedCache.DistributedCacheEntry file) {
        if (file == null) {
            throw new IllegalArgumentException();
        }

        userArtifacts.putIfAbsent(name, file);
    }

    @Override
    public Map<String, DistributedCache.DistributedCacheEntry> getUserArtifacts() {
        return userArtifacts;
    }

    @Override
    public void addUserJarBlobKey(PermanentBlobKey key) {
        if (key == null) {
            throw new IllegalArgumentException();
        }

        if (!userJarBlobKeys.contains(key)) {
            userJarBlobKeys.add(key);
        }
    }

    @Override
    public void setUserArtifactBlobKey(String entryName, PermanentBlobKey blobKey)
            throws IOException {
        byte[] serializedBlobKey;
        serializedBlobKey = InstantiationUtil.serializeObject(blobKey);

        userArtifacts.computeIfPresent(
                entryName,
                (key, originalEntry) ->
                        new DistributedCache.DistributedCacheEntry(
                                originalEntry.filePath,
                                originalEntry.isExecutable,
                                serializedBlobKey,
                                originalEntry.isZipped));
    }

    @Override
    public void writeUserArtifactEntriesToConfiguration() {
        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> userArtifact :
                userArtifacts.entrySet()) {
            DistributedCache.writeFileInfoToConfig(
                    userArtifact.getKey(), userArtifact.getValue(), jobConfiguration);
        }
    }

    @Override
    public int getMaximumParallelism() {
        int maxParallelism = -1;
        for (StreamNode node : streamNodes.values()) {
            maxParallelism = Math.max(node.getParallelism(), maxParallelism);
        }
        return maxParallelism;
    }

    public void setInitialClientHeartbeatTimeout(long initialClientHeartbeatTimeout) {
        this.initialClientHeartbeatTimeout = initialClientHeartbeatTimeout;
    }

    @Override
    public long getInitialClientHeartbeatTimeout() {
        return initialClientHeartbeatTimeout;
    }

    @Override
    public boolean isPartialResourceConfigured() {
        boolean hasVerticesWithUnknownResource = false;
        boolean hasVerticesWithConfiguredResource = false;

        for (ResourceSpec minResource : streamNodeMinResources.values()) {
            if (minResource == ResourceSpec.UNKNOWN) {
                hasVerticesWithUnknownResource = true;
            } else {
                hasVerticesWithConfiguredResource = true;
            }

            if (hasVerticesWithUnknownResource && hasVerticesWithConfiguredResource) {
                return true;
            }
        }

        return false;
    }

    public void serializeUserDefinedInstances() throws IOException {
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        try {
            this.userDefinedObjectsHolder =
                    new UserDefinedObjectsHolder(
                            streamNodes,
                            virtualSideOutputNodes,
                            virtualPartitionNodes,
                            executionConfig,
                            serializationExecutor);
        } finally {
            serializationExecutor.shutdown();
        }
    }

    public void deserializeUserDefinedInstances(
            ClassLoader userClassLoader, Executor serializationExecutor) throws Exception {
        this.userDefinedObjectsHolder.deserialize(userClassLoader, serializationExecutor);
    }

    @Override
    public String toString() {
        return "StreamGraph(jobId: " + jobId + ")";
    }

    /**
     * A static inner class designed to hold user-defined objects for serialization and
     * deserialization in the stream graph.
     */
    private class UserDefinedObjectsHolder implements Serializable {

        private static final long serialVersionUID = 1L;

        private final SerializedValue<
                        Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>>
                serializedVirtualPartitionNodes;

        private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

        private SerializedValue<Map<Integer, StreamNode>> serializedStreamNodes;

        /**
         * This collection stores operator factories serialized separately from the {@link
         * StreamGraph}. This separation allows for the parallel serialization of operator
         * factories, improving the overall performance of the serialization process.
         *
         * <p>Each tuple in this collection consists of an integer key that identifies the stream
         * node, and a value that wraps the serialized representation of the associated {@link
         * StreamOperatorFactory} instance.
         */
        private Collection<Tuple2<Integer, SerializedValue<StreamOperatorFactory<?>>>>
                streamNodeToSerializedOperatorFactories;

        private final SerializedValue<Map<Integer, Tuple2<Integer, OutputTag>>>
                serializedVirtualSideOutputNodes;

        public UserDefinedObjectsHolder(
                Map<Integer, StreamNode> streamNodes,
                Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes,
                Map<Integer, Tuple3<Integer, StreamPartitioner<?>, StreamExchangeMode>>
                        virtualPartitionNodes,
                ExecutionConfig executionConfig,
                Executor serializationExecutor)
                throws IOException {
            serializeStreamNodes(streamNodes, serializationExecutor);

            this.serializedVirtualSideOutputNodes = new SerializedValue<>(virtualSideOutputNodes);
            this.serializedVirtualPartitionNodes = new SerializedValue<>(virtualPartitionNodes);
            this.serializedExecutionConfig = new SerializedValue<>(executionConfig);
        }

        private void serializeStreamNodes(
                Map<Integer, StreamNode> toBeSerializedStreamNodes,
                Executor serializationExecutor) {
            try {
                this.streamNodeToSerializedOperatorFactories =
                        serializeOperatorFactories(
                                toBeSerializedStreamNodes.values(), serializationExecutor);
                this.serializedStreamNodes = new SerializedValue<>(toBeSerializedStreamNodes);
            } catch (Exception e) {
                throw new RuntimeException("Could not serialize stream nodes", e);
            }
        }

        private Collection<Tuple2<Integer, SerializedValue<StreamOperatorFactory<?>>>>
                serializeOperatorFactories(
                        Collection<StreamNode> streamNodes, Executor serializationExecutor)
                        throws Exception {
            List<CompletableFuture<Tuple2<Integer, SerializedValue<StreamOperatorFactory<?>>>>>
                    futures =
                            streamNodes.stream()
                                    .filter(node -> node.getOperatorFactory() != null)
                                    .map(
                                            node ->
                                                    serializeOperatorFactoriesAsync(
                                                            serializationExecutor, node))
                                    .collect(Collectors.toList());
            return FutureUtils.combineAll(futures).get();
        }

        private CompletableFuture<Tuple2<Integer, SerializedValue<StreamOperatorFactory<?>>>>
                serializeOperatorFactoriesAsync(Executor serializationExecutor, StreamNode node) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            return Tuple2.of(
                                    node.getId(), new SerializedValue<>(node.getOperatorFactory()));
                        } catch (Throwable throwable) {
                            throw new RuntimeException(
                                    String.format("Could not serialize stream node %s", node),
                                    throwable);
                        }
                    },
                    serializationExecutor);
        }

        private void deserialize(ClassLoader userClassLoader, Executor serializationExecutor)
                throws Exception {
            Collection<Tuple2<Integer, StreamOperatorFactory<?>>> streamNodeToOperatorFactories =
                    deserializeOperators(userClassLoader, serializationExecutor);

            virtualSideOutputNodes =
                    serializedVirtualSideOutputNodes.deserializeValue(userClassLoader);
            virtualPartitionNodes =
                    serializedVirtualPartitionNodes.deserializeValue(userClassLoader);
            executionConfig = serializedExecutionConfig.deserializeValue(userClassLoader);
            streamNodes = serializedStreamNodes.deserializeValue(userClassLoader);
            streamNodeToOperatorFactories.forEach(
                    tuple2 -> getStreamNode(tuple2.f0).setOperatorFactory(tuple2.f1));
        }

        private Collection<Tuple2<Integer, StreamOperatorFactory<?>>> deserializeOperators(
                ClassLoader userClassLoader, Executor serializationExecutor) throws Exception {
            List<CompletableFuture<Tuple2<Integer, StreamOperatorFactory<?>>>> futures =
                    streamNodeToSerializedOperatorFactories.stream()
                            .map(
                                    tuple2 ->
                                            deserializeOperatorFactoriesAsync(
                                                    userClassLoader, serializationExecutor, tuple2))
                            .collect(Collectors.toList());
            return FutureUtils.combineAll(futures).get();
        }

        private CompletableFuture<Tuple2<Integer, StreamOperatorFactory<?>>>
                deserializeOperatorFactoriesAsync(
                        ClassLoader userClassLoader,
                        Executor serializationExecutor,
                        Tuple2<Integer, SerializedValue<StreamOperatorFactory<?>>> tuple2) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        try {
                            StreamOperatorFactory<?> streamOperatorFactory =
                                    tuple2.f1.deserializeValue(userClassLoader);
                            return Tuple2.of(tuple2.f0, streamOperatorFactory);
                        } catch (Throwable throwable) {
                            throw new RuntimeException(
                                    String.format(
                                            "Could not deserialize stream node %s", tuple2.f0),
                                    throwable);
                        }
                    },
                    serializationExecutor);
        }
    }
}

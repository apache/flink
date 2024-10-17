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
import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.ClassLoaderUtil;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.FutureUtils;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Internal configuration for a {@link StreamOperator}. This is created and populated by the {@link
 * StreamingJobGraphGenerator}.
 */
@Internal
public class StreamConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    // ------------------------------------------------------------------------
    //  Config Keys
    // ------------------------------------------------------------------------

    public static final String SERIALIZED_UDF = "serializedUDF";
    /**
     * Introduce serializedUdfClassName to avoid unnecessarily heavy {@link
     * #getStreamOperatorFactory}.
     */
    public static final String SERIALIZED_UDF_CLASS = "serializedUdfClass";

    private static final ConfigOption<Integer> NUMBER_OF_OUTPUTS =
            ConfigOptions.key("numberOfOutputs").intType().defaultValue(0);
    private static final ConfigOption<Integer> NUMBER_OF_NETWORK_INPUTS =
            ConfigOptions.key("numberOfNetworkInputs").intType().defaultValue(0);
    private static final String CHAINED_OUTPUTS = "chainedOutputs";
    private static final String CHAINED_TASK_CONFIG = "chainedTaskConfig_";
    private static final ConfigOption<Boolean> IS_CHAINED_VERTEX =
            ConfigOptions.key("isChainedSubtask").booleanType().defaultValue(false);
    private static final ConfigOption<Integer> CHAIN_INDEX =
            ConfigOptions.key("chainIndex").intType().defaultValue(0);
    private static final ConfigOption<Integer> VERTEX_NAME =
            ConfigOptions.key("vertexID").intType().defaultValue(-1);
    private static final String ITERATION_ID = "iterationId";
    private static final String INPUTS = "inputs";
    private static final String TYPE_SERIALIZER_OUT_1 = "typeSerializer_out";
    private static final String TYPE_SERIALIZER_SIDEOUT_PREFIX = "typeSerializer_sideout_";
    private static final ConfigOption<Long> ITERATON_WAIT =
            ConfigOptions.key("iterationWait").longType().defaultValue(0L);
    private static final String OP_NONCHAINED_OUTPUTS = "opNonChainedOutputs";
    private static final String VERTEX_NONCHAINED_OUTPUTS = "vertexNonChainedOutputs";
    private static final String IN_STREAM_EDGES = "inStreamEdges";
    private static final String OPERATOR_NAME = "operatorName";
    private static final String OPERATOR_ID = "operatorID";
    private static final ConfigOption<Boolean> CHAIN_END =
            ConfigOptions.key("chainEnd").booleanType().defaultValue(false);

    private static final ConfigOption<Boolean> GRAPH_CONTAINING_LOOPS =
            ConfigOptions.key("graphContainingLoops").booleanType().defaultValue(false);

    private static final ConfigOption<Boolean> CHECKPOINTING_ENABLED =
            ConfigOptions.key("checkpointing").booleanType().defaultValue(false);

    private static final ConfigOption<Integer> CHECKPOINT_MODE =
            ConfigOptions.key("checkpointMode").intType().defaultValue(-1);

    private static final String SAVEPOINT_DIR = "savepointdir";
    private static final String CHECKPOINT_STORAGE = "checkpointstorage";
    private static final String STATE_BACKEND = "statebackend";
    private static final String ENABLE_CHANGE_LOG_STATE_BACKEND = "enablechangelog";
    private static final String TIMER_SERVICE_PROVIDER = "timerservice";
    private static final String STATE_PARTITIONER = "statePartitioner";

    private static final String STATE_KEY_SERIALIZER = "statekeyser";

    private static final String MANAGED_MEMORY_FRACTION_PREFIX = "managedMemFraction.";

    private static final String ATTRIBUTE = "attribute";

    private static final ConfigOption<Boolean> STATE_BACKEND_USE_MANAGED_MEMORY =
            ConfigOptions.key("statebackend.useManagedMemory")
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(
                            "If state backend is specified, whether it uses managed memory.");

    // ------------------------------------------------------------------------
    //  Default Values
    // ------------------------------------------------------------------------

    private static final CheckpointingMode DEFAULT_CHECKPOINTING_MODE =
            CheckpointingMode.EXACTLY_ONCE;

    private static final double DEFAULT_MANAGED_MEMORY_FRACTION = 0.0;

    // ------------------------------------------------------------------------
    //  Config
    // ------------------------------------------------------------------------

    private final Configuration config;

    // To make the parallelization of the StreamConfig serialization easier, we use this map
    // to collect all the need-to-be-serialized objects. These objects will be serialized all at
    // once then.
    private final transient Map<String, Object> toBeSerializedConfigObjects = new HashMap<>();
    private final transient Map<Integer, CompletableFuture<StreamConfig>> chainedTaskFutures =
            new HashMap<>();
    private final transient CompletableFuture<StreamConfig> serializationFuture =
            new CompletableFuture<>();

    /**
     * In order to release memory during processing data, some keys are removed in {@link
     * #clearInitialConfigs()}. Recording these keys here to prevent they are accessed after
     * removing.
     */
    private final Set<String> removedKeys = new HashSet<>();

    public StreamConfig(Configuration config) {
        this.config = config;
    }

    public Configuration getConfiguration() {
        return config;
    }

    public CompletableFuture<StreamConfig> getSerializationFuture() {
        return serializationFuture;
    }

    /** Trigger the object config serialization and return the completable future. */
    public CompletableFuture<StreamConfig> triggerSerializationAndReturnFuture(
            Executor ioExecutor) {
        FutureUtils.combineAll(chainedTaskFutures.values())
                .thenAcceptAsync(
                        chainedConfigs -> {
                            try {
                                // Serialize all the objects to config.
                                serializeAllConfigs();
                                InstantiationUtil.writeObjectToConfig(
                                        chainedConfigs.stream()
                                                .collect(
                                                        Collectors.toMap(
                                                                StreamConfig::getVertexID,
                                                                Function.identity())),
                                        this.config,
                                        CHAINED_TASK_CONFIG);
                                serializationFuture.complete(this);
                            } catch (Throwable throwable) {
                                serializationFuture.completeExceptionally(throwable);
                            }
                        },
                        ioExecutor);
        return serializationFuture;
    }

    /**
     * Serialize all object configs synchronously. Only used for operators which need to reconstruct
     * the StreamConfig internally or test.
     */
    public void serializeAllConfigs() {
        toBeSerializedConfigObjects.forEach(
                (key, object) -> {
                    try {
                        InstantiationUtil.writeObjectToConfig(object, this.config, key);
                    } catch (IOException e) {
                        throw new StreamTaskException(
                                String.format("Could not serialize object for key %s.", key), e);
                    }
                });
    }

    @VisibleForTesting
    public void setAndSerializeTransitiveChainedTaskConfigs(
            Map<Integer, StreamConfig> chainedTaskConfigs) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    chainedTaskConfigs, this.config, CHAINED_TASK_CONFIG);
        } catch (IOException e) {
            throw new StreamTaskException(
                    "Could not serialize object for key chained task config.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Configured Properties
    // ------------------------------------------------------------------------

    public void setVertexID(Integer vertexID) {
        config.set(VERTEX_NAME, vertexID);
    }

    public Integer getVertexID() {
        return config.get(VERTEX_NAME);
    }

    /** Fraction of managed memory reserved for the given use case that this operator should use. */
    public void setManagedMemoryFractionOperatorOfUseCase(
            ManagedMemoryUseCase managedMemoryUseCase, double fraction) {
        final ConfigOption<Double> configOption =
                getManagedMemoryFractionConfigOption(managedMemoryUseCase);

        checkArgument(
                fraction >= 0.0 && fraction <= 1.0,
                String.format(
                        "%s should be in range [0.0, 1.0], but was: %s",
                        configOption.key(), fraction));

        config.set(configOption, fraction);
    }

    /**
     * Fraction of total managed memory in the slot that this operator should use for the given use
     * case.
     */
    public double getManagedMemoryFractionOperatorUseCaseOfSlot(
            ManagedMemoryUseCase managedMemoryUseCase,
            Configuration jobConfig,
            Configuration taskManagerConfig,
            ClassLoader cl) {
        return ManagedMemoryUtils.convertToFractionOfSlot(
                managedMemoryUseCase,
                config.get(getManagedMemoryFractionConfigOption(managedMemoryUseCase)),
                getAllManagedMemoryUseCases(),
                jobConfig,
                taskManagerConfig,
                config.getOptional(STATE_BACKEND_USE_MANAGED_MEMORY),
                cl);
    }

    private static ConfigOption<Double> getManagedMemoryFractionConfigOption(
            ManagedMemoryUseCase managedMemoryUseCase) {
        return ConfigOptions.key(
                        MANAGED_MEMORY_FRACTION_PREFIX + checkNotNull(managedMemoryUseCase))
                .doubleType()
                .defaultValue(DEFAULT_MANAGED_MEMORY_FRACTION);
    }

    private Set<ManagedMemoryUseCase> getAllManagedMemoryUseCases() {
        return config.keySet().stream()
                .filter((key) -> key.startsWith(MANAGED_MEMORY_FRACTION_PREFIX))
                .map(
                        (key) ->
                                ManagedMemoryUseCase.valueOf(
                                        key.replaceFirst(MANAGED_MEMORY_FRACTION_PREFIX, "")))
                .collect(Collectors.toSet());
    }

    public void setTypeSerializerOut(TypeSerializer<?> serializer) {
        setTypeSerializer(TYPE_SERIALIZER_OUT_1, serializer);
    }

    public <T> TypeSerializer<T> getTypeSerializerOut(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, TYPE_SERIALIZER_OUT_1, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate serializer.", e);
        }
    }

    public void setTypeSerializerSideOut(OutputTag<?> outputTag, TypeSerializer<?> serializer) {
        setTypeSerializer(TYPE_SERIALIZER_SIDEOUT_PREFIX + outputTag.getId(), serializer);
    }

    private void setTypeSerializer(String key, TypeSerializer<?> typeWrapper) {
        toBeSerializedConfigObjects.put(key, typeWrapper);
    }

    public <T> TypeSerializer<T> getTypeSerializerSideOut(OutputTag<?> outputTag, ClassLoader cl) {
        checkNotNull(outputTag, "Side output id must not be null.");
        try {
            return InstantiationUtil.readObjectFromConfig(
                    this.config, TYPE_SERIALIZER_SIDEOUT_PREFIX + outputTag.getId(), cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate serializer.", e);
        }
    }

    public void setupNetworkInputs(TypeSerializer<?>... serializers) {
        InputConfig[] inputs = new InputConfig[serializers.length];
        for (int i = 0; i < serializers.length; i++) {
            inputs[i] = new NetworkInputConfig(serializers[i], i, InputRequirement.PASS_THROUGH);
        }
        setInputs(inputs);
    }

    public void setInputs(InputConfig... inputs) {
        toBeSerializedConfigObjects.put(INPUTS, inputs);
    }

    public InputConfig[] getInputs(ClassLoader cl) {
        try {
            InputConfig[] inputs = InstantiationUtil.readObjectFromConfig(this.config, INPUTS, cl);
            if (inputs == null) {
                return new InputConfig[0];
            }
            return inputs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not deserialize inputs", e);
        }
    }

    @Deprecated
    public <T> TypeSerializer<T> getTypeSerializerIn1(ClassLoader cl) {
        return getTypeSerializerIn(0, cl);
    }

    @Deprecated
    public <T> TypeSerializer<T> getTypeSerializerIn2(ClassLoader cl) {
        return getTypeSerializerIn(1, cl);
    }

    public <T> TypeSerializer<T> getTypeSerializerIn(int index, ClassLoader cl) {
        InputConfig[] inputs = getInputs(cl);
        checkState(index < inputs.length);
        checkState(
                inputs[index] instanceof NetworkInputConfig,
                "Input [%s] was assumed to be network input",
                index);
        return (TypeSerializer<T>) ((NetworkInputConfig) inputs[index]).typeSerializer;
    }

    @VisibleForTesting
    public void setStreamOperator(StreamOperator<?> operator) {
        setStreamOperatorFactory(SimpleOperatorFactory.of(operator));
    }

    public void setStreamOperatorFactory(StreamOperatorFactory<?> factory) {
        if (factory != null) {
            toBeSerializedConfigObjects.put(SERIALIZED_UDF, factory);
            toBeSerializedConfigObjects.put(SERIALIZED_UDF_CLASS, factory.getClass());
        }
    }

    @VisibleForTesting
    public <T extends StreamOperator<?>> T getStreamOperator(ClassLoader cl) {
        SimpleOperatorFactory<?> factory = getStreamOperatorFactory(cl);
        return (T) factory.getOperator();
    }

    public <T extends StreamOperatorFactory<?>> T getStreamOperatorFactory(ClassLoader cl) {
        try {
            checkState(
                    !removedKeys.contains(SERIALIZED_UDF),
                    String.format("%s has been removed.", SERIALIZED_UDF));
            return InstantiationUtil.readObjectFromConfig(this.config, SERIALIZED_UDF, cl);
        } catch (ClassNotFoundException e) {
            String classLoaderInfo = ClassLoaderUtil.getUserCodeClassLoaderInfo(cl);
            boolean loadableDoubleCheck = ClassLoaderUtil.validateClassLoadable(e, cl);

            String exceptionMessage =
                    "Cannot load user class: "
                            + e.getMessage()
                            + "\nClassLoader info: "
                            + classLoaderInfo
                            + (loadableDoubleCheck
                                    ? "\nClass was actually found in classloader - deserialization issue."
                                    : "\nClass not resolvable through given classloader.");

            throw new StreamTaskException(exceptionMessage, e);
        } catch (Exception e) {
            throw new StreamTaskException("Cannot instantiate user function.", e);
        }
    }

    public <T extends StreamOperatorFactory<?>> Class<T> getStreamOperatorFactoryClass(
            ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, SERIALIZED_UDF_CLASS, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate serialized udf class.", e);
        }
    }

    public void setIterationId(String iterationId) {
        config.setString(ITERATION_ID, iterationId);
    }

    public String getIterationId() {
        return config.getString(ITERATION_ID, "");
    }

    public void setIterationWaitTime(long time) {
        config.set(ITERATON_WAIT, time);
    }

    public long getIterationWaitTime() {
        return config.get(ITERATON_WAIT);
    }

    public void setNumberOfNetworkInputs(int numberOfInputs) {
        config.set(NUMBER_OF_NETWORK_INPUTS, numberOfInputs);
    }

    public int getNumberOfNetworkInputs() {
        return config.get(NUMBER_OF_NETWORK_INPUTS);
    }

    public void setNumberOfOutputs(int numberOfOutputs) {
        config.set(NUMBER_OF_OUTPUTS, numberOfOutputs);
    }

    public int getNumberOfOutputs() {
        return config.get(NUMBER_OF_OUTPUTS);
    }

    /** Sets the operator level non-chained outputs. */
    public void setOperatorNonChainedOutputs(List<NonChainedOutput> nonChainedOutputs) {
        toBeSerializedConfigObjects.put(OP_NONCHAINED_OUTPUTS, nonChainedOutputs);
    }

    public List<NonChainedOutput> getOperatorNonChainedOutputs(ClassLoader cl) {
        try {
            List<NonChainedOutput> nonChainedOutputs =
                    InstantiationUtil.readObjectFromConfig(this.config, OP_NONCHAINED_OUTPUTS, cl);
            return nonChainedOutputs == null ? new ArrayList<>() : nonChainedOutputs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate non chained outputs.", e);
        }
    }

    public void setChainedOutputs(List<StreamEdge> chainedOutputs) {
        toBeSerializedConfigObjects.put(CHAINED_OUTPUTS, chainedOutputs);
    }

    public List<StreamEdge> getChainedOutputs(ClassLoader cl) {
        try {
            List<StreamEdge> chainedOutputs =
                    InstantiationUtil.readObjectFromConfig(this.config, CHAINED_OUTPUTS, cl);
            return chainedOutputs == null ? new ArrayList<StreamEdge>() : chainedOutputs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate chained outputs.", e);
        }
    }

    public void setInPhysicalEdges(List<StreamEdge> inEdges) {
        toBeSerializedConfigObjects.put(IN_STREAM_EDGES, inEdges);
    }

    public List<StreamEdge> getInPhysicalEdges(ClassLoader cl) {
        try {
            List<StreamEdge> inEdges =
                    InstantiationUtil.readObjectFromConfig(this.config, IN_STREAM_EDGES, cl);
            return inEdges == null ? new ArrayList<StreamEdge>() : inEdges;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate inputs.", e);
        }
    }

    // --------------------- checkpointing -----------------------

    public void setCheckpointingEnabled(boolean enabled) {
        config.set(CHECKPOINTING_ENABLED, enabled);
    }

    public boolean isCheckpointingEnabled() {
        return config.get(CHECKPOINTING_ENABLED);
    }

    public void setCheckpointMode(CheckpointingMode mode) {
        config.set(CHECKPOINT_MODE, mode.ordinal());
    }

    public CheckpointingMode getCheckpointMode() {
        int ordinal = config.get(CHECKPOINT_MODE, -1);
        if (ordinal >= 0) {
            return CheckpointingMode.values()[ordinal];
        } else {
            return DEFAULT_CHECKPOINTING_MODE;
        }
    }

    public void setUnalignedCheckpointsEnabled(boolean enabled) {
        config.set(CheckpointingOptions.ENABLE_UNALIGNED, enabled);
    }

    public boolean isUnalignedCheckpointsEnabled() {
        return config.get(CheckpointingOptions.ENABLE_UNALIGNED, false);
    }

    public void setUnalignedCheckpointsSplittableTimersEnabled(boolean enabled) {
        config.set(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS, enabled);
    }

    public boolean isUnalignedCheckpointsSplittableTimersEnabled() {
        return config.get(CheckpointingOptions.ENABLE_UNALIGNED_INTERRUPTIBLE_TIMERS);
    }

    public boolean isExactlyOnceCheckpointMode() {
        return getCheckpointMode() == CheckpointingMode.EXACTLY_ONCE;
    }

    public Duration getAlignedCheckpointTimeout() {
        return config.get(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT);
    }

    public void setAlignedCheckpointTimeout(Duration alignedCheckpointTimeout) {
        config.set(CheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, alignedCheckpointTimeout);
    }

    public void setMaxConcurrentCheckpoints(int maxConcurrentCheckpoints) {
        config.set(CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, maxConcurrentCheckpoints);
    }

    public int getMaxConcurrentCheckpoints() {
        return config.get(
                CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS,
                CheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS.defaultValue());
    }

    public int getMaxSubtasksPerChannelStateFile() {
        return config.get(CheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE);
    }

    public void setMaxSubtasksPerChannelStateFile(int maxSubtasksPerChannelStateFile) {
        config.set(
                CheckpointingOptions.UNALIGNED_MAX_SUBTASKS_PER_CHANNEL_STATE_FILE,
                maxSubtasksPerChannelStateFile);
    }

    /**
     * Sets the job vertex level non-chained outputs. The given output list must have the same order
     * with {@link JobVertex#getProducedDataSets()}.
     */
    public void setVertexNonChainedOutputs(List<NonChainedOutput> nonChainedOutputs) {
        toBeSerializedConfigObjects.put(VERTEX_NONCHAINED_OUTPUTS, nonChainedOutputs);
    }

    public List<NonChainedOutput> getVertexNonChainedOutputs(ClassLoader cl) {
        try {
            List<NonChainedOutput> nonChainedOutputs =
                    InstantiationUtil.readObjectFromConfig(
                            this.config, VERTEX_NONCHAINED_OUTPUTS, cl);
            return nonChainedOutputs == null ? new ArrayList<>() : nonChainedOutputs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate outputs in order.", e);
        }
    }

    public void setTransitiveChainedTaskConfigs(Map<Integer, StreamConfig> chainedTaskConfigs) {
        if (chainedTaskConfigs != null) {
            chainedTaskConfigs.forEach(
                    (id, config) -> chainedTaskFutures.put(id, config.getSerializationFuture()));
        }
    }

    public Map<Integer, StreamConfig> getTransitiveChainedTaskConfigs(ClassLoader cl) {
        try {
            checkState(
                    !removedKeys.contains(CHAINED_TASK_CONFIG),
                    String.format("%s has been removed.", CHAINED_TASK_CONFIG));
            Map<Integer, StreamConfig> confs =
                    InstantiationUtil.readObjectFromConfig(this.config, CHAINED_TASK_CONFIG, cl);
            return confs == null ? new HashMap<Integer, StreamConfig>() : confs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate configuration.", e);
        }
    }

    public Map<Integer, StreamConfig> getTransitiveChainedTaskConfigsWithSelf(ClassLoader cl) {
        // TODO: could this logic be moved to the user of #setTransitiveChainedTaskConfigs() ?
        Map<Integer, StreamConfig> chainedTaskConfigs = getTransitiveChainedTaskConfigs(cl);
        chainedTaskConfigs.put(getVertexID(), this);
        return chainedTaskConfigs;
    }

    public void setOperatorID(OperatorID operatorID) {
        this.config.setBytes(OPERATOR_ID, operatorID.getBytes());
    }

    public OperatorID getOperatorID() {
        byte[] operatorIDBytes = config.getBytes(OPERATOR_ID, null);
        return new OperatorID(checkNotNull(operatorIDBytes));
    }

    public void setOperatorName(String name) {
        this.config.setString(OPERATOR_NAME, name);
    }

    public String getOperatorName() {
        return this.config.getString(OPERATOR_NAME, null);
    }

    public void setChainIndex(int index) {
        this.config.set(CHAIN_INDEX, index);
    }

    public int getChainIndex() {
        return this.config.get(CHAIN_INDEX);
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    public void setStateBackend(StateBackend backend) {
        if (backend != null) {
            toBeSerializedConfigObjects.put(STATE_BACKEND, backend);
            setStateBackendUsesManagedMemory(backend.useManagedMemory());
        }
    }

    public void setChangelogStateBackendEnabled(TernaryBoolean enabled) {
        toBeSerializedConfigObjects.put(ENABLE_CHANGE_LOG_STATE_BACKEND, enabled);
    }

    @VisibleForTesting
    public void setStateBackendUsesManagedMemory(boolean usesManagedMemory) {
        this.config.set(STATE_BACKEND_USE_MANAGED_MEMORY, usesManagedMemory);
    }

    public void setSerializedStateBackend(
            SerializedValue<StateBackend> serializedStateBackend, boolean useManagedMemory) {
        if (serializedStateBackend != null) {
            this.config.setBytes(STATE_BACKEND, serializedStateBackend.getByteArray());
            setStateBackendUsesManagedMemory(useManagedMemory);
        }
    }

    public void setSerializedCheckpointStorage(
            SerializedValue<CheckpointStorage> serializedCheckpointStorage) {
        if (serializedCheckpointStorage != null) {
            this.config.setBytes(CHECKPOINT_STORAGE, serializedCheckpointStorage.getByteArray());
        }
    }

    public StateBackend getStateBackend(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, STATE_BACKEND, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate statehandle provider.", e);
        }
    }

    public TernaryBoolean isChangelogStateBackendEnabled(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(
                    this.config, ENABLE_CHANGE_LOG_STATE_BACKEND, cl);
        } catch (Exception e) {
            throw new StreamTaskException(
                    "Could not instantiate change log state backend enable flag.", e);
        }
    }

    @VisibleForTesting
    public void setCheckpointStorage(CheckpointStorage storage) {
        if (storage != null) {
            toBeSerializedConfigObjects.put(CHECKPOINT_STORAGE, storage);
        }
    }

    public CheckpointStorage getCheckpointStorage(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, CHECKPOINT_STORAGE, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate checkpoint storage.", e);
        }
    }

    public void setTimerServiceProvider(InternalTimeServiceManager.Provider timerServiceProvider) {
        if (timerServiceProvider != null) {
            toBeSerializedConfigObjects.put(TIMER_SERVICE_PROVIDER, timerServiceProvider);
        }
    }

    public InternalTimeServiceManager.Provider getTimerServiceProvider(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, TIMER_SERVICE_PROVIDER, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate timer service provider.", e);
        }
    }

    public void setStatePartitioner(int input, KeySelector<?, ?> partitioner) {
        toBeSerializedConfigObjects.put(STATE_PARTITIONER + input, partitioner);
    }

    public <IN, K extends Serializable> KeySelector<IN, K> getStatePartitioner(
            int input, ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(
                    this.config, STATE_PARTITIONER + input, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate state partitioner.", e);
        }
    }

    public void setStateKeySerializer(TypeSerializer<?> serializer) {
        toBeSerializedConfigObjects.put(STATE_KEY_SERIALIZER, serializer);
    }

    public <K> TypeSerializer<K> getStateKeySerializer(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, STATE_KEY_SERIALIZER, cl);
        } catch (Exception e) {
            throw new StreamTaskException(
                    "Could not instantiate state key serializer from task config.", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Miscellaneous
    // ------------------------------------------------------------------------

    public void setChainStart() {
        config.set(IS_CHAINED_VERTEX, true);
    }

    public boolean isChainStart() {
        return config.get(IS_CHAINED_VERTEX);
    }

    public void setChainEnd() {
        config.set(CHAIN_END, true);
    }

    public boolean isChainEnd() {
        return config.get(CHAIN_END);
    }

    @Override
    public String toString() {

        ClassLoader cl = getClass().getClassLoader();

        StringBuilder builder = new StringBuilder();
        builder.append("\n=======================");
        builder.append("Stream Config");
        builder.append("=======================");
        builder.append("\nNumber of non-chained inputs: ").append(getNumberOfNetworkInputs());
        builder.append("\nNumber of non-chained outputs: ").append(getNumberOfOutputs());
        builder.append("\nOutput names: ").append(getOperatorNonChainedOutputs(cl));
        builder.append("\nPartitioning:");
        for (NonChainedOutput output : getOperatorNonChainedOutputs(cl)) {
            String outputName = output.getDataSetId().toString();
            builder.append("\n\t").append(outputName).append(": ").append(output.getPartitioner());
        }

        builder.append("\nChained subtasks: ").append(getChainedOutputs(cl));

        try {
            builder.append("\nOperator: ")
                    .append(getStreamOperatorFactoryClass(cl).getSimpleName());
        } catch (Exception e) {
            builder.append("\nOperator: Missing");
        }
        builder.append("\nState Monitoring: ").append(isCheckpointingEnabled());
        if (isChainStart() && getChainedOutputs(cl).size() > 0) {
            builder.append(
                    "\n\n\n---------------------\nChained task configs\n---------------------\n");
            builder.append(getTransitiveChainedTaskConfigs(cl));
        }

        return builder.toString();
    }

    public void setGraphContainingLoops(boolean graphContainingLoops) {
        config.set(GRAPH_CONTAINING_LOOPS, graphContainingLoops);
    }

    public boolean isGraphContainingLoops() {
        return config.get(GRAPH_CONTAINING_LOOPS);
    }

    public void setAttribute(Attribute attribute) {
        if (attribute != null) {
            toBeSerializedConfigObjects.put(ATTRIBUTE, attribute);
        }
    }

    public Attribute getAttribute(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, ATTRIBUTE, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate checkpoint storage.", e);
        }
    }

    /**
     * In general, we don't clear any configuration. However, the {@link #SERIALIZED_UDF} may be
     * very large when operator includes some large objects, the SERIALIZED_UDF is used to create a
     * StreamOperator and usually only needs to be called once. {@link #CHAINED_TASK_CONFIG} may be
     * large as well due to the StreamConfig of all non-head operators in OperatorChain will be
     * serialized and stored in CHAINED_TASK_CONFIG. They can be cleared to reduce the memory after
     * StreamTask is initialized. If so, TM will have more memory during running. See FLINK-33315
     * and FLINK-33317 for more information.
     */
    public void clearInitialConfigs() {
        removedKeys.add(SERIALIZED_UDF);
        config.removeKey(SERIALIZED_UDF);

        removedKeys.add(CHAINED_TASK_CONFIG);
        config.removeKey(CHAINED_TASK_CONFIG);
    }

    /**
     * Requirements of the different inputs of an operator. Each input can have a different
     * requirement. For all {@link #SORTED} inputs, records are sorted/grouped by key and all
     * records of a given key are passed to the operator consecutively before moving on to the next
     * group.
     */
    public enum InputRequirement {
        /**
         * Records from all sorted inputs are grouped (sorted) by key and are then fed to the
         * operator one group at a time. This "zig-zags" between different inputs if records for the
         * same key arrive on multiple inputs to ensure that the operator sees all records with a
         * key as one consecutive group.
         */
        SORTED,

        /**
         * Records from {@link #PASS_THROUGH} inputs are passed to the operator before passing any
         * records from {@link #SORTED} inputs. There are no guarantees on ordering between and
         * within the different {@link #PASS_THROUGH} inputs.
         */
        PASS_THROUGH;
    }

    /** Interface representing chained inputs. */
    public interface InputConfig extends Serializable {}

    /** A representation of a Network {@link InputConfig}. */
    public static class NetworkInputConfig implements InputConfig {
        private final TypeSerializer<?> typeSerializer;
        private final InputRequirement inputRequirement;

        private int inputGateIndex;

        public NetworkInputConfig(TypeSerializer<?> typeSerializer, int inputGateIndex) {
            this(typeSerializer, inputGateIndex, InputRequirement.PASS_THROUGH);
        }

        public NetworkInputConfig(
                TypeSerializer<?> typeSerializer,
                int inputGateIndex,
                InputRequirement inputRequirement) {
            this.typeSerializer = typeSerializer;
            this.inputGateIndex = inputGateIndex;
            this.inputRequirement = inputRequirement;
        }

        public TypeSerializer<?> getTypeSerializer() {
            return typeSerializer;
        }

        public int getInputGateIndex() {
            return inputGateIndex;
        }

        public InputRequirement getInputRequirement() {
            return inputRequirement;
        }
    }

    /** A serialized representation of an input. */
    public static class SourceInputConfig implements InputConfig {
        private final StreamEdge inputEdge;

        public SourceInputConfig(StreamEdge inputEdge) {
            this.inputEdge = inputEdge;
        }

        public StreamEdge getInputEdge() {
            return inputEdge;
        }

        @Override
        public String toString() {
            return inputEdge.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof SourceInputConfig)) {
                return false;
            }
            SourceInputConfig other = (SourceInputConfig) obj;
            return Objects.equals(other.inputEdge, inputEdge);
        }

        @Override
        public int hashCode() {
            return inputEdge.hashCode();
        }
    }

    public static boolean requiresSorting(StreamConfig.InputConfig inputConfig) {
        return inputConfig instanceof StreamConfig.NetworkInputConfig
                && ((StreamConfig.NetworkInputConfig) inputConfig).getInputRequirement()
                        == StreamConfig.InputRequirement.SORTED;
    }
}

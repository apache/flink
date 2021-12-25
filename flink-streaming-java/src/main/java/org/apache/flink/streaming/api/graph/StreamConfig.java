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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.util.CorruptConfigurationException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TernaryBoolean;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    @VisibleForTesting public static final String SERIALIZEDUDF = "serializedUDF";

    private static final String NUMBER_OF_OUTPUTS = "numberOfOutputs";
    private static final String NUMBER_OF_NETWORK_INPUTS = "numberOfNetworkInputs";
    private static final String CHAINED_OUTPUTS = "chainedOutputs";
    private static final String CHAINED_TASK_CONFIG = "chainedTaskConfig_";
    private static final String IS_CHAINED_VERTEX = "isChainedSubtask";
    private static final String CHAIN_INDEX = "chainIndex";
    private static final String VERTEX_NAME = "vertexID";
    private static final String ITERATION_ID = "iterationId";
    private static final String INPUTS = "inputs";
    private static final String TYPE_SERIALIZER_OUT_1 = "typeSerializer_out";
    private static final String TYPE_SERIALIZER_SIDEOUT_PREFIX = "typeSerializer_sideout_";
    private static final String ITERATON_WAIT = "iterationWait";
    private static final String NONCHAINED_OUTPUTS = "nonChainedOutputs";
    private static final String EDGES_IN_ORDER = "edgesInOrder";
    private static final String IN_STREAM_EDGES = "inStreamEdges";
    private static final String OPERATOR_NAME = "operatorName";
    private static final String OPERATOR_ID = "operatorID";
    private static final String CHAIN_END = "chainEnd";
    private static final String GRAPH_CONTAINING_LOOPS = "graphContainingLoops";

    private static final String CHECKPOINTING_ENABLED = "checkpointing";
    private static final String CHECKPOINT_MODE = "checkpointMode";

    private static final String SAVEPOINT_DIR = "savepointdir";
    private static final String CHECKPOINT_STORAGE = "checkpointstorage";
    private static final String STATE_BACKEND = "statebackend";
    private static final String ENABLE_CHANGE_LOG_STATE_BACKEND = "enablechangelog";
    private static final String TIMER_SERVICE_PROVIDER = "timerservice";
    private static final String STATE_PARTITIONER = "statePartitioner";

    private static final String STATE_KEY_SERIALIZER = "statekeyser";

    private static final String TIME_CHARACTERISTIC = "timechar";

    private static final String MANAGED_MEMORY_FRACTION_PREFIX = "managedMemFraction.";
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

    public StreamConfig(Configuration config) {
        this.config = config;
    }

    public Configuration getConfiguration() {
        return config;
    }

    // ------------------------------------------------------------------------
    //  Configured Properties
    // ------------------------------------------------------------------------

    public void setVertexID(Integer vertexID) {
        config.setInteger(VERTEX_NAME, vertexID);
    }

    public Integer getVertexID() {
        return config.getInteger(VERTEX_NAME, -1);
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

        config.setDouble(configOption, fraction);
    }

    /**
     * Fraction of total managed memory in the slot that this operator should use for the given use
     * case.
     */
    public double getManagedMemoryFractionOperatorUseCaseOfSlot(
            ManagedMemoryUseCase managedMemoryUseCase,
            Configuration taskManagerConfig,
            ClassLoader cl) {
        return ManagedMemoryUtils.convertToFractionOfSlot(
                managedMemoryUseCase,
                config.getDouble(getManagedMemoryFractionConfigOption(managedMemoryUseCase)),
                getAllManagedMemoryUseCases(),
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

    public void setTimeCharacteristic(TimeCharacteristic characteristic) {
        config.setInteger(TIME_CHARACTERISTIC, characteristic.ordinal());
    }

    public TimeCharacteristic getTimeCharacteristic() {
        int ordinal = config.getInteger(TIME_CHARACTERISTIC, -1);
        if (ordinal >= 0) {
            return TimeCharacteristic.values()[ordinal];
        } else {
            throw new CorruptConfigurationException("time characteristic is not set");
        }
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
        try {
            InstantiationUtil.writeObjectToConfig(typeWrapper, this.config, key);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize type serializer.", e);
        }
    }

    public <T> TypeSerializer<T> getTypeSerializerSideOut(OutputTag<?> outputTag, ClassLoader cl) {
        Preconditions.checkNotNull(outputTag, "Side output id must not be null.");
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
        try {
            InstantiationUtil.writeObjectToConfig(inputs, this.config, INPUTS);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize inputs.", e);
        }
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
            try {
                InstantiationUtil.writeObjectToConfig(factory, this.config, SERIALIZEDUDF);
            } catch (IOException e) {
                throw new StreamTaskException(
                        "Cannot serialize operator object " + factory.getClass() + ".", e);
            }
        }
    }

    @VisibleForTesting
    public <T extends StreamOperator<?>> T getStreamOperator(ClassLoader cl) {
        SimpleOperatorFactory<?> factory = getStreamOperatorFactory(cl);
        return (T) factory.getOperator();
    }

    public <T extends StreamOperatorFactory<?>> T getStreamOperatorFactory(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, SERIALIZEDUDF, cl);
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

    public void setIterationId(String iterationId) {
        config.setString(ITERATION_ID, iterationId);
    }

    public String getIterationId() {
        return config.getString(ITERATION_ID, "");
    }

    public void setIterationWaitTime(long time) {
        config.setLong(ITERATON_WAIT, time);
    }

    public long getIterationWaitTime() {
        return config.getLong(ITERATON_WAIT, 0);
    }

    public void setNumberOfNetworkInputs(int numberOfInputs) {
        config.setInteger(NUMBER_OF_NETWORK_INPUTS, numberOfInputs);
    }

    public int getNumberOfNetworkInputs() {
        return config.getInteger(NUMBER_OF_NETWORK_INPUTS, 0);
    }

    public void setNumberOfOutputs(int numberOfOutputs) {
        config.setInteger(NUMBER_OF_OUTPUTS, numberOfOutputs);
    }

    public int getNumberOfOutputs() {
        return config.getInteger(NUMBER_OF_OUTPUTS, 0);
    }

    public void setNonChainedOutputs(List<StreamEdge> outputvertexIDs) {
        try {
            InstantiationUtil.writeObjectToConfig(outputvertexIDs, this.config, NONCHAINED_OUTPUTS);
        } catch (IOException e) {
            throw new StreamTaskException("Cannot serialize non chained outputs.", e);
        }
    }

    public List<StreamEdge> getNonChainedOutputs(ClassLoader cl) {
        try {
            List<StreamEdge> nonChainedOutputs =
                    InstantiationUtil.readObjectFromConfig(this.config, NONCHAINED_OUTPUTS, cl);
            return nonChainedOutputs == null ? new ArrayList<StreamEdge>() : nonChainedOutputs;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate non chained outputs.", e);
        }
    }

    public void setChainedOutputs(List<StreamEdge> chainedOutputs) {
        try {
            InstantiationUtil.writeObjectToConfig(chainedOutputs, this.config, CHAINED_OUTPUTS);
        } catch (IOException e) {
            throw new StreamTaskException("Cannot serialize chained outputs.", e);
        }
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
        try {
            InstantiationUtil.writeObjectToConfig(inEdges, this.config, IN_STREAM_EDGES);
        } catch (IOException e) {
            throw new StreamTaskException("Cannot serialize inward edges.", e);
        }
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
        config.setBoolean(CHECKPOINTING_ENABLED, enabled);
    }

    public boolean isCheckpointingEnabled() {
        return config.getBoolean(CHECKPOINTING_ENABLED, false);
    }

    public void setCheckpointMode(CheckpointingMode mode) {
        config.setInteger(CHECKPOINT_MODE, mode.ordinal());
    }

    public CheckpointingMode getCheckpointMode() {
        int ordinal = config.getInteger(CHECKPOINT_MODE, -1);
        if (ordinal >= 0) {
            return CheckpointingMode.values()[ordinal];
        } else {
            return DEFAULT_CHECKPOINTING_MODE;
        }
    }

    public void setUnalignedCheckpointsEnabled(boolean enabled) {
        config.setBoolean(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, enabled);
    }

    public boolean isUnalignedCheckpointsEnabled() {
        return config.getBoolean(ExecutionCheckpointingOptions.ENABLE_UNALIGNED, false);
    }

    public boolean isExactlyOnceCheckpointMode() {
        return getCheckpointMode() == CheckpointingMode.EXACTLY_ONCE;
    }

    public Duration getAlignedCheckpointTimeout() {
        return config.get(ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT);
    }

    public void setAlignedCheckpointTimeout(Duration alignedCheckpointTimeout) {
        config.set(
                ExecutionCheckpointingOptions.ALIGNED_CHECKPOINT_TIMEOUT, alignedCheckpointTimeout);
    }

    public void setOutEdgesInOrder(List<StreamEdge> outEdgeList) {
        try {
            InstantiationUtil.writeObjectToConfig(outEdgeList, this.config, EDGES_IN_ORDER);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize outputs in order.", e);
        }
    }

    public List<StreamEdge> getOutEdgesInOrder(ClassLoader cl) {
        try {
            List<StreamEdge> outEdgesInOrder =
                    InstantiationUtil.readObjectFromConfig(this.config, EDGES_IN_ORDER, cl);
            return outEdgesInOrder == null ? new ArrayList<StreamEdge>() : outEdgesInOrder;
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate outputs in order.", e);
        }
    }

    public void setTransitiveChainedTaskConfigs(Map<Integer, StreamConfig> chainedTaskConfigs) {

        try {
            InstantiationUtil.writeObjectToConfig(
                    chainedTaskConfigs, this.config, CHAINED_TASK_CONFIG);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize configuration.", e);
        }
    }

    public Map<Integer, StreamConfig> getTransitiveChainedTaskConfigs(ClassLoader cl) {
        try {
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
        return new OperatorID(Preconditions.checkNotNull(operatorIDBytes));
    }

    public void setOperatorName(String name) {
        this.config.setString(OPERATOR_NAME, name);
    }

    public String getOperatorName() {
        return this.config.getString(OPERATOR_NAME, null);
    }

    public void setChainIndex(int index) {
        this.config.setInteger(CHAIN_INDEX, index);
    }

    public int getChainIndex() {
        return this.config.getInteger(CHAIN_INDEX, 0);
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    public void setStateBackend(StateBackend backend) {
        if (backend != null) {
            try {
                InstantiationUtil.writeObjectToConfig(backend, this.config, STATE_BACKEND);
                setStateBackendUsesManagedMemory(backend.useManagedMemory());
            } catch (Exception e) {
                throw new StreamTaskException("Could not serialize stateHandle provider.", e);
            }
        }
    }

    public void setChangelogStateBackendEnabled(TernaryBoolean enabled) {
        try {
            InstantiationUtil.writeObjectToConfig(
                    enabled, this.config, ENABLE_CHANGE_LOG_STATE_BACKEND);
        } catch (Exception e) {
            throw new StreamTaskException(
                    "Could not serialize change log state backend enable flag.", e);
        }
    }

    @VisibleForTesting
    public void setStateBackendUsesManagedMemory(boolean usesManagedMemory) {
        this.config.setBoolean(STATE_BACKEND_USE_MANAGED_MEMORY, usesManagedMemory);
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

    public void setSavepointDir(Path directory) {
        if (directory != null) {
            try {
                InstantiationUtil.writeObjectToConfig(directory, config, SAVEPOINT_DIR);
            } catch (Exception e) {
                throw new StreamTaskException("Could not serialize savepoint directory.", e);
            }
        }
    }

    public Path getSavepointDir(ClassLoader cl) {
        try {
            return InstantiationUtil.readObjectFromConfig(this.config, SAVEPOINT_DIR, cl);
        } catch (Exception e) {
            throw new StreamTaskException("Could not instantiate savepoint directory.", e);
        }
    }

    public void setCheckpointStorage(CheckpointStorage storage) {
        if (storage != null) {
            try {
                InstantiationUtil.writeObjectToConfig(storage, config, CHECKPOINT_STORAGE);
            } catch (Exception e) {
                throw new StreamTaskException("Could not serialize checkpoint storage.", e);
            }
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
            try {
                InstantiationUtil.writeObjectToConfig(
                        timerServiceProvider, this.config, TIMER_SERVICE_PROVIDER);
            } catch (Exception e) {
                throw new StreamTaskException("Could not serialize timer service provider.", e);
            }
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
        try {
            InstantiationUtil.writeObjectToConfig(
                    partitioner, this.config, STATE_PARTITIONER + input);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize state partitioner.", e);
        }
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
        try {
            InstantiationUtil.writeObjectToConfig(serializer, this.config, STATE_KEY_SERIALIZER);
        } catch (IOException e) {
            throw new StreamTaskException("Could not serialize state key serializer.", e);
        }
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
        config.setBoolean(IS_CHAINED_VERTEX, true);
    }

    public boolean isChainStart() {
        return config.getBoolean(IS_CHAINED_VERTEX, false);
    }

    public void setChainEnd() {
        config.setBoolean(CHAIN_END, true);
    }

    public boolean isChainEnd() {
        return config.getBoolean(CHAIN_END, false);
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
        builder.append("\nOutput names: ").append(getNonChainedOutputs(cl));
        builder.append("\nPartitioning:");
        for (StreamEdge output : getNonChainedOutputs(cl)) {
            int outputname = output.getTargetId();
            builder.append("\n\t").append(outputname).append(": ").append(output.getPartitioner());
        }

        builder.append("\nChained subtasks: ").append(getChainedOutputs(cl));

        try {
            builder.append("\nOperator: ")
                    .append(getStreamOperatorFactory(cl).getClass().getSimpleName());
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
        config.setBoolean(GRAPH_CONTAINING_LOOPS, graphContainingLoops);
    }

    public boolean isGraphContainingLoops() {
        return config.getBoolean(GRAPH_CONTAINING_LOOPS, false);
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

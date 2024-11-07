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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.state.v2.KeyedStateStoreV2;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext}, for streaming
 * operators.
 */
@Internal
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

    /** The task environment running the operator. */
    private final Environment taskEnvironment;

    private final StreamConfig streamConfig;
    private final String operatorUniqueID;
    private final ProcessingTimeService processingTimeService;
    private @Nullable KeyedStateStore keyedStateStore;
    private @Nullable KeyedStateStoreV2 keyedStateStoreV2;
    private SupportKeyedStateApiSet supportKeyedStateApiSet;
    private final ExternalResourceInfoProvider externalResourceInfoProvider;

    @VisibleForTesting
    public StreamingRuntimeContext(
            AbstractStreamOperator<?> operator,
            Environment env,
            Map<String, Accumulator<?, ?>> accumulators) {
        this(
                env,
                accumulators,
                operator.getMetricGroup(),
                operator.getOperatorID(),
                operator.getProcessingTimeService(),
                operator.getKeyedStateStore(),
                env.getExternalResourceInfoProvider());
    }

    public StreamingRuntimeContext(
            Environment env,
            Map<String, Accumulator<?, ?>> accumulators,
            OperatorMetricGroup operatorMetricGroup,
            OperatorID operatorID,
            ProcessingTimeService processingTimeService,
            @Nullable KeyedStateStore keyedStateStore,
            ExternalResourceInfoProvider externalResourceInfoProvider) {
        super(
                checkNotNull(env).getJobInfo(),
                checkNotNull(env).getTaskInfo(),
                env.getUserCodeClassLoader(),
                env.getExecutionConfig(),
                accumulators,
                env.getDistributedCacheEntries(),
                operatorMetricGroup);
        this.taskEnvironment = env;
        this.streamConfig = new StreamConfig(env.getTaskConfiguration());
        this.operatorUniqueID = checkNotNull(operatorID).toString();
        this.processingTimeService = processingTimeService;
        this.keyedStateStore = keyedStateStore;
        // By default, support state v1
        this.supportKeyedStateApiSet = SupportKeyedStateApiSet.STATE_V1;
        this.externalResourceInfoProvider = externalResourceInfoProvider;
    }

    public void setKeyedStateStore(@Nullable KeyedStateStore keyedStateStore) {
        this.keyedStateStore = keyedStateStore;
    }

    public void setKeyedStateStoreV2(@Nullable KeyedStateStoreV2 keyedStateStoreV2) {
        if (keyedStateStoreV2 != null) {
            // Only if the keyedStateStoreV2 is set, this context is switch to support state v2
            this.keyedStateStoreV2 = keyedStateStoreV2;
            this.supportKeyedStateApiSet = SupportKeyedStateApiSet.STATE_V2;
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Returns the input split provider associated with the operator.
     *
     * @return The input split provider.
     */
    public InputSplitProvider getInputSplitProvider() {
        return taskEnvironment.getInputSplitProvider();
    }

    public ProcessingTimeService getProcessingTimeService() {
        return processingTimeService;
    }

    /**
     * Returns the global aggregate manager for the current job.
     *
     * @return The global aggregate manager.
     */
    public GlobalAggregateManager getGlobalAggregateManager() {
        return taskEnvironment.getGlobalAggregateManager();
    }

    /**
     * Returned value is guaranteed to be unique between operators within the same job and to be
     * stable and the same across job submissions.
     *
     * <p>This operation is currently only supported in Streaming (DataStream) contexts.
     *
     * @return String representation of the operator's unique id.
     */
    public String getOperatorUniqueID() {
        return operatorUniqueID;
    }

    /**
     * Returns the task manager runtime info of the task manager running this stream task.
     *
     * @return The task manager runtime info.
     */
    public TaskManagerRuntimeInfo getTaskManagerRuntimeInfo() {
        return taskEnvironment.getTaskManagerInfo();
    }

    public Configuration getJobConfiguration() {
        return taskEnvironment.getJobConfiguration();
    }

    public JobType getJobType() {
        return taskEnvironment.getJobType();
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        return externalResourceInfoProvider.getExternalResourceInfos(resourceName);
    }

    // ------------------------------------------------------------------------
    //  broadcast variables
    // ------------------------------------------------------------------------

    @Override
    public boolean hasBroadcastVariable(String name) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(
            String name, BroadcastVariableInitializer<T, C> initializer) {
        throw new UnsupportedOperationException(
                "Broadcast variables can only be used in DataSet programs");
    }

    // ------------------------------------------------------------------------
    //  key/value state
    // ------------------------------------------------------------------------

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getState(stateProperties);
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getListState(stateProperties);
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getReducingState(stateProperties);
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getAggregatingState(stateProperties);
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
        stateProperties.initializeSerializerUnlessSet(this::createSerializer);
        return keyedStateStore.getMapState(stateProperties);
    }

    private KeyedStateStore checkPreconditionsAndGetKeyedStateStore(
            StateDescriptor<?, ?> stateDescriptor) {
        checkNotNull(stateDescriptor, "The state properties must not be null");
        checkState(
                supportKeyedStateApiSet == SupportKeyedStateApiSet.STATE_V1,
                "Current operator integrates the logic of async processing, "
                        + "thus only support state v2 APIs. Please use StateDescriptor under "
                        + "'org.apache.flink.runtime.state.v2' or make current operator extend "
                        + "from AbstractStreamOperator/AbstractStreamOperatorV2.");
        checkNotNull(
                keyedStateStore,
                String.format(
                        "Keyed state '%s' with type %s can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                        stateDescriptor.getName(), stateDescriptor.getType()));
        return keyedStateStore;
    }

    // TODO: Reconstruct this after StateManager is ready in FLIP-410.
    public <T> org.apache.flink.api.common.state.v2.ValueState<T> getValueState(
            org.apache.flink.runtime.state.v2.ValueStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getValueState(stateProperties);
    }

    public <T> org.apache.flink.api.common.state.v2.ListState<T> getListState(
            org.apache.flink.runtime.state.v2.ListStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getListState(stateProperties);
    }

    public <UK, UV> org.apache.flink.api.common.state.v2.MapState<UK, UV> getMapState(
            org.apache.flink.runtime.state.v2.MapStateDescriptor<UK, UV> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getMapState(stateProperties);
    }

    public <T> org.apache.flink.api.common.state.v2.ReducingState<T> getReducingState(
            org.apache.flink.runtime.state.v2.ReducingStateDescriptor<T> stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getReducingState(stateProperties);
    }

    public <IN, ACC, OUT>
            org.apache.flink.api.common.state.v2.AggregatingState<IN, OUT> getAggregatingState(
                    org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<IN, ACC, OUT>
                            stateProperties) {
        KeyedStateStoreV2 keyedStateStoreV2 =
                checkPreconditionsAndGetKeyedStateStoreV2(stateProperties);
        return keyedStateStoreV2.getAggregatingState(stateProperties);
    }

    private KeyedStateStoreV2 checkPreconditionsAndGetKeyedStateStoreV2(
            org.apache.flink.runtime.state.v2.StateDescriptor<?> stateDescriptor) {
        checkNotNull(stateDescriptor, "The state properties must not be null");
        checkState(
                supportKeyedStateApiSet == SupportKeyedStateApiSet.STATE_V2,
                "Current operator does not integrate the logic of async processing, "
                        + "thus only support state v1 APIs. Please use StateDescriptor under "
                        + "'org.apache.flink.runtime.state' or make current operator extend from "
                        + "AbstractAsyncStateStreamOperator/AbstractAsyncStateStreamOperatorV2.");
        checkNotNull(
                keyedStateStoreV2,
                String.format(
                        "Keyed state '%s' with type %s can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.",
                        stateDescriptor.getStateId(), stateDescriptor.getType()));
        return keyedStateStoreV2;
    }

    // ------------------ expose (read only) relevant information from the stream config -------- //

    /**
     * Returns true if checkpointing is enabled for the running job.
     *
     * @return true if checkpointing is enabled.
     */
    public boolean isCheckpointingEnabled() {
        return streamConfig.isCheckpointingEnabled();
    }

    /**
     * Currently, we only support one keyed state api set. This is determined by the stream
     * operator. This will be set via {@link #setKeyedStateStore} or {@link #setKeyedStateStoreV2}.
     */
    private enum SupportKeyedStateApiSet {
        STATE_V1,
        STATE_V2
    }
}

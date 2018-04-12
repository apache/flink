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
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.FoldingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
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
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
@Internal
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

	/** The operator to which this function belongs. */
	private final AbstractStreamOperator<?> operator;

	/** The task environment running the operator. */
	private final Environment taskEnvironment;

	private final StreamConfig streamConfig;

	public StreamingRuntimeContext(AbstractStreamOperator<?> operator,
									Environment env, Map<String, Accumulator<?, ?>> accumulators) {
		super(env.getTaskInfo(),
				env.getUserClassLoader(),
				operator.getExecutionConfig(),
				accumulators,
				env.getDistributedCacheEntries(),
				operator.getMetricGroup());

		this.operator = operator;
		this.taskEnvironment = env;
		this.streamConfig = new StreamConfig(env.getTaskConfiguration());
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
		return operator.getProcessingTimeService();
	}

	// ------------------------------------------------------------------------
	//  broadcast variables
	// ------------------------------------------------------------------------

	@Override
	public boolean hasBroadcastVariable(String name) {
		throw new UnsupportedOperationException("Broadcast variables can only be used in DataSet programs");
	}

	@Override
	public <RT> List<RT> getBroadcastVariable(String name) {
		throw new UnsupportedOperationException("Broadcast variables can only be used in DataSet programs");
	}

	@Override
	public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
		throw new UnsupportedOperationException("Broadcast variables can only be used in DataSet programs");
	}

	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getState(stateProperties);
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getListState(stateProperties);
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getReducingState(stateProperties);
	}

	@Override
	public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getAggregatingState(stateProperties);
	}

	@Override
	public <T, ACC> FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getFoldingState(stateProperties);
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getMapState(stateProperties);
	}

	private KeyedStateStore checkPreconditionsAndGetKeyedStateStore(StateDescriptor<?, ?> stateDescriptor) {
		Preconditions.checkNotNull(stateDescriptor, "The state properties must not be null");
		KeyedStateStore keyedStateStore = operator.getKeyedStateStore();
		Preconditions.checkNotNull(keyedStateStore, "Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.");
		return keyedStateStore;
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
	 * Returns the checkpointing mode.
	 *
	 * @return checkpointing mode
	 */
	public CheckpointingMode getCheckpointMode() {
		return streamConfig.getCheckpointMode();
	}

	/**
	 * Returns the buffer timeout of the job.
	 *
	 * @return buffer timeout (in milliseconds)
	 */
	public long getBufferTimeout() {
		return streamConfig.getBufferTimeout();
	}

}

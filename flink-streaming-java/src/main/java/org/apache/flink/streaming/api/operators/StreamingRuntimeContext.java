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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.operators.Triggerable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
@PublicEvolving
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

	/** The operator to which this function belongs */
	private final AbstractStreamOperator<?> operator;
	
	/** The task environment running the operator */
	private final Environment taskEnvironment;

	private final StreamConfig streamConfig;
	
	public StreamingRuntimeContext(AbstractStreamOperator<?> operator,
									Environment env, Map<String, Accumulator<?, ?>> accumulators) {
		super(env.getTaskInfo(),
				env.getUserClassLoader(),
				operator.getExecutionConfig(),
				accumulators,
				env.getDistributedCacheEntries());
		
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

	/**
	 * Register a timer callback. At the specified time the {@link Triggerable } will be invoked.
	 * This call is guaranteed to not happen concurrently with method calls on the operator.
	 *
	 * @param time The absolute time in milliseconds.
	 * @param target The target to be triggered.
	 */
	public ScheduledFuture<?> registerTimer(long time, Triggerable target) {
		return operator.registerTimer(time, target);
	}
	
	// ------------------------------------------------------------------------
	//  broadcast variables
	// ------------------------------------------------------------------------

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
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
			return operator.getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
			return operator.getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
			return operator.getPartitionedState(stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

	@Override
	@Deprecated
	public <S> OperatorState<S> getKeyValueState(String name, Class<S> stateType, S defaultState) {
		requireNonNull(stateType, "The state type class must not be null");

		TypeInformation<S> typeInfo;
		try {
			typeInfo = TypeExtractor.getForClass(stateType);
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot analyze type '" + stateType.getName() +
					"' from the class alone, due to generic type parameters. " +
					"Please specify the TypeInformation directly.", e);
		}

		return getKeyValueState(name, typeInfo, defaultState);
	}

	@Override
	@Deprecated
	public <S> OperatorState<S> getKeyValueState(String name, TypeInformation<S> stateType, S defaultState) {
		requireNonNull(name, "The name of the state must not be null");
		requireNonNull(stateType, "The state type information must not be null");

		ValueStateDescriptor<S> stateProps = 
				new ValueStateDescriptor<>(name, stateType, defaultState);
		return getState(stateProps);
	}

	// ------------------ expose (read only) relevant information from the stream config -------- //

	/**
	 * Returns true if checkpointing is enabled for the running job.
	 * @return true if checkpointing is enabled.
	 */
	public boolean isCheckpointingEnabled() {
		return streamConfig.isCheckpointingEnabled();
	}

	/**
	 * Returns the checkpointing mode
	 * @return checkpointing mode
	 */
	public CheckpointingMode getCheckpointMode() {
		return streamConfig.getCheckpointMode();
	}

	/**
	 * Returns the buffer timeout of the job
	 * @return buffer timeout (in milliseconds)
	 */
	public long getBufferTimeout() {
		return streamConfig.getBufferTimeout();
	}

}

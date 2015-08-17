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

package org.apache.flink.streaming.runtime.tasks;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.state.PartitionedStreamOperatorState;
import org.apache.flink.streaming.api.state.StreamOperatorState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Implementation of the {@link RuntimeContext}, created by runtime stream UDF
 * operators.
 */
public class StreamingRuntimeContext extends RuntimeUDFContext {

	private final Environment env;
	private final Map<String, StreamOperatorState<?, ?>> states;
	private final List<PartitionedStreamOperatorState<?, ?, ?>> partitionedStates;
	private final KeySelector<?, ?> statePartitioner;
	private final StateHandleProvider<Serializable> provider;
	
	
	@SuppressWarnings("unchecked")
	public StreamingRuntimeContext(
			Environment env,
			ExecutionConfig executionConfig,
			KeySelector<?, ?> statePartitioner,
			StateHandleProvider<?> provider,
			Map<String, Accumulator<?, ?>> accumulatorMap) {
		
		super(env.getTaskName(), env.getNumberOfSubtasks(), env.getIndexInSubtaskGroup(),
				env.getUserClassLoader(), executionConfig,
				env.getDistributedCacheEntries(), accumulatorMap);
		
		this.env = env;
		this.statePartitioner = statePartitioner;
		this.states = new HashMap<>();
		this.partitionedStates = new LinkedList<>();
		this.provider = (StateHandleProvider<Serializable>) provider;
	}

	/**
	 * Returns the input split provider associated with the operator.
	 * 
	 * @return The input split provider.
	 */
	public InputSplitProvider getInputSplitProvider() {
		return env.getInputSplitProvider();
	}

	/**
	 * Returns the stub parameters associated with the {@link TaskConfig} of the
	 * operator.
	 * 
	 * @return The stub parameters.
	 */
	public Configuration getTaskStubParameters() {
		return new TaskConfig(env.getTaskConfiguration()).getStubParameters();
	}
	
	public StateHandleProvider<Serializable> getStateHandleProvider() {
		return provider;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S, C extends Serializable> OperatorState<S> getOperatorState(String name,
			S defaultState, boolean partitioned, StateCheckpointer<S, C> checkpointer) throws IOException {
		if (defaultState == null) {
			throw new RuntimeException("Cannot set default state to null.");
		}
		StreamOperatorState<S, C> state = (StreamOperatorState<S, C>) getState(name, partitioned);
		state.setDefaultState(defaultState);
		state.setCheckpointer(checkpointer);

		return (OperatorState<S>) state;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState,
			boolean partitioned) throws IOException {
		if (defaultState == null) {
			throw new RuntimeException("Cannot set default state to null.");
		}
		StreamOperatorState<S, S> state = (StreamOperatorState<S, S>) getState(name, partitioned);
		state.setDefaultState(defaultState);

		return (OperatorState<S>) state;
	}

	public StreamOperatorState<?, ?> getState(String name, boolean partitioned) {
		// Try fetching state from the map
		StreamOperatorState<?, ?> state = states.get(name);
		if (state == null) {
			// If not found, create empty state and add to the map
			state = createRawState(partitioned);
			states.put(name, state);
			// We keep a reference to all partitioned states for registering input
			if (state instanceof PartitionedStreamOperatorState) {
				partitionedStates.add((PartitionedStreamOperatorState<?, ?, ?>) state);
			}
		}
		return state;
	}

	/**
	 * Creates an empty {@link OperatorState}.
	 * 
	 * @return An empty operator state.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	public StreamOperatorState<?, ?> createRawState(boolean partitioned) {
		if (partitioned) {
			if (statePartitioner != null) {
				return new PartitionedStreamOperatorState(provider, statePartitioner, getUserCodeClassLoader());
			} else {
				throw new RuntimeException(
						"Partitioned state can only be used with KeyedDataStreams.");
			}
		} else {
			return new StreamOperatorState(provider);
		}
	}

	/**
	 * Provides access to the all the states contained in the context
	 * 
	 * @return All the states for the underlying operator.
	 */
	public Map<String, StreamOperatorState<?, ?>> getOperatorStates() {
		return states;
	}

	/**
	 * Sets the next input of the underlying operators, used to access
	 * partitioned states.
	 * 
	 * @param nextRecord
	 *            Next input of the operator.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setNextInput(StreamRecord<?> nextRecord) {
		if (statePartitioner != null) {
			for (PartitionedStreamOperatorState state : partitionedStates) {
				state.setCurrentInput(nextRecord.getValue());
			}
		}
	}

}

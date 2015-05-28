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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
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

/**
 * Implementation of the {@link RuntimeContext}, created by runtime stream UDF
 * operators.
 */
@SuppressWarnings("rawtypes")
public class StreamingRuntimeContext extends RuntimeUDFContext {

	private final Environment env;
	private final Map<String, StreamOperatorState> states;
	private final KeySelector<?, ?> statePartitioner;
	private final StateHandleProvider<?> provider;

	public StreamingRuntimeContext(String name, Environment env, ClassLoader userCodeClassLoader,
			ExecutionConfig executionConfig, KeySelector<?, ?> statePartitioner,
			StateHandleProvider<?> provider) {
		super(name, env.getNumberOfSubtasks(), env.getIndexInSubtaskGroup(), userCodeClassLoader,
				executionConfig, env.getDistributedCacheEntries());
		this.env = env;
		this.statePartitioner = statePartitioner;
		this.states = new HashMap<String, StreamOperatorState>();
		this.provider = provider;
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

	@SuppressWarnings("unchecked")
	@Override
	public <S, C extends Serializable> OperatorState<S> getOperatorState(String name,
			S defaultState, StateCheckpointer<S, C> checkpointer) {
		StreamOperatorState state;
		if (states.containsKey(name)) {
			state = states.get(name);
		} else {
			state = createRawState();
			states.put(name, state);
		}
		state.setDefaultState(defaultState);
		state.setCheckpointer(checkpointer);

		return (OperatorState<S>) state;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <S extends Serializable> OperatorState<S> getOperatorState(String name, S defaultState) {
		StreamOperatorState state;
		if (states.containsKey(name)) {
			state = states.get(name);
		} else {
			state = createRawState();
			states.put(name, state);
		}
		state.setDefaultState(defaultState);

		return (OperatorState<S>) state;
	}

	/**
	 * Creates an empty state depending on the partitioning state.
	 * 
	 * @return An empty operator state.
	 */
	@SuppressWarnings("unchecked")
	public StreamOperatorState createRawState() {
		if (statePartitioner == null) {
			return new StreamOperatorState(provider);
		} else {
			return new PartitionedStreamOperatorState(provider, statePartitioner);
		}
	}
	
	/**
	 * Provides access to the all the states contained in the context
	 * 
	 * @return All the states for the underlying operator.
	 */
	public Map<String, StreamOperatorState> getOperatorStates() {
		return states;
	}

	/**
	 * Sets the next input of the underlying operators, used to access partitioned states.
	 * @param nextRecord Next input of the operator.
	 */
	@SuppressWarnings("unchecked")
	public void setNextInput(Object nextRecord) {
		if (statePartitioner != null) {
			for (StreamOperatorState state : states.values()) {
				((PartitionedStreamOperatorState) state).setCurrentInput(nextRecord);
			}
		}
	}

}

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

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.streaming.runtime.operators.Triggerable;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

	/** The operator to which this function belongs */
	private final AbstractStreamOperator<?> operator;
	
	/** The task environment running the operator */
	private final Environment taskEnvironment;
	
	/** The key/value state, if the user-function requests it */
	private OperatorState<?> keyValueState;
	
	/** Type of the values stored in the state, to make sure repeated requests of the state are consistent */
	private TypeInformation<?> stateTypeInfo;
	
	
	public StreamingRuntimeContext(AbstractStreamOperator<?> operator,
									Environment env, Map<String, Accumulator<?, ?>> accumulators) {
		super(env.getTaskName(),
				env.getNumberOfSubtasks(),
				env.getIndexInSubtaskGroup(),
				env.getUserClassLoader(),
				operator.getExecutionConfig(),
				accumulators,
				env.getDistributedCacheEntries());
		
		this.operator = operator;
		this.taskEnvironment = env;
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
	public void registerTimer(long time, Triggerable target) {
		operator.registerTimer(time, target);
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
	public <S> OperatorState<S> getKeyValueState(Class<S> stateType, S defaultState) {
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
		
		return getKeyValueState(typeInfo, defaultState);
	}

	@Override
	public <S> OperatorState<S> getKeyValueState(TypeInformation<S> stateType, S defaultState) {
		requireNonNull(stateType, "The state type information must not be null");
		
		// check if this is a repeated call to access the state 
		if (this.stateTypeInfo != null && this.keyValueState != null) {
			// repeated call
			if (this.stateTypeInfo.equals(stateType)) {
				// valid case, same type requested again
				@SuppressWarnings("unchecked")
				OperatorState<S> previous = (OperatorState<S>) this.keyValueState;
				return previous;
			}
			else {
				// invalid case, different type requested this time
				throw new IllegalStateException("Cannot initialize key/value state for type " + stateType +
						" ; The key/value state has already been created and initialized for a different type: " +
						this.stateTypeInfo);
			}
		}
		else {
			// first time access to the key/value state
			try {
				OperatorState<S> state = operator.createKeyValueState(stateType, defaultState);
				this.keyValueState = state;
				this.stateTypeInfo = stateType;
				return state;
			}
			catch (RuntimeException e) {
				throw e;
			}
			catch (Exception e) {
				throw new RuntimeException("Cannot initialize the key/value state", e);
			}
		}
	}
}

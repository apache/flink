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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointCommitter;
import org.apache.flink.streaming.api.state.StreamOperatorState;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

/**
 * This is used as the base class for operators that have a user-defined
 * function.
 * 
 * @param <OUT>
 *            The output type of the operator
 * @param <F>
 *            The type of the user function
 */
public abstract class AbstractUdfStreamOperator<OUT, F extends Function & Serializable> extends AbstractStreamOperator<OUT> implements StatefulStreamOperator<OUT> {

	private static final long serialVersionUID = 1L;

	protected final F userFunction;

	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = userFunction;
	}

	@Override
	public void setup(Output<OUT> output, StreamingRuntimeContext runtimeContext) {
		super.setup(output, runtimeContext);
		FunctionUtils.setFunctionRuntimeContext(userFunction, runtimeContext);
	}


	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		FunctionUtils.openFunction(userFunction, parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		FunctionUtils.closeFunction(userFunction);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void restoreInitialState(Serializable state) throws Exception {

		Map<String, Map<Serializable, StateHandle<Serializable>>> snapshots = (Map<String, Map<Serializable, StateHandle<Serializable>>>) state;

		Map<String, StreamOperatorState> operatorStates = runtimeContext.getOperatorStates();
		
		for (Entry<String, Map<Serializable, StateHandle<Serializable>>> snapshot : snapshots.entrySet()) {
			StreamOperatorState restoredState = runtimeContext.createRawState();
			restoredState.restoreState(snapshot.getValue());
			operatorStates.put(snapshot.getKey(), restoredState);
		}

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Serializable getStateSnapshotFromFunction(long checkpointId, long timestamp) throws Exception {

		Map<String, StreamOperatorState> operatorStates = runtimeContext.getOperatorStates();
		Map<String, Map<Serializable, StateHandle<Serializable>>> snapshots = new HashMap<String, Map<Serializable, StateHandle<Serializable>>>();

		for (Entry<String, StreamOperatorState> state : operatorStates.entrySet()) {
			snapshots.put(state.getKey(), state.getValue().snapshotState(checkpointId, timestamp));
		}

		return (Serializable) snapshots;
	}

	public void confirmCheckpointCompleted(long checkpointId,
			StateHandle<Serializable> checkpointedState) throws Exception {
		if (userFunction instanceof CheckpointCommitter) {
			try {
				((CheckpointCommitter) userFunction).commitCheckpoint(checkpointId, checkpointedState);
			} catch (Exception e) {
				throw new Exception("Error while confirming checkpoint " + checkpointId + " to the stream function", e);
			}
		}
	}

	public F getUserFunction() {
		return userFunction;
	}
}

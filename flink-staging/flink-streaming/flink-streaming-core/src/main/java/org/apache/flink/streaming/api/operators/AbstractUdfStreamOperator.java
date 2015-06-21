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
import org.apache.flink.runtime.state.PartitionedStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointCommitter;
import org.apache.flink.streaming.api.state.StreamOperatorState;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import com.google.common.collect.ImmutableMap;

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
	public void restoreInitialState(Map<String, PartitionedStateHandle> snapshots) throws Exception {
		// We iterate over the states registered for this operator, initialize and restore it
		for (Entry<String, PartitionedStateHandle> snapshot : snapshots.entrySet()) {
			Map<Serializable, StateHandle<Serializable>> handles = snapshot.getValue().getState();
			StreamOperatorState restoredState = runtimeContext.getState(snapshot.getKey(),
					!(handles instanceof ImmutableMap));
			restoredState.restoreState(snapshot.getValue().getState());
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Map<String, PartitionedStateHandle> getStateSnapshotFromFunction(long checkpointId, long timestamp)
			throws Exception {
		// Get all the states for the operator
		Map<String, StreamOperatorState> operatorStates = runtimeContext.getOperatorStates();
		if (operatorStates.isEmpty()) {
			// We return null to signal that there is nothing to checkpoint
			return null;
		} else {
			// Checkpoint the states and store the handles in a map
			Map<String, PartitionedStateHandle> snapshots = new HashMap<String, PartitionedStateHandle>();

			for (Entry<String, StreamOperatorState> state : operatorStates.entrySet()) {
				snapshots.put(state.getKey(),
						new PartitionedStateHandle(state.getValue().snapshotState(checkpointId, timestamp)));
			}

			return snapshots;
		}

	}

	public void confirmCheckpointCompleted(long checkpointId, String stateName,
			StateHandle<Serializable> checkpointedState) throws Exception {
		if (userFunction instanceof CheckpointCommitter) {
			try {
				((CheckpointCommitter) userFunction).commitCheckpoint(checkpointId, stateName, checkpointedState);
			} catch (Exception e) {
				throw new Exception("Error while confirming checkpoint " + checkpointId + " to the stream function", e);
			}
		}
	}

	public F getUserFunction() {
		return userFunction;
	}
}

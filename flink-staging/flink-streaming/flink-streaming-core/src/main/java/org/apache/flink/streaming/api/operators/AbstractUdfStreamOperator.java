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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.state.OperatorStateHandle;
import org.apache.flink.streaming.api.state.PartitionedStreamOperatorState;
import org.apache.flink.streaming.api.state.StreamOperatorState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
	public final void setup(Output<StreamRecord<OUT>> output, StreamingRuntimeContext runtimeContext) {
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

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void restoreInitialState(Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>> snapshots) throws Exception {

		// Restore state using the Checkpointed interface
		if (userFunction instanceof Checkpointed && snapshots.f0 != null) {
			((Checkpointed) userFunction).restoreState(snapshots.f0.getState(runtimeContext.getUserCodeClassLoader()));
		}
		
		if (snapshots.f1 != null) {
			// We iterate over the states registered for this operator, initialize and restore it
			for (Entry<String, OperatorStateHandle> snapshot : snapshots.f1.entrySet()) {
				StreamOperatorState restoredOpState = runtimeContext.getState(snapshot.getKey(), snapshot.getValue().isPartitioned());
				StateHandle<Serializable> checkpointHandle = snapshot.getValue();
				restoredOpState.restoreState(checkpointHandle, runtimeContext.getUserCodeClassLoader());
			}
		}
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple2<StateHandle<Serializable>, Map<String, OperatorStateHandle>> getStateSnapshotFromFunction(long checkpointId, long timestamp)
			throws Exception {
		// Get all the states for the operator
		Map<String, StreamOperatorState<?, ?>> operatorStates = runtimeContext.getOperatorStates();
		
		Map<String, OperatorStateHandle> operatorStateSnapshots;
		if (operatorStates.isEmpty()) {
			// We return null to signal that there is nothing to checkpoint
			operatorStateSnapshots = null;
		} else {
			// Checkpoint the states and store the handles in a map
			Map<String, OperatorStateHandle> snapshots = new HashMap<String, OperatorStateHandle>();

			for (Entry<String, StreamOperatorState<?, ?>> state : operatorStates.entrySet()) {
				boolean isPartitioned = state.getValue() instanceof PartitionedStreamOperatorState;
				snapshots.put(state.getKey(),
						new OperatorStateHandle(state.getValue().snapshotState(checkpointId, timestamp),
								isPartitioned));
			}

			operatorStateSnapshots = snapshots;
		}
		
		StateHandle<Serializable> checkpointedSnapshot = null;
		// if the UDF implements the Checkpointed interface we draw a snapshot
		if (userFunction instanceof Checkpointed) {
			StateHandleProvider<Serializable> provider = runtimeContext.getStateHandleProvider();
			Serializable state = ((Checkpointed) userFunction).snapshotState(checkpointId, timestamp);
			if (state != null) {
				checkpointedSnapshot = provider.createStateHandle(state);
			}
		}
		
		// if we have either operator or checkpointed state we store it in a
		// tuple2 otherwise return null
		if (operatorStateSnapshots != null || checkpointedSnapshot != null) {
			return Tuple2.of(checkpointedSnapshot, operatorStateSnapshots);
		} else {
			return null;
		}

	}

	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if (userFunction instanceof CheckpointNotifier) {
			try {
				((CheckpointNotifier) userFunction).notifyCheckpointComplete(checkpointId);
			} catch (Exception e) {
				throw new Exception("Error while confirming checkpoint " + checkpointId + " to the stream function", e);
			}
		}
	}

	public F getUserFunction() {
		return userFunction;
	}
}

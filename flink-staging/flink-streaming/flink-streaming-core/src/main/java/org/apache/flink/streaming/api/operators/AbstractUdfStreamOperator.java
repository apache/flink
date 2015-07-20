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
import org.apache.flink.runtime.state.PartitionedStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
	public void restoreInitialState(Tuple2<StateHandle<Serializable>, Map<String, PartitionedStateHandle>> snapshots) throws Exception {
		// Restore state using the Checkpointed interface
		if (userFunction instanceof Checkpointed) {
			((Checkpointed) userFunction).restoreState(snapshots.f0.getState());
		}
		
		if (snapshots.f1 != null) {
			// We iterate over the states registered for this operator, initialize and restore it
			for (Entry<String, PartitionedStateHandle> snapshot : snapshots.f1.entrySet()) {
				Map<Serializable, StateHandle<Serializable>> handles = snapshot.getValue().getState();
				StreamOperatorState restoredState = runtimeContext.getState(snapshot.getKey(),
						!(handles instanceof ImmutableMap));
				restoredState.restoreState(snapshot.getValue().getState());
			}
		}
		
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Tuple2<StateHandle<Serializable>, Map<String, PartitionedStateHandle>> getStateSnapshotFromFunction(long checkpointId, long timestamp)
			throws Exception {
		// Get all the states for the operator
		Map<String, StreamOperatorState> operatorStates = runtimeContext.getOperatorStates();
		
		Map<String, PartitionedStateHandle> operatorStateSnapshots;
		if (operatorStates.isEmpty()) {
			// We return null to signal that there is nothing to checkpoint
			operatorStateSnapshots = null;
		} else {
			// Checkpoint the states and store the handles in a map
			Map<String, PartitionedStateHandle> snapshots = new HashMap<String, PartitionedStateHandle>();

			for (Entry<String, StreamOperatorState> state : operatorStates.entrySet()) {
				snapshots.put(state.getKey(),
						new PartitionedStateHandle(state.getValue().snapshotState(checkpointId, timestamp)));
			}

			operatorStateSnapshots = snapshots;
		}
		
		StateHandle<Serializable> checkpointedSnapshot = null;

		if (userFunction instanceof Checkpointed) {
			StateHandleProvider<Serializable> provider = runtimeContext.getStateHandleProvider();
			checkpointedSnapshot = provider.createStateHandle(((Checkpointed) userFunction)
					.snapshotState(checkpointId, timestamp));
		}
		
		if (operatorStateSnapshots != null || checkpointedSnapshot != null) {
			return new Tuple2<StateHandle<Serializable>, Map<String, PartitionedStateHandle>>(
					checkpointedSnapshot, operatorStateSnapshots);
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

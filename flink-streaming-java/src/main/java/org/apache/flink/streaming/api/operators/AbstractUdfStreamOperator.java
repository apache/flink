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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import static java.util.Objects.requireNonNull;

/**
 * This is used as the base class for operators that have a user-defined
 * function. This class handles the opening and closing of the user-defined functions,
 * as part of the operator life cycle.
 * 
 * @param <OUT>
 *            The output type of the operator
 * @param <F>
 *            The type of the user function
 */
public abstract class AbstractUdfStreamOperator<OUT, F extends Function> extends AbstractStreamOperator<OUT> {

	private static final long serialVersionUID = 1L;
	
	
	/** the user function */
	protected final F userFunction;
	
	/** Flag to prevent duplicate function.close() calls in close() and dispose() */
	private transient boolean functionsClosed = false;
	
	
	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = requireNonNull(userFunction);
	}

	/**
	 * Gets the user function executed in this operator.
	 * @return The user function of this operator.
	 */
	public F getUserFunction() {
		return userFunction;
	}
	
	// ------------------------------------------------------------------------
	//  operator life cycle
	// ------------------------------------------------------------------------


	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		
		FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
	}

	@Override
	public void open() throws Exception {
		super.open();
		
		FunctionUtils.openFunction(userFunction, new Configuration());
	}

	@Override
	public void close() throws Exception {
		super.close();
		functionsClosed = true;
		FunctionUtils.closeFunction(userFunction);
	}

	@Override
	public void dispose() {
		if (!functionsClosed) {
			functionsClosed = true;
			try {
				FunctionUtils.closeFunction(userFunction);
			}
			catch (Throwable t) {
				LOG.error("Exception while closing user function while failing or canceling task", t);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  checkpointing and recovery
	// ------------------------------------------------------------------------
	
	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState state = super.snapshotOperatorState(checkpointId, timestamp);

		if (userFunction instanceof Checkpointed) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;
			
			Serializable udfState;
			try {
				udfState = chkFunction.snapshotState(checkpointId, timestamp);
			} 
			catch (Exception e) {
				throw new Exception("Failed to draw state snapshot from function: " + e.getMessage(), e);
			}
			
			if (udfState != null) {
				try {
					StateBackend<?> stateBackend = getStateBackend();
					StateHandle<Serializable> handle = 
							stateBackend.checkpointStateSerializable(udfState, checkpointId, timestamp);
					state.setFunctionState(handle);
				}
				catch (Exception e) {
					throw new Exception("Failed to add the state snapshot of the function to the checkpoint: "
							+ e.getMessage(), e);
				}
			}
		}
		
		return state;
	}

	@Override
	public void restoreState(StreamTaskState state) throws Exception {
		super.restoreState(state);
		
		StateHandle<Serializable> stateHandle =  state.getFunctionState();
		
		if (userFunction instanceof Checkpointed && stateHandle != null) {
			@SuppressWarnings("unchecked")
			Checkpointed<Serializable> chkFunction = (Checkpointed<Serializable>) userFunction;
			
			Serializable functionState = stateHandle.getState(getUserCodeClassloader());
			if (functionState != null) {
				try {
					chkFunction.restoreState(functionState);
				}
				catch (Exception e) {
					throw new Exception("Failed to restore state to function: " + e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		super.notifyOfCompletedCheckpoint(checkpointId);

		if (userFunction instanceof CheckpointNotifier) {
			((CheckpointNotifier) userFunction).notifyCheckpointComplete(checkpointId);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * 
	 * Since the streaming API does not implement any parametrization of functions via a
	 * configuration, the config returned here is actually empty.
	 * 
	 * @return The user function parameters (currently empty)
	 */
	public Configuration getUserFunctionParameters() {
		return new Configuration();
	}
}

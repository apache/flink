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

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointCommitter;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;

import java.io.Serializable;

/**
 * This is used as the base class for operators that have a user-defined function.
 * 
 * @param <OUT> The output type of the operator
 * @param <F> The type of the user function
 */
public abstract class AbstractUdfStreamOperator<OUT, F extends Function & Serializable> extends AbstractStreamOperator<OUT> implements StatefulStreamOperator<OUT> {

	private static final long serialVersionUID = 1L;

	protected final F userFunction;

	public AbstractUdfStreamOperator(F userFunction) {
		this.userFunction = userFunction;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		FunctionUtils.setFunctionRuntimeContext(userFunction, runtimeContext);
		FunctionUtils.openFunction(userFunction, parameters);
	}

	@Override
	public void close() throws Exception{
		super.close();
		FunctionUtils.closeFunction(userFunction);
	}

	public void restoreInitialState(Serializable state) throws Exception {
		if (userFunction instanceof Checkpointed) {
			setStateOnFunction(state, userFunction);
		}
		else {
			throw new IllegalStateException("Trying to restore state of a non-checkpointed function");
		}
	}

	public Serializable getStateSnapshotFromFunction(long checkpointId, long timestamp) throws Exception {
		if (userFunction instanceof Checkpointed) {
			return ((Checkpointed<?>) userFunction).snapshotState(checkpointId, timestamp);
		}
		else {
			return null;
		}
	}

	public void confirmCheckpointCompleted(long checkpointId, long timestamp) throws Exception {
		if (userFunction instanceof CheckpointCommitter) {
			try {
				((CheckpointCommitter) userFunction).commitCheckpoint(checkpointId);
			}
			catch (Exception e) {
				throw new Exception("Error while confirming checkpoint " + checkpointId + " to the stream function", e);
			}
		}
	}

	private static <T extends Serializable> void setStateOnFunction(Serializable state, Function function) {
		@SuppressWarnings("unchecked")
		T typedState = (T) state;
		@SuppressWarnings("unchecked")
		Checkpointed<T> typedFunction = (Checkpointed<T>) function;

		typedFunction.restoreState(typedState);
	}
}

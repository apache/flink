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

import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.state.KvStateSnapshot;

import java.io.Serializable;

/**
 * The state checkpointed by a {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}.
 * This state consists of any combination of those three:
 * <ul>
 *     <li>The state of the stream operator, if it implements the Checkpointed interface.</li>
 *     <li>The state of the user function, if it implements the Checkpointed interface.</li>
 *     <li>The key/value state of the operator, if it executes on a KeyedDataStream.</li>
 * </ul>
 */
public class StreamTaskState implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private StateHandle<?> operatorState;

	private StateHandle<Serializable> functionState;

	private KvStateSnapshot<?, ?, ?> kvState;

	// ------------------------------------------------------------------------

	public StateHandle<?> getOperatorState() {
		return operatorState;
	}

	public void setOperatorState(StateHandle<?> operatorState) {
		this.operatorState = operatorState;
	}

	public StateHandle<Serializable> getFunctionState() {
		return functionState;
	}

	public void setFunctionState(StateHandle<Serializable> functionState) {
		this.functionState = functionState;
	}

	public KvStateSnapshot<?, ?, ?> getKvState() {
		return kvState;
	}

	public void setKvState(KvStateSnapshot<?, ?, ?> kvState) {
		this.kvState = kvState;
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks if this state object actually contains any state, or if all of the state
	 * fields are null.
	 * 
	 * @return True, if all state is null, false if at least one state is not null.
	 */
	public boolean isEmpty() {
		return operatorState == null & functionState == null & kvState == null;
	}

	/**
	 * Discards all the contained states and sets them to null.
	 * 
	 * @throws Exception Forwards exceptions that occur when releasing the
	 *                   state handles and snapshots.
	 */
	public void discardState() throws Exception {
		StateHandle<?> operatorState = this.operatorState;
		StateHandle<?> functionState = this.functionState;
		KvStateSnapshot<?, ?, ?> kvState = this.kvState;
		
		if (operatorState != null) {
			operatorState.discardState();
		}
		if (functionState != null) {
			functionState.discardState();
		}
		if (kvState != null) {
			kvState.discardState();
		}

		this.operatorState = null;
		this.functionState = null;
		this.kvState = null;
	}
}

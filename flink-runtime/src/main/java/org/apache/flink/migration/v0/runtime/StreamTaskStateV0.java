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

package org.apache.flink.migration.v0.runtime;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;


/**
 * The state of a operator in SavepointV0. This state consists of any
 * combination of those three:
 * <ul>
 *     <li>The state of the stream operator.</li>
 *     <li>The state of the user function, if it implements the Checkpointed interface.</li>
 *     <li>The key/value state of the operator, if it executes on a KeyedDataStream.</li>
 * </ul>
 */
@Deprecated
@SuppressWarnings("deprecation")
public class StreamTaskStateV0 implements Serializable, Closeable {

	private static final long serialVersionUID = 1L;
	
	private StateHandleV0 operatorState;

	private StateHandleV0 functionState;

	private HashMap<String, KvStateSnapshotV0<?, ?, ?, ?>> kvStates;

	// ------------------------------------------------------------------------

	public StateHandleV0 getOperatorState() {
		return operatorState;
	}

	public void setOperatorState(StateHandleV0 operatorState) {
		this.operatorState = operatorState;
	}

	public StateHandleV0 getFunctionState() {
		return functionState;
	}

	public void setFunctionState(StateHandleV0 functionState) {
		this.functionState = functionState;
	}

	public HashMap<String, KvStateSnapshotV0<?, ?, ?, ?>> getKvStates() {
		return kvStates;
	}

	public void setKvStates(HashMap<String, KvStateSnapshotV0<?, ?, ?, ?>> kvStates) {
		this.kvStates = kvStates;
	}

	// ------------------------------------------------------------------------

	/**
	 * Checks if this state object actually contains any state, or if all of the state
	 * fields are null.
	 * 
	 * @return True, if all state is null, false if at least one state is not null.
	 */
	public boolean isEmpty() {
		return operatorState == null & functionState == null & kvStates == null;
	}


	@Override
	public void close() throws IOException {

	}
}

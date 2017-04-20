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

package org.apache.flink.migration.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.StateHandle;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

/**
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
@Internal
@SuppressWarnings("deprecation")
public class StreamTaskState implements Serializable, Closeable {

	private static final long serialVersionUID = 1L;
	
	private StateHandle<?> operatorState;

	private StateHandle<Serializable> functionState;

	private HashMap<String, KvStateSnapshot<?, ?, ?, ?>> kvStates;

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

	public HashMap<String, KvStateSnapshot<?, ?, ?, ?>> getKvStates() {
		return kvStates;
	}

	public void setKvStates(HashMap<String, KvStateSnapshot<?, ?, ?, ?>> kvStates) {
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

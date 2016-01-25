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

import org.apache.flink.runtime.state.KvStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;

import java.util.HashMap;

/**
 * List of task states for a chain of streaming tasks.
 */
public class StreamTaskStateList implements StateHandle<StreamTaskState[]> {

	private static final long serialVersionUID = 1L;

	/** The states for all operator */
	private final StreamTaskState[] states;

	private final long stateSize;
	
	public StreamTaskStateList(StreamTaskState[] states) throws Exception {
		this.states = states;

		long sumStateSize = 0;

		if (states != null) {
			for (StreamTaskState state : states) {
				if (state != null) {
					StateHandle<?> operatorState = state.getOperatorState();
					StateHandle<?> functionState = state.getFunctionState();
					HashMap<String, KvStateSnapshot<?, ?, ?, ?, ?>> kvStates = state.getKvStates();

					if (operatorState != null) {
						sumStateSize += operatorState.getStateSize();
					}

					if (functionState != null) {
						sumStateSize += functionState.getStateSize();
					}

					if (kvStates != null) {
						for (KvStateSnapshot<?, ?, ?, ?, ?> kvState : kvStates.values()) {
							if (kvState != null) {
								sumStateSize += kvState.getStateSize();
							}
						}
					}
				}
			}
		}

		// State size as sum of all state sizes
		stateSize = sumStateSize;
	}

	public boolean isEmpty() {
		for (StreamTaskState state : states) {
			if (state != null) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public StreamTaskState[] getState(ClassLoader userCodeClassLoader) {
		return states;
	}

	@Override
	public void discardState() throws Exception {
		for (StreamTaskState state : states) {
			if (state != null) {
				state.discardState();
			}
		}
	}

	@Override
	public long getStateSize() throws Exception {
		return stateSize;
	}
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateHandle;


/**
 * List of non-partitioned stream operator states for a chain of streaming operators.
 */
@Internal
public class StreamTaskState implements StateHandle<StreamOperatorNonPartitionedState[]> {

	private static final long serialVersionUID = 1L;

	/** The states for all operator */
	private final StreamOperatorNonPartitionedState[] nonPartitionedStates;

	public StreamTaskState(StreamOperatorNonPartitionedState[] nonPartitionedStates) throws Exception {
		this.nonPartitionedStates = nonPartitionedStates;
	}

	public boolean isEmpty() {
		for (StreamOperatorNonPartitionedState state : nonPartitionedStates) {
			if (state != null) {
				return false;
			}
		}
		return true;
	}
	
	@Override
	public StreamOperatorNonPartitionedState[] getState(ClassLoader userCodeClassLoader) {
		return nonPartitionedStates;
	}

	@Override
	public void discardState() throws Exception {
		for (StreamOperatorNonPartitionedState state : nonPartitionedStates) {
			if (state != null) {
				state.discardState();
			}
		}
	}

	@Override
	public long getStateSize() throws Exception {
		long sumStateSize = 0;

		if (nonPartitionedStates != null) {
			for (StreamOperatorNonPartitionedState state : nonPartitionedStates) {
				if (state != null) {
					StateHandle<?> operatorState = state.getOperatorState();
					StateHandle<?> functionState = state.getFunctionState();

					if (operatorState != null) {
						sumStateSize += operatorState.getStateSize();
					}

					if (functionState != null) {
						sumStateSize += functionState.getStateSize();
					}
				}
			}
		}

		// State size as sum of all state sizes
		return sumStateSize;
	}
}

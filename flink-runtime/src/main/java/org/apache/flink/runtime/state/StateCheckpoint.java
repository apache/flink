/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import java.io.Serializable;

/**
 * Base class for creating checkpoints for {@link OperatorState}. This
 * checkpoints will be used to backup states in stateful Flink operators and
 * also to restore them in case of node failure. To allow incremental
 * checkpoints override the {@link #update(StateCheckpoint)} method.
 * 
 * @param <T>
 *            The type of the state.
 */
public class StateCheckpoint<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	public OperatorState<T> checkpointedState;

	/**
	 * Creates a state checkpoint from the given {@link OperatorState}
	 * 
	 * @param operatorState
	 *            The {@link OperatorState} to checkpoint.
	 */
	public StateCheckpoint(OperatorState<T> operatorState) {
		this.checkpointedState = operatorState;
	}

	public OperatorState<T> restore() {
		return checkpointedState;
	}

	@Override
	public String toString() {
		return checkpointedState.toString();
	}

	public boolean stateEquals(StateCheckpoint<T> other) {
		return checkpointedState.equals(other.checkpointedState);
	}
}

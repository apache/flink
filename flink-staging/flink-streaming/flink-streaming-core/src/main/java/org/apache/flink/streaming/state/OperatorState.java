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

package org.apache.flink.streaming.state;

import java.io.Serializable;

import org.apache.flink.streaming.state.checkpoint.StateCheckpoint;

/**
 * Abstract class for representing operator states in Flink programs. By
 * implementing the methods declared in this abstraction the state of the
 * operator can be checkpointed by the fault tolerance mechanism.
 *
 * @param <T>
 *            The type of the operator state.
 */
public abstract class OperatorState<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	protected T state;

	/**
	 * Constructor used for initializing the state. In case of failure, the
	 * state will be reinitialized using this constructor, then
	 * {@link #restore(StateCheckpoint)} will be used to restore from the last
	 * available backup.
	 */
	public OperatorState() {
		state = null;
	}

	/**
	 * Initializes the state using the given state object.
	 * 
	 * @param initialState
	 *            The initial state object
	 */
	public OperatorState(T initialState) {
		state = initialState;
	}

	/**
	 * Returns the currently stored state object.
	 * 
	 * @return The state.
	 */
	public T getState() {
		return state;
	}

	/**
	 * Sets the current state object.
	 * 
	 * @param state
	 *            The new state object.
	 * @return The operator state with the new state object set.
	 */
	public OperatorState<T> setState(T state) {
		this.state = state;
		return this;
	}

	/**
	 * Creates a {@link StateCheckpoint} that will be used to backup the state
	 * for failure recovery.
	 * 
	 * @return The {@link StateCheckpoint} created.
	 */
	public abstract StateCheckpoint<T> checkpoint();

	/**
	 * Restores the state from the given {@link StateCheckpoint}.
	 * 
	 * @param checkpoint
	 *            The checkpoint to restore from
	 * @return The restored operator.
	 */
	public abstract OperatorState<T> restore(StateCheckpoint<T> checkpoint);

	@Override
	public String toString() {
		return state.toString();
	}

	public boolean stateEquals(OperatorState<T> other) {
		return state.equals(other.state);
	}

}

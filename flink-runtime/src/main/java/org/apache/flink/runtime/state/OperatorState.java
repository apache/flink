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
 * Abstract class for representing operator states in Flink programs. By
 * implementing the methods declared in this abstraction the state of the
 * operator can be checkpointed by the fault tolerance mechanism.
 * 
 * @param <T>
 *            The type of the operator state.
 */
public class OperatorState<T> implements Serializable {

	private static final long serialVersionUID = 1L;

	private T stateObject;

	/**
	 * Initializes the state using the given state object.
	 * 
	 * @param initialState
	 *            The initial state object
	 */
	public OperatorState(T initialState) {
		stateObject = initialState;
	}

	/**
	 * Returns the currently stored state object.
	 * 
	 * @return The state.
	 */
	public T getState() {
		return stateObject;
	}

	/**
	 * Updates the current state object. States should be only updated using
	 * this method to avoid concurrency issues.
	 * 
	 * @param stateUpdate
	 *            The update applied.
	 */
	@SuppressWarnings("unchecked")
	public synchronized void update(Object stateUpdate) {
		this.stateObject = (T) stateUpdate;
	}

	/**
	 * Creates a {@link StateCheckpoint} that will be used to backup the state
	 * for failure recovery. This method will be called by the state
	 * checkpointer.
	 * 
	 * @return The {@link StateCheckpoint} created.
	 */
	public synchronized StateCheckpoint<T> checkpoint() {
		return new StateCheckpoint<T>(this);
	}

	@Override
	public String toString() {
		return stateObject.toString();
	}

	public boolean stateEquals(OperatorState<T> other) {
		return stateObject.equals(other.stateObject);
	}

}

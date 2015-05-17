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

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * Base class for all streaming operator states. It can represent both
 * partitioned (when state partitioning is defined in the program) or
 * non-partitioned user states.
 * 
 * State can be accessed and manipulated using the {@link #getState()} and
 * {@link #updateState(T)} methods. These calls are only valid in the
 * transformation call the operator represents, for instance inside
 * {@link MapFunction#map()} and invalid in
 * {@link #open(org.apache.flink.configuration.Configuration)} or
 * {@link #close()}.
 * 
 * @param <T>
 *            Type of the operator state
 */
public interface OperatorState<T> {

	/**
	 * Gets the current state for the operator. When the state is not
	 * partitioned the returned state is the same for all inputs. If state
	 * partitioning is applied the state returned depends on the current
	 * operator input, as the operator maintains an independent state for each
	 * partitions.
	 * 
	 * <p>
	 * {@link #getState()} returns <code>null</code> if there is no state stored
	 * in the operator. This is the expected behaviour before initializing the
	 * state with {@link #updateState(T)}.
	 * </p>
	 * 
	 * @return The operator state corresponding to the current input.
	 */
	T getState();

	/**
	 * Updates the operator state accessible by {@link #getState()} to the given
	 * value. The next time {@link #getState()} is called (for the same state
	 * partition) the returned state will represent the updated value.
	 * 
	 * @param state
	 *            The updated state.
	 */
	void updateState(T state);
	
}

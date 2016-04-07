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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;

import java.util.Map;

/**
 * A collection of utility methods for dealing with operator state.
 */
public class StateUtils {

	/**
	 * Utility method to define a common generic bound to be used for setting a
	 * generic state handle on a generic state carrier.
	 * 
	 * This has no impact on runtime, since internally, it performs unchecked
	 * casts. The purpose is merely to allow the use of generic interfaces
	 * without resorting to raw types, by giving the compiler a common type
	 * bound.
	 * 
	 * @param op
	 *            The state carrier operator.
	 * @param state
	 *            The state handle.
	 * @param keyGroupState
	 *            The key group state handle
	 * @param recoveryTimestamp
	 *            Global recovery timestamp
	 * @param <T>
	 *            Type bound for the
	 */
	public static <T extends StateHandle<?>, K extends StateHandle<?>> void setOperatorState(
		StatefulTask<?, ?> op,
		StateHandle<?> state,
		Map<Integer, StateHandle<?>> keyGroupState,
		long recoveryTimestamp) throws Exception {
		@SuppressWarnings("unchecked")
		StatefulTask<T, K> typedOp = (StatefulTask<T, K>) op;
		@SuppressWarnings("unchecked")
		T typedHandle = (T) state;
		@SuppressWarnings("unchecked")
		Map<Integer, K> typedKeyGroupState = (Map<Integer, K>) keyGroupState;

		typedOp.setInitialState(typedHandle, typedKeyGroupState, recoveryTimestamp);
	}

	// ------------------------------------------------------------------------

	/** Do not instantiate */
	private StateUtils() {
	}
}

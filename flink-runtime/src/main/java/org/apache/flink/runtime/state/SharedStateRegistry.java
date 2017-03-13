/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code SharedStateRegistry} will be deployed in the 
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator} to 
 * maintain the reference count of the {@link SharedStateHandle}s.
 */
public class SharedStateRegistry {

	/** All registered state objects */
	private final Map<String, Tuple2<SharedStateHandle, Integer>> registeredStates = new HashMap<>();

	/** All state objects that are not referenced any more */
	private List<StateObject> discardedStates = new ArrayList<>();

	/**
	 * Register the state in the registry
	 *
	 * @param state The state to register
	 */
	public void register(StateObject state) {
		synchronized (this) {
			Integer referenceCount = registeredStates.get(state);

			if (referenceCount != null) {
				registeredStates.put(state, referenceCount + 1);
			} else {
				registeredStates.put(state, 1);
			}
		}
	}

	/**
	 * Decrease the reference count of the state in the registry
	 *
	 * @param state The state to unregister
	 */
	public void unregister(StateObject state) {
		synchronized (this) {
			Integer referenceCount = registeredStates.get(state);

			if (referenceCount == null) {
				throw new IllegalStateException("Cannot unregister an unexisted state.");
			}

			referenceCount--;

			if (referenceCount == 0) {
				registeredStates.remove(state);
				discardedStates.add(state);
			}
		}
	}

	/**
	 * Gets and resets the list of discarded state objects
	 *
	 * @return A list of cached unreferenced state objects
	 */
	public List<StateObject> getAndResetDiscardedStates() {
		synchronized (this) {
			List<StateObject> result = new ArrayList<>(discardedStates);
			discardedStates.clear();
			return result;
		}
	}
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code SharedStateRegistry} will be deployed in the 
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator} to 
 * maintain the reference count of those state objects shared among different
 * checkpoints. Each shared state object must be identified by a unique key. 
 */
public class SharedStateRegistry implements Serializable {

	private static final long serialVersionUID = -8357254413007773970L;

	/** All registered state objects */
	private final Map<String, Tuple2<StateObject, Integer>> registeredStates = new HashMap<>();

	/** All state objects that are not referenced any more */
	private transient final List<StateObject> discardedStates = new ArrayList<>();

	/**
	 * Register the state in the registry
	 *
	 * @param key The key of the state to register
	 * @param state The state to register
	 */
	public void register(String key, StateObject state) {
		Tuple2<StateObject, Integer> stateAndRefCnt = registeredStates.get(key);

		if (stateAndRefCnt == null) {
			registeredStates.put(key, new Tuple2<>(state, 1));
		} else {
			if (!stateAndRefCnt.f0.equals(state)) {
				throw new IllegalStateException("Cannot register a key with different states.");
			}

			stateAndRefCnt.f1++;
		}
	}

	/**
	 * Decrease the reference count of the state in the registry
	 *
	 * @param key The key of the state to unregister
	 */
	public void unregister(String key) {
		Tuple2<StateObject, Integer> stateAndRefCnt = registeredStates.get(key);

		if (stateAndRefCnt == null) {
			throw new IllegalStateException("Cannot unregister an unexisted state.");
		}

		stateAndRefCnt.f1--;

		// Remove the state from the registry when it's not referenced any more.
		if (stateAndRefCnt.f1 == 0) {
			registeredStates.remove(key);
			discardedStates.add(stateAndRefCnt.f0);
		}
	}

	/**
	 * Register all the shared states in the given state handles.
	 * 
	 * @param stateHandles The state handles to register their shared states
	 */
	public void registerAll(Collection<? extends CompositeStateHandle> stateHandles) {
		synchronized (this) {
			if (stateHandles != null) {
				for (CompositeStateHandle stateHandle : stateHandles) {
					stateHandle.register(this);
				}
			}
		}
	}
	
	/**
	 * Register all the shared states in the given state handle.
	 * 
	 * @param stateHandle The state handle to register its shared states
	 */
	public void registerAll(CompositeStateHandle stateHandle) {
		if (stateHandle != null) {
			synchronized (this) {
				stateHandle.register(this);
			}
		}
	}

	/**
	 * Unregister all the shared states in the given state handles and return
	 * those unreferenced states after these shared states are unregistered.
	 * 
	 * @param stateHandles The state handles to unregister their shared states
	 * @return The states that are not referenced any more
	 */
	public List<StateObject> unregisterAll(Collection<? extends CompositeStateHandle> stateHandles) {
		synchronized (this) {
			discardedStates.clear();

			if (stateHandles != null) {
				for (CompositeStateHandle stateHandle : stateHandles) {
					stateHandle.unregister(this);
				}
			}

			return discardedStates;
		}

	}

	/**
	 * Unregister all the shared states in the given state handles and return
	 * those unreferenced states after these shared states are unregistered.
	 *
	 * @param stateHandle The state handle to unregister its shared states
	 * @return The states that are not referenced any more
	 */
	public List<StateObject> unregisterAll(CompositeStateHandle stateHandle) {
		if (stateHandle == null) {
			return null;
		} else {
			synchronized (this) {
				discardedStates.clear();

				stateHandle.unregister(this);

				return discardedStates;
			}
		}
	}

	@VisibleForTesting
	int getReferenceCount(String key) {
		Tuple2<StateObject, Integer> stateAndRefCnt = registeredStates.get(key);

		return stateAndRefCnt == null ? 0 : stateAndRefCnt.f1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SharedStateRegistry that = (SharedStateRegistry) o;

		return registeredStates.equals(that.registeredStates);
	}

	@Override
	public int hashCode() {
		int result = registeredStates.hashCode();
		result = 31 * result + discardedStates.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "SharedStateRegistry{" + "registeredStates=" + registeredStates +
			", discardedStates=" + discardedStates + '}';
	}
}

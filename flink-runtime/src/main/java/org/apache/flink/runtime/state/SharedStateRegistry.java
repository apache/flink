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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code SharedStateRegistry} will be deployed in the 
 * {@link org.apache.flink.runtime.checkpoint.CheckpointCoordinator} to 
 * maintain the reference count of {@link SharedStateHandle}s which are shared
 * among different checkpoints.
 */
public class SharedStateRegistry implements Serializable {

	private static Logger LOG = LoggerFactory.getLogger(SharedStateRegistry.class);

	private static final long serialVersionUID = -8357254413007773970L;

	/** All registered state objects */
	private final Map<String, SharedStateEntry> registeredStates = new HashMap<>();

	/**
	 * Register the state in the registry
	 *
	 * @param state The state to register
	 * @param isNew True if the shared state is newly created
	 */
	public void register(SharedStateHandle state, boolean isNew) {
		if (state == null) {
			return;
		}

		synchronized (registeredStates) {
			SharedStateEntry entry = registeredStates.get(state.getKey());

			if (isNew) {
				Preconditions.checkState(entry == null,
					"The state cannot be created more than once.");

				registeredStates.put(state.getKey(), new SharedStateEntry(state));
			} else {
				Preconditions.checkState(entry != null,
					"The state cannot be referenced if it has not been created yet.");

				entry.increaseReferenceCount();
			}
		}
	}

	/**
	 * Unregister the state in the registry
	 *
	 * @param state The state to unregister
	 */
	public void unregister(SharedStateHandle state) {
		if (state == null) {
			return;
		}

		synchronized (registeredStates) {
			SharedStateEntry entry = registeredStates.get(state.getKey());

			if (entry == null) {
				throw new IllegalStateException("Cannot unregister an unexisted state.");
			}

			entry.decreaseReferenceCount();

			// Remove the state from the registry when it's not referenced any more.
			if (entry.getReferenceCount() == 0) {
				registeredStates.remove(state.getKey());

				try {
					entry.getState().discardState();
				} catch (Exception e) {
					LOG.warn("Cannot properly discard the state " + entry.getState() + ".", e);
				}
			}
		}
	}

	/**
	 * Register given shared states in the registry.
	 *
	 * @param stateHandles The shared states to register.
	 */
	public void registerAll(Collection<? extends CompositeStateHandle> stateHandles) {
		if (stateHandles == null) {
			return;
		}

		synchronized (registeredStates) {
			for (CompositeStateHandle stateHandle : stateHandles) {
				stateHandle.registerSharedStates(this);
			}
		}
	}



	/**
	 * Unregister all the shared states referenced by the given.
	 *
	 * @param stateHandles The shared states to unregister.
	 */
	public void unregisterAll(Collection<? extends CompositeStateHandle> stateHandles) {
		if (stateHandles == null) {
			return;
		}

		synchronized (registeredStates) {
			for (CompositeStateHandle stateHandle : stateHandles) {
				stateHandle.unregisterSharedStates(this);
			}
		}
	}

	private static class SharedStateEntry {
		/** The shared object */
		private final SharedStateHandle state;

		/** The reference count of the object */
		private int referenceCount;

		SharedStateEntry(SharedStateHandle value) {
			this.state = value;
			this.referenceCount = 1;
		}

		SharedStateHandle getState() {
			return state;
		}

		int getReferenceCount() {
			return referenceCount;
		}

		void increaseReferenceCount() {
			++referenceCount;
		}

		void decreaseReferenceCount() {
			--referenceCount;
		}
	}


	@VisibleForTesting
	public int getReferenceCount(SharedStateHandle state) {
		SharedStateEntry entry = registeredStates.get(state.getKey());

		return entry == null ? 0 : entry.getReferenceCount();
	}
}

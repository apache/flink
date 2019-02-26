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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An implementation of {@link KeyedStateHandle} which will be used in incremental snapshot/restore.
 */
public class IncrementalKeyedStateSnapshot implements KeyedStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(IncrementalKeyedStateHandle.class);

	private static final long serialVersionUID = 1L;

	/**
	 * The checkpoint Id.
	 */
	private final long checkpointId;

	/**
	 * Shared state in the incremental checkpoint.
	 */
	private final Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedState;

	/**
	 * Private state in the incremental checkpoint.
	 */
	private final Map<StateHandleID, StreamStateHandle> privateState;

	/**
	 * The key-group range covered by this state handle.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * Primary meta data state of the incremental checkpoint.
	 */
	@Nonnull
	private final StreamStateHandle metaStateHandle;

	/**
	 * Once the shared states are registered, it is the {@link SharedStateRegistry}'s
	 * responsibility to cleanup those shared states.
	 * But in the cases where the state handle is discarded before performing the registration,
	 * the handle should delete all the shared states created by it.
	 *
	 * <p>his variable is not null iff the handles was registered.
	 */
	private transient SharedStateRegistry sharedStateRegistry;

	public IncrementalKeyedStateSnapshot(
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle) {

		this.keyGroupRange = keyGroupRange;
		this.checkpointId = checkpointId;
		this.sharedState = sharedState;
		this.privateState = privateState;
		this.sharedStateRegistry = null;
		this.metaStateHandle = metaStateHandle;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public Map<StateHandleID, Tuple2<String, StreamStateHandle>> getSharedState() {
		return sharedState;
	}

	public Map<StateHandleID, StreamStateHandle> getPrivateState() {
		return privateState;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Override
	public KeyedStateHandle getIntersection(KeyGroupRange otherKeyGroupRange) {
		if (keyGroupRange.getIntersection(otherKeyGroupRange).getNumberOfKeyGroups() > 0) {
			return this;
		} else {
			return new IncrementalKeyedStateSnapshot(
				KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
				this.checkpointId,
				this.sharedState,
				this.privateState,
				this.metaStateHandle);
		}
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {

		// This is a quick check to avoid that we register twice with the same registry. However, the code allows to
		// register again with a different registry. The implication is that ownership is transferred to this new
		// registry. This should only happen in case of a restart, when the CheckpointCoordinator creates a new
		// SharedStateRegistry for the current attempt and the old registry becomes meaningless. We also assume that
		// an old registry object from a previous run is due to be GCed and will never be used for registration again.
		Preconditions.checkState(
			sharedStateRegistry != stateRegistry,
			"The state handle has already registered its shared states to the given registry.");

		sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

		LOG.trace("Registering IncrementalKeyedStateSnapshot for checkpoint {} from backend.", checkpointId);

		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandle : sharedState.entrySet()) {
			String uniqueId = sharedStateHandle.getValue().f0;
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromUniqueId(uniqueId, sharedStateHandle.getKey());

			SharedStateRegistry.Result result =
				stateRegistry.registerReference(registryKey, sharedStateHandle.getValue().f1);

			// This step consolidates our shared handles with the registry, which does two things:
			//
			// 1) Replace placeholder state handle with already registered, actual state handles.
			//
			// 2) Deduplicate re-uploads of incremental state due to missing confirmations about
			// completed checkpoints.
			//
			// This prevents the following problem:
			// A previous checkpoint n has already registered the state. This can happen if a
			// following checkpoint (n + x) wants to reference the same state before the backend got
			// notified that checkpoint n completed. In this case, the shared registry did
			// deduplication and returns the previous reference.
			sharedStateHandle.setValue(Tuple2.of(uniqueId, result.getReference()));
		}
	}

	@Override
	public void discardState() throws Exception {
		SharedStateRegistry registry = this.sharedStateRegistry;
		final boolean isRegistered = (registry != null);

		LOG.trace("Discarding IncrementalKeyedStateSnapshot (registered = {}) for checkpoint {} from backend.",
			isRegistered,
			checkpointId);

		try {
			metaStateHandle.discardState();
		} catch (Exception e) {
			LOG.warn("Could not properly discard meta data.", e);
		}

		try {
			StateUtil.bestEffortDiscardAllStateObjects(privateState.values());
		} catch (Exception e) {
			LOG.warn("Could not properly discard misc file states.", e);
		}

		// If this was not registered, we can delete the shared state. We can simply apply this
		// to all handles, because all handles that have not been created for the first time for this
		// are only placeholders at this point (disposing them is a NOP).
		if (isRegistered) {
			// If this was registered, we only unregister all our referenced shared states
			// from the registry.
			for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : sharedState.entrySet()) {
				registry.unregisterReference(
					createSharedStateRegistryKeyFromUniqueId(entry.getValue().f0, entry.getKey()));
			}
		} else {
			// Otherwise, we assume to own those handles and dispose them directly.
			try {
				StateUtil.bestEffortDiscardAllStateObjects(sharedState.values().stream().map(t -> t.f1).collect(Collectors.toSet()));
			} catch (Exception e) {
				LOG.warn("Could not properly discard new sst file states.", e);
			}
		}
	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (Tuple2<String, StreamStateHandle> sharedStateHandle : sharedState.values()) {
			size += sharedStateHandle.f1.getStateSize();
		}

		for (StreamStateHandle privateStateHandle : privateState.values()) {
			size += privateStateHandle.getStateSize();
		}

		return size;
	}

	@Nonnull
	public StreamStateHandle getMetaStateHandle() {
		return metaStateHandle;
	}

	/**
	 * Create a unique key to register one of our shared state handles.
	 */
	@VisibleForTesting
	public SharedStateRegistryKey createSharedStateRegistryKeyFromUniqueId(String uniqueId, StateHandleID shId) {
		return new SharedStateRegistryKey(uniqueId, shId);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		IncrementalKeyedStateSnapshot that = (IncrementalKeyedStateSnapshot) o;

		if (getCheckpointId() != that.getCheckpointId()) {
			return false;
		}
		if (!keyGroupRange.equals(that.getKeyGroupRange())) {
			return false;
		}
		if (!getSharedState().equals(that.getSharedState())) {
			return false;
		}
		if (!getPrivateState().equals(that.getPrivateState())) {
			return false;
		}
		return getMetaStateHandle().equals(that.getMetaStateHandle());
	}

	@Override
	public int hashCode() {
		int result = getKeyGroupRange().hashCode();
		result = 31 * result + (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
		result = 31 * result + getSharedState().hashCode();
		result = 31 * result + getPrivateState().hashCode();
		result = 31 * result + getMetaStateHandle().hashCode();
		return result;
	}
}

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
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The handle to states of an incremental snapshot.
 * <p>
 * The states contained in an incremental snapshot include:
 * <ul>
 * <li> Created shared state which includes shared files produced since the last
 * completed checkpoint. These files can be referenced by succeeding checkpoints if the
 * checkpoint succeeds to complete. </li>
 * <li> Referenced shared state which includes the shared files materialized in previous
 * checkpoints. Until we this is registered to a {@link SharedStateRegistry}, all referenced
 * shared state handles are only placeholders, so that we do not send state handles twice
 * from which we know that they already exist on the checkpoint coordinator.</li>
 * <li> Private state which includes all other files, typically mutable, that cannot be shared by
 * other checkpoints. </li>
 * <li> Backend meta state which includes the information of existing states. </li>
 * </ul>
 *
 * When this should become a completed checkpoint on the checkpoint coordinator, it must first be
 * registered with a {@link SharedStateRegistry}, so that all placeholder state handles to
 * previously existing state are replaced with the originals.
 *
 * IMPORTANT: This class currently overrides equals and hash code only for testing purposes. They
 * should not be called from production code. This means this class is also not suited to serve as
 * a key, e.g. in hash maps.
 */
public class IncrementalKeyedStateHandle implements KeyedStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(IncrementalKeyedStateHandle.class);

	private static final long serialVersionUID = -8328808513197388231L;

	/**
	 * The operator instance identifier for this handle
	 */
	private final String operatorIdentifier;

	/**
	 * The key-group range covered by this state handle
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The checkpoint Id
	 */
	private final long checkpointId;

	/**
	 * Shared state in the incremental checkpoint. This i
	 */
	private final Map<StateHandleID, StreamStateHandle> sharedState;

	/**
	 * Private state in the incremental checkpoint
	 */
	private final Map<StateHandleID, StreamStateHandle> privateState;

	/**
	 * Primary meta data state of the incremental checkpoint
	 */
	private final StreamStateHandle metaStateHandle;

	/**
	 * Once the shared states are registered, it is the {@link SharedStateRegistry}'s
	 * responsibility to cleanup those shared states.
	 * But in the cases where the state handle is discarded before performing the registration,
	 * the handle should delete all the shared states created by it.
	 *
	 * This variable is not null iff the handles was registered.
	 */
	private transient SharedStateRegistry sharedStateRegistry;

	public IncrementalKeyedStateHandle(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> sharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle) {

		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.checkpointId = checkpointId;
		this.sharedState = Preconditions.checkNotNull(sharedState);
		this.privateState = Preconditions.checkNotNull(privateState);
		this.metaStateHandle = Preconditions.checkNotNull(metaStateHandle);
		this.sharedStateRegistry = null;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public Map<StateHandleID, StreamStateHandle> getSharedState() {
		return sharedState;
	}

	public Map<StateHandleID, StreamStateHandle> getPrivateState() {
		return privateState;
	}

	public StreamStateHandle getMetaStateHandle() {
		return metaStateHandle;
	}

	public String getOperatorIdentifier() {
		return operatorIdentifier;
	}

	@Override
	public KeyedStateHandle getIntersection(KeyGroupRange keyGroupRange) {
		if (this.keyGroupRange.getIntersection(keyGroupRange) != KeyGroupRange.EMPTY_KEY_GROUP_RANGE) {
			return this;
		} else {
			return null;
		}
	}

	@Override
	public void discardState() throws Exception {

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
		if (sharedStateRegistry == null) {
			try {
				StateUtil.bestEffortDiscardAllStateObjects(sharedState.values());
			} catch (Exception e) {
				LOG.warn("Could not properly discard new sst file states.", e);
			}
		} else {
			// If this was registered, we only unregister all our referenced shared states
			// from the registry.
			for (StateHandleID stateHandleID : sharedState.keySet()) {
				sharedStateRegistry.unregisterReference(
					createSharedStateRegistryKeyFromFileName(stateHandleID));
			}
		}
	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (StreamStateHandle sharedStateHandle : sharedState.values()) {
			size += sharedStateHandle.getStateSize();
		}

		for (StreamStateHandle privateStateHandle : privateState.values()) {
			size += privateStateHandle.getStateSize();
		}

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {

		Preconditions.checkState(sharedStateRegistry == null, "The state handle has already registered its shared states.");

		sharedStateRegistry = Preconditions.checkNotNull(stateRegistry);

		for (Map.Entry<StateHandleID, StreamStateHandle> sharedStateHandle : sharedState.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(sharedStateHandle.getKey());

			SharedStateRegistry.Result result =
				stateRegistry.registerReference(registryKey, sharedStateHandle.getValue());

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
			sharedStateHandle.setValue(result.getReference());
		}
	}

	/**
	 * Create a unique key to register one of our shared state handles.
	 */
	@VisibleForTesting
	public SharedStateRegistryKey createSharedStateRegistryKeyFromFileName(StateHandleID shId) {
		return new SharedStateRegistryKey(operatorIdentifier + '-' + keyGroupRange, shId);
	}

	/**
	 * This method is should only be called in tests! This should never serve as key in a hash map.
	 */
	@VisibleForTesting
	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		IncrementalKeyedStateHandle that = (IncrementalKeyedStateHandle) o;

		if (getCheckpointId() != that.getCheckpointId()) {
			return false;
		}
		if (!getOperatorIdentifier().equals(that.getOperatorIdentifier())) {
			return false;
		}
		if (!getKeyGroupRange().equals(that.getKeyGroupRange())) {
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

	/**
	 * This method should only be called in tests! This should never serve as key in a hash map.
	 */
	@VisibleForTesting
	@Override
	public int hashCode() {
		int result = getOperatorIdentifier().hashCode();
		result = 31 * result + getKeyGroupRange().hashCode();
		result = 31 * result + (int) (getCheckpointId() ^ (getCheckpointId() >>> 32));
		result = 31 * result + getSharedState().hashCode();
		result = 31 * result + getPrivateState().hashCode();
		result = 31 * result + getMetaStateHandle().hashCode();
		return result;
	}
}


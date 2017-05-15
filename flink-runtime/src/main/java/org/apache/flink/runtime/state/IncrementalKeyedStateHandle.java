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
 * The states contained in an incremental snapshot include
 * <ul>
 * <li> Created shared state which includes (the supposed to be) shared files produced since the last
 * completed checkpoint. These files can be referenced by succeeding checkpoints if the
 * checkpoint succeeds to complete. </li>
 * <li> Referenced shared state which includes the shared files materialized in previous
 * checkpoints. </li>
 * <li> Private state which includes all other files, typically mutable, that cannot be shared by
 * other checkpoints. </li>
 * <li> Backend meta state which includes the information of existing states. </li>
 * </ul>
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
	 * State that the incremental checkpoint created new
	 */
	private final Map<StateHandleID, StreamStateHandle> createdSharedState;

	/**
	 * State that the incremental checkpoint references from previous checkpoints
	 */
	private final Map<StateHandleID, StreamStateHandle> referencedSharedState;

	/**
	 * Private state in the incremental checkpoint
	 */
	private final Map<StateHandleID, StreamStateHandle> privateState;

	/**
	 * Primary meta data state of the incremental checkpoint
	 */
	private final StreamStateHandle metaStateHandle;

	/**
	 * True if the state handle has already registered shared states.
	 * <p>
	 * Once the shared states are registered, it's the {@link SharedStateRegistry}'s
	 * responsibility to maintain the shared states. But in the cases where the
	 * state handle is discarded before performing the registration, the handle
	 * should delete all the shared states created by it.
	 */
	private boolean registered;

	public IncrementalKeyedStateHandle(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		long checkpointId,
		Map<StateHandleID, StreamStateHandle> createdSharedState,
		Map<StateHandleID, StreamStateHandle> referencedSharedState,
		Map<StateHandleID, StreamStateHandle> privateState,
		StreamStateHandle metaStateHandle) {

		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.checkpointId = checkpointId;
		this.createdSharedState = Preconditions.checkNotNull(createdSharedState);
		this.referencedSharedState = Preconditions.checkNotNull(referencedSharedState);
		this.privateState = Preconditions.checkNotNull(privateState);
		this.metaStateHandle = Preconditions.checkNotNull(metaStateHandle);
		this.registered = false;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	public Map<StateHandleID, StreamStateHandle> getCreatedSharedState() {
		return createdSharedState;
	}

	public Map<StateHandleID, StreamStateHandle> getReferencedSharedState() {
		return referencedSharedState;
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

		Preconditions.checkState(!registered, "Attempt to dispose a registered composite state with registered shared state. Must unregister first.");

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

		try {
			StateUtil.bestEffortDiscardAllStateObjects(createdSharedState.values());
		} catch (Exception e) {
			LOG.warn("Could not properly discard new sst file states.", e);
		}

	}

	@Override
	public long getStateSize() {
		long size = getPrivateStateSize();

		for (StreamStateHandle oldSstFileHandle : referencedSharedState.values()) {
			size += oldSstFileHandle.getStateSize();
		}

		return size;
	}

	/**
	 * Returns the size of the state that is privately owned by this handle.
	 */
	public long getPrivateStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (StreamStateHandle newSstFileHandle : createdSharedState.values()) {
			size += newSstFileHandle.getStateSize();
		}

		for (StreamStateHandle miscFileHandle : privateState.values()) {
			size += miscFileHandle.getStateSize();
		}

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {

		Preconditions.checkState(!registered, "The state handle has already registered its shared states.");

		for (Map.Entry<StateHandleID, StreamStateHandle> newSstFileEntry : createdSharedState.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(newSstFileEntry.getKey());

			SharedStateRegistry.Result result =
				stateRegistry.registerNewReference(registryKey, newSstFileEntry.getValue());

			// We update our reference with the result from the registry, to prevent the following
			// problem:
			// A previous checkpoint n has already registered the state. This can happen if a
			// following checkpoint (n + x) wants to reference the same state before the backend got
			// notified that checkpoint n completed. In this case, the shared registry did
			// deduplication and returns the previous reference.
			newSstFileEntry.setValue(result.getReference());
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> oldSstFileName : referencedSharedState.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(oldSstFileName.getKey());

			SharedStateRegistry.Result result = stateRegistry.obtainReference(registryKey);

			// Again we update our state handle with the result from the registry, thus replacing
			// placeholder state handles with the originals.
			oldSstFileName.setValue(result.getReference());
		}

		// Migrate state from unregistered to registered, so that it will not count as private state
		// for #discardState() from now.
		referencedSharedState.putAll(createdSharedState);
		createdSharedState.clear();

		registered = true;
	}

	@Override
	public void unregisterSharedStates(SharedStateRegistry stateRegistry) {

		Preconditions.checkState(registered, "The state handle has not registered its shared states yet.");

		for (Map.Entry<StateHandleID, StreamStateHandle> newSstFileEntry : createdSharedState.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(newSstFileEntry.getKey());
			stateRegistry.releaseReference(registryKey);
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> oldSstFileEntry : referencedSharedState.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(oldSstFileEntry.getKey());
			stateRegistry.releaseReference(registryKey);
		}

		registered = false;
	}

	private SharedStateRegistryKey createSharedStateRegistryKeyFromFileName(StateHandleID shId) {
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
		if (!getCreatedSharedState().equals(that.getCreatedSharedState())) {
			return false;
		}
		if (!getReferencedSharedState().equals(that.getReferencedSharedState())) {
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
		result = 31 * result + getCreatedSharedState().hashCode();
		result = 31 * result + getReferencedSharedState().hashCode();
		result = 31 * result + getPrivateState().hashCode();
		result = 31 * result + getMetaStateHandle().hashCode();
		return result;
	}
}


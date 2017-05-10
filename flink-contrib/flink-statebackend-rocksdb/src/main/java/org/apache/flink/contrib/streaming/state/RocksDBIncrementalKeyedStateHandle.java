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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryKey;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The handle to states in incremental snapshots taken by {@link RocksDBKeyedStateBackend}.
 *
 * The states contained in an incremental snapshot include
 * <ul>
 * <li> New SST state which includes the sst files produced since the last completed
 *   checkpoint. These files can be referenced by succeeding checkpoints if the
 *   checkpoint succeeds to complete. </li>
 * <li> Old SST state which includes the sst files materialized in previous
 *   checkpoints. </li>
 * <li> MISC state which include the other files in the RocksDB instance, e.g. the
 *   LOG and MANIFEST files. These files are mutable, hence cannot be shared by
 *   other checkpoints. </li>
 * <li> Meta state which includes the information of existing states. </li>
 * </ul>
 */
public class RocksDBIncrementalKeyedStateHandle implements KeyedStateHandle, CompositeStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalKeyedStateHandle.class);

	private static final long serialVersionUID = -8328808513197388231L;

	private final String operatorIdentifier;

	private final KeyGroupRange keyGroupRange;

	private final long checkpointId;

	private final Map<String, StreamStateHandle> unregisteredSstFiles;

	private final Map<String, StreamStateHandle> registeredSstFiles;

	private final Map<String, StreamStateHandle> miscFiles;

	private final StreamStateHandle metaStateHandle;

	/**
	 * True if the state handle has already registered shared states.
	 *
	 * Once the shared states are registered, it's the {@link SharedStateRegistry}'s
	 * responsibility to maintain the shared states. But in the cases where the
	 * state handle is discarded before performing the registration, the handle
	 * should delete all the shared states created by it.
	 */
	private boolean registered;

	RocksDBIncrementalKeyedStateHandle(
			String operatorIdentifier,
			KeyGroupRange keyGroupRange,
			long checkpointId,
			Map<String, StreamStateHandle> unregisteredSstFiles,
			Map<String, StreamStateHandle> registeredSstFiles,
			Map<String, StreamStateHandle> miscFiles,
			StreamStateHandle metaStateHandle) {

		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.checkpointId = checkpointId;
		this.unregisteredSstFiles = Preconditions.checkNotNull(unregisteredSstFiles);
		this.registeredSstFiles = Preconditions.checkNotNull(registeredSstFiles);
		this.miscFiles = Preconditions.checkNotNull(miscFiles);
		this.metaStateHandle = Preconditions.checkNotNull(metaStateHandle);
		this.registered = false;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	long getCheckpointId() {
		return checkpointId;
	}

	Map<String, StreamStateHandle> getUnregisteredSstFiles() {
		return unregisteredSstFiles;
	}

	Map<String, StreamStateHandle> getRegisteredSstFiles() {
		return registeredSstFiles;
	}

	Map<String, StreamStateHandle> getMiscFiles() {
		return miscFiles;
	}

	StreamStateHandle getMetaStateHandle() {
		return metaStateHandle;
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
			StateUtil.bestEffortDiscardAllStateObjects(miscFiles.values());
		} catch (Exception e) {
			LOG.warn("Could not properly discard misc file states.", e);
		}

		try {
			StateUtil.bestEffortDiscardAllStateObjects(unregisteredSstFiles.values());
		} catch (Exception e) {
			LOG.warn("Could not properly discard new sst file states.", e);
		}

	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (StreamStateHandle newSstFileHandle : unregisteredSstFiles.values()) {
			size += newSstFileHandle.getStateSize();
		}

		for (StreamStateHandle oldSstFileHandle : registeredSstFiles.values()) {
			size += oldSstFileHandle.getStateSize();
		}

		for (StreamStateHandle miscFileHandle : miscFiles.values()) {
			size += miscFileHandle.getStateSize();
		}

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {

		Preconditions.checkState(!registered, "The state handle has already registered its shared states.");

		for (Map.Entry<String, StreamStateHandle> newSstFileEntry : unregisteredSstFiles.entrySet()) {
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

		for (Map.Entry<String, StreamStateHandle> oldSstFileName : registeredSstFiles.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(oldSstFileName.getKey());

			SharedStateRegistry.Result result = stateRegistry.obtainReference(registryKey);

			// Again we update our state handle with the result from the registry, thus replacing
			// placeholder state handles with the originals.
			oldSstFileName.setValue(result.getReference());
		}

		// Migrate state from unregistered to registered, so that it will not count as private state
		// for #discardState() from now.
		registeredSstFiles.putAll(unregisteredSstFiles);
		unregisteredSstFiles.clear();

		registered = true;
	}

	@Override
	public void unregisterSharedStates(SharedStateRegistry stateRegistry) {

		Preconditions.checkState(registered, "The state handle has not registered its shared states yet.");

		for (Map.Entry<String, StreamStateHandle> newSstFileEntry : unregisteredSstFiles.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(newSstFileEntry.getKey());
			stateRegistry.releaseReference(registryKey);
		}

		for (Map.Entry<String, StreamStateHandle> oldSstFileEntry : registeredSstFiles.entrySet()) {
			SharedStateRegistryKey registryKey =
				createSharedStateRegistryKeyFromFileName(oldSstFileEntry.getKey());
			stateRegistry.releaseReference(registryKey);
		}

		registered = false;
	}

	private SharedStateRegistryKey createSharedStateRegistryKeyFromFileName(String fileName) {
		return new SharedStateRegistryKey(operatorIdentifier + "-" + keyGroupRange + "-" + fileName);
	}
}


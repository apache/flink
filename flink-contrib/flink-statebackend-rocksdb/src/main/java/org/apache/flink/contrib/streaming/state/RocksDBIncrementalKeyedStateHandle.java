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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.state.CompositeStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SharedStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
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

	private final JobID jobId;

	private final String operatorIdentifier;

	private final KeyGroupRange keyGroupRange;

	private final long checkpointId;

	private final Map<String, StreamStateHandle> newSstFiles;

	private final Map<String, StreamStateHandle> oldSstFiles;

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
			JobID jobId,
			String operatorIdentifier,
			KeyGroupRange keyGroupRange,
			long checkpointId,
			Map<String, StreamStateHandle> newSstFiles,
			Map<String, StreamStateHandle> oldSstFiles,
			Map<String, StreamStateHandle> miscFiles,
			StreamStateHandle metaStateHandle) {

		this.jobId = Preconditions.checkNotNull(jobId);
		this.operatorIdentifier = Preconditions.checkNotNull(operatorIdentifier);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
		this.checkpointId = checkpointId;
		this.newSstFiles = Preconditions.checkNotNull(newSstFiles);
		this.oldSstFiles = Preconditions.checkNotNull(oldSstFiles);
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

	Map<String, StreamStateHandle> getNewSstFiles() {
		return newSstFiles;
	}

	Map<String, StreamStateHandle> getOldSstFiles() {
		return oldSstFiles;
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

		if (!registered) {
			try {
				StateUtil.bestEffortDiscardAllStateObjects(newSstFiles.values());
			} catch (Exception e) {
				LOG.warn("Could not properly discard new sst file states.", e);
			}
		}
	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (StreamStateHandle newSstFileHandle : newSstFiles.values()) {
			size += newSstFileHandle.getStateSize();
		}

		for (StreamStateHandle oldSstFileHandle : oldSstFiles.values()) {
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

		for (Map.Entry<String, StreamStateHandle> newSstFileEntry : newSstFiles.entrySet()) {
			SstFileStateHandle stateHandle = new SstFileStateHandle(newSstFileEntry.getKey(), newSstFileEntry.getValue());

			int referenceCount = stateRegistry.register(stateHandle);
			Preconditions.checkState(referenceCount == 1);
		}

		for (Map.Entry<String, StreamStateHandle> oldSstFileEntry : oldSstFiles.entrySet()) {
			SstFileStateHandle stateHandle = new SstFileStateHandle(oldSstFileEntry.getKey(), oldSstFileEntry.getValue());

			int referenceCount = stateRegistry.register(stateHandle);
			Preconditions.checkState(referenceCount > 1);
		}

		registered = true;
	}

	@Override
	public void unregisterSharedStates(SharedStateRegistry stateRegistry) {
		Preconditions.checkState(registered, "The state handle has not registered its shared states yet.");

		for (Map.Entry<String, StreamStateHandle> newSstFileEntry : newSstFiles.entrySet()) {
			stateRegistry.unregister(new SstFileStateHandle(newSstFileEntry.getKey(), newSstFileEntry.getValue()));
		}

		for (Map.Entry<String, StreamStateHandle> oldSstFileEntry : oldSstFiles.entrySet()) {
			stateRegistry.unregister(new SstFileStateHandle(oldSstFileEntry.getKey(), oldSstFileEntry.getValue()));
		}

		registered = false;
	}

	private class SstFileStateHandle implements SharedStateHandle {

		private static final long serialVersionUID = 9092049285789170669L;

		private final String fileName;

		private final StreamStateHandle delegateStateHandle;

		private SstFileStateHandle(
				String fileName,
				StreamStateHandle delegateStateHandle) {
			this.fileName = fileName;
			this.delegateStateHandle = delegateStateHandle;
		}

		@Override
		public String getRegistrationKey() {
			return jobId + "-" + operatorIdentifier + "-" + keyGroupRange + "-" + fileName;
		}

		@Override
		public void discardState() throws Exception {
			delegateStateHandle.discardState();
		}

		@Override
		public long getStateSize() {
			return delegateStateHandle.getStateSize();
		}
	}
}


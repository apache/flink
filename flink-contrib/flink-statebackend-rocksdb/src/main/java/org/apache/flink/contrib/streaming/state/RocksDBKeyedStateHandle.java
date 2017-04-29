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
import java.util.Set;

/**
 * The handle to states in incremental snapshots taken by {@link RocksDBKeyedStateBackend}
 */
public class RocksDBKeyedStateHandle implements KeyedStateHandle, CompositeStateHandle {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBKeyedStateHandle.class);

	private static final long serialVersionUID = -8328808513197388231L;

	private final JobID jobId;

	private final String operatorIdentifier;

	private final KeyGroupRange keyGroupRange;

	private final Set<String> newSstFileNames;

	private final Map<String, StreamStateHandle> sstFiles;

	private final Map<String, StreamStateHandle> miscFiles;

	private final StreamStateHandle metaStateHandle;

	private boolean registered;

	RocksDBKeyedStateHandle(
			JobID jobId,
			String operatorIdentifier,
			KeyGroupRange keyGroupRange,
			Set<String> newSstFileNames,
			Map<String, StreamStateHandle> sstFiles,
			Map<String, StreamStateHandle> miscFiles,
			StreamStateHandle metaStateHandle) {

		this.jobId = jobId;
		this.operatorIdentifier = operatorIdentifier;
		this.keyGroupRange = keyGroupRange;
		this.newSstFileNames = newSstFileNames;
		this.sstFiles = sstFiles;
		this.miscFiles = miscFiles;
		this.metaStateHandle = metaStateHandle;
		this.registered = false;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	public Map<String, StreamStateHandle> getSstFiles() {
		return sstFiles;
	}

	public Map<String, StreamStateHandle> getMiscFiles() {
		return miscFiles;
	}

	public StreamStateHandle getMetaStateHandle() {
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
			LOG.warn("Could not properly discard misc file state.", e);
		}

		if (!registered) {
			for (String newSstFileName : newSstFileNames) {
				StreamStateHandle handle = sstFiles.get(newSstFileName);
				try {
					handle.discardState();
				} catch (Exception e) {
					LOG.warn("Could not properly discard sst file state", e);
				}
			}
		}
	}

	@Override
	public long getStateSize() {
		long size = StateUtil.getStateSize(metaStateHandle);

		for (StreamStateHandle sstFileHandle : sstFiles.values()) {
			size += sstFileHandle.getStateSize();
		}

		for (StreamStateHandle miscFileHandle : miscFiles.values()) {
			size += miscFileHandle.getStateSize();
		}

		return size;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		Preconditions.checkState(!registered, "The state handle has already registered its shared states.");

		for (Map.Entry<String, StreamStateHandle> sstFileEntry : sstFiles.entrySet()) {
			SstFileStateHandle stateHandle = new SstFileStateHandle(sstFileEntry.getKey(), sstFileEntry.getValue());

			int referenceCount = stateRegistry.register(stateHandle);

			if (newSstFileNames.contains(sstFileEntry.getKey())) {
				Preconditions.checkState(referenceCount == 1);
			} else {
				Preconditions.checkState(referenceCount > 1);
			}
		}

		registered = true;
	}

	@Override
	public void unregisterSharedStates(SharedStateRegistry stateRegistry) {
		Preconditions.checkState(registered, "The state handle has not registered its shared states yet.");

		for (Map.Entry<String, StreamStateHandle> sstFileEntry : sstFiles.entrySet()) {
			stateRegistry.unregister(new SstFileStateHandle(sstFileEntry.getKey(), sstFileEntry.getValue()));
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


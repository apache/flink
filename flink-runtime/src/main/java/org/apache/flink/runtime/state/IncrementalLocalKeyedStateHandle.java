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

import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.Set;
import java.util.UUID;

/**
 * State handle for local copies of {@link IncrementalKeyedStateHandle}. Consists of a {@link DirectoryStateHandle} that
 * represents the directory of the native RocksDB snapshot, the key groups, and a stream state handle for Flink's state
 * meta data file.
 */
public class IncrementalLocalKeyedStateHandle extends DirectoryKeyedStateHandle {

	private static final long serialVersionUID = 1L;

	/** Id of the checkpoint that created this state handle. */
	@Nonnegative
	private final long checkpointId;

	/** UUID to identify the backend which created this state handle. */
	@Nonnull
	private final UUID backendIdentifier;

	/** Handle to Flink's state meta data. */
	@Nonnull
	private final StreamStateHandle metaDataState;

	/** Set with the ids of all shared state handles created by the checkpoint. */
	@Nonnull
	private final Set<StateHandleID> sharedStateHandleIDs;

	public IncrementalLocalKeyedStateHandle(
		@Nonnull UUID backendIdentifier,
		@Nonnegative long checkpointId,
		@Nonnull DirectoryStateHandle directoryStateHandle,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnull StreamStateHandle metaDataState,
		@Nonnull Set<StateHandleID> sharedStateHandleIDs) {

		super(directoryStateHandle, keyGroupRange);
		this.backendIdentifier = backendIdentifier;
		this.checkpointId = checkpointId;
		this.metaDataState = metaDataState;
		this.sharedStateHandleIDs = sharedStateHandleIDs;
	}

	@Nonnull
	public StreamStateHandle getMetaDataState() {
		return metaDataState;
	}

	public long getCheckpointId() {
		return checkpointId;
	}

	@Nonnull
	public UUID getBackendIdentifier() {
		return backendIdentifier;
	}

	@Nonnull
	public Set<StateHandleID> getSharedStateHandleIDs() {
		return sharedStateHandleIDs;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}

		IncrementalLocalKeyedStateHandle that = (IncrementalLocalKeyedStateHandle) o;

		return getMetaDataState().equals(that.getMetaDataState());
	}

	@Override
	public void discardState() throws Exception {

		Exception collectedEx = null;

		try {
			super.discardState();
		} catch (Exception e) {
			collectedEx = e;
		}

		try {
			metaDataState.discardState();
		} catch (Exception e) {
			collectedEx = ExceptionUtils.firstOrSuppressed(e, collectedEx);
		}

		if (collectedEx != null) {
			throw collectedEx;
		}
	}

	@Override
	public long getStateSize() {
		return super.getStateSize() + metaDataState.getStateSize();
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + getMetaDataState().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "IncrementalLocalKeyedStateHandle{" +
			"metaDataState=" + metaDataState +
			"} " + super.toString();
	}
}

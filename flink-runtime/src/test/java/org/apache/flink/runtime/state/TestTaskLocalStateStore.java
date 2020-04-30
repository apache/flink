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

import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.LongPredicate;

/**
 * Test implementation of a {@link TaskLocalStateStore}.
 */
public class TestTaskLocalStateStore implements TaskLocalStateStore {

	private final SortedMap<Long, TaskStateSnapshot> taskStateSnapshotsByCheckpointID;

	private final LocalRecoveryConfig localRecoveryConfig;

	private boolean disposed;

	public TestTaskLocalStateStore() {
		this(TestLocalRecoveryConfig.disabled());
	}

	public TestTaskLocalStateStore(@Nonnull LocalRecoveryConfig localRecoveryConfig) {
		this.localRecoveryConfig = localRecoveryConfig;
		this.taskStateSnapshotsByCheckpointID = new TreeMap<>();
		this.disposed = false;
	}

	@Override
	public void storeLocalState(long checkpointId, @Nullable TaskStateSnapshot localState) {
		Preconditions.checkState(!disposed);
		taskStateSnapshotsByCheckpointID.put(checkpointId, localState);
	}

	@Nullable
	@Override
	public TaskStateSnapshot retrieveLocalState(long checkpointID) {
		Preconditions.checkState(!disposed);
		return taskStateSnapshotsByCheckpointID.get(checkpointID);
	}

	public void dispose() {
		if (!disposed) {
			disposed = true;
			for (TaskStateSnapshot stateSnapshot : taskStateSnapshotsByCheckpointID.values()) {
				try {
					stateSnapshot.discardState();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			taskStateSnapshotsByCheckpointID.clear();
		}
	}

	@Nonnull
	@Override
	public LocalRecoveryConfig getLocalRecoveryConfig() {
		Preconditions.checkState(!disposed);
		return Preconditions.checkNotNull(localRecoveryConfig);
	}

	@Override
	public void confirmCheckpoint(long confirmedCheckpointId) {
		Preconditions.checkState(!disposed);
		Iterator<Map.Entry<Long, TaskStateSnapshot>> iterator = taskStateSnapshotsByCheckpointID.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Long, TaskStateSnapshot> entry = iterator.next();
			if (entry.getKey() < confirmedCheckpointId) {
				iterator.remove();
				try {
					entry.getValue().discardState();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			} else {
				break;
			}
		}
	}

	@Override
	public void pruneMatchingCheckpoints(LongPredicate matcher) {
		taskStateSnapshotsByCheckpointID.keySet().removeIf(matcher::test);
	}

	public boolean isDisposed() {
		return disposed;
	}

	public SortedMap<Long, TaskStateSnapshot> getTaskStateSnapshotsByCheckpointID() {
		return taskStateSnapshotsByCheckpointID;
	}
}

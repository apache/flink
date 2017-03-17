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

package org.apache.flink.migration.v0;

import org.apache.flink.migration.v0.runtime.TaskStateV0;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Savepoint version 0.
 *
 * <p>This format was introduced with Flink 1.1.0.
 * <pre>
 *     checkpointId: long
 *     numTaskStates: int
 *     |----jobVertexID: long[2]
 *     |----parallelism: int
 *     |----numSubtaskStates: int
 *     |    |----subtaskIndex: int
 *     |    |----serializedValueLength: int
 *     |    |----serializedValue: byte[] (null if serializedValueLength is -1)
 *     |    |----subtaskStateSize: long
 *     |    |----subtaskStateDuration: long
 *     |----numKeyGroupStates: int
 *     |    |----subtaskIndex: int
 *     |    |----serializedValueLength: int
 *     |    |----serializedValue: byte[] (null if serializedValueLength is -1)
 *     |    |----keyGroupStateSize: long
 *     |    |----keyGroupStateDuration: long
 * </pre>
 */
@Deprecated
@SuppressWarnings("deprecation")
public class SavepointV0 implements Savepoint {

	/** The classes that are migrated in SavepointV0 */
	public static final Map<String, String> MigrationMapping = new HashMap<String, String>() {{

		/* migrated state descriptors */
		put("org.apache.flink.api.common.state.StateDescriptor",
			"org.apache.flink.migration.v0.api.StateDescriptorV0");

		put("org.apache.flink.api.common.state.ValueStateDescriptor",
			"org.apache.flink.migration.v0.api.ValueStateDescriptorV0");

		put("org.apache.flink.api.common.state.ListStateDescriptor",
			"org.apache.flink.migration.v0.api.ListStateDescriptorV0");

		put("org.apache.flink.api.common.state.ReducingStateDescriptor",
			"org.apache.flink.migration.v0.api.ReducingStateDescriptorV0");

		put("org.apache.flink.api.common.state.FoldingStateDescriptor",
			"org.apache.flink.migration.v0.api.FoldingStateDescriptorV0");

		/* migrated classes in runtime */
		put("org.apache.flink.streaming.runtime.tasks.StreamTaskStateList",
			"org.apache.flink.migration.v0.runtime.StreamTaskStateListV0");

		put("org.apache.flink.streaming.runtime.tasks.StreamTaskState",
			"org.apache.flink.migration.v0.runtime.StreamTaskStateV0");

		put("org.apache.flink.runtime.state.AbstractCloseableHandle",
			"org.apache.flink.migration.v0.runtime.AbstractCloseableHandleV0");

		put("org.apache.flink.runtime.state.AbstractStateBackend$DataInputViewHandle",
			"org.apache.flink.migration.v0.runtime.AbstractStateBackendV0$DataInputViewHandle");

		/* migrated classes in memory state backend */
		put("org.apache.flink.runtime.state.memory.AbstractMemStateSnapshot",
			"org.apache.flink.migration.v0.runtime.memory.AbstractMemStateSnapshotV0");

		put("org.apache.flink.runtime.state.memory.SerializedStateHandle",
			"org.apache.flink.migration.v0.runtime.memory.SerializedStateHandleV0");

		put("org.apache.flink.runtime.state.memory.ByteStreamStateHandle",
			"org.apache.flink.migration.v0.runtime.memory.ByteStreamStateHandleV0");

		put("org.apache.flink.runtime.state.memory.MemValueState$Snapshot",
			"org.apache.flink.migration.v0.runtime.memory.MemValueStateV0$Snapshot");

		put("org.apache.flink.runtime.state.memory.MemListState$Snapshot",
			"org.apache.flink.migration.v0.runtime.memory.MemListStateV0$Snapshot");

		put("org.apache.flink.runtime.state.memory.MemReducingState$Snapshot",
			"org.apache.flink.migration.v0.runtime.memory.MemReducingStateV0$Snapshot");

		put("org.apache.flink.runtime.state.memory.MemFoldingState$Snapshot",
			"org.apache.flink.migration.v0.runtime.memory.MemFoldingStateV0$Snapshot");

		/* migrated classes in fs state backend */
		put("org.apache.flink.runtime.state.filesystem.AbstractFsStateSnapshot",
			"org.apache.flink.migration.v0.runtime.filesystem.AbstractFsStateSnapshot");

		put("org.apache.flink.runtime.state.filesystem.AbstractFileStateHandle",
			"org.apache.flink.migration.v0.runtime.filesystem.AbstractFileStateHandleV0");

		put("org.apache.flink.runtime.state.filesystem.FileSerializableStateHandle",
			"org.apache.flink.migration.v0.runtime.filesystem.FileSerializableStateHandleV0");

		put("org.apache.flink.runtime.state.filesystem.FileStreamStateHandle",
			"org.apache.flink.migration.v0.runtime.filesystem.FileStreamStateHandleV0");

		put("org.apache.flink.runtime.state.filesystem.FsValueState$Snapshot",
			"org.apache.flink.migration.v0.runtime.filesystem.FsValueStateV0$Snapshot");

		put("org.apache.flink.runtime.state.filesystem.FsListState$Snapshot",
			"org.apache.flink.migration.v0.runtime.filesystem.FsListStateV0$Snapshot");

		put("org.apache.flink.runtime.state.filesystem.FsReducingState$Snapshot",
			"org.apache.flink.migration.v0.runtime.filesystem.FsReducingStateV0$Snapshot");

		put("org.apache.flink.runtime.state.filesystem.FsFoldingState$Snapshot",
			"org.apache.flink.migration.v0.runtime.filesystem.FsFoldingStateV0$Snapshot");

		/* migrated classes in rocksdb state backend */
		put("org.apache.flink.contrib.streaming.state.RocksDBStateBackend$FinalFullyAsyncSnapshot",
			"org.apache.flink.migration.v0.runtime.rocksdb.RocksDBStateBackendV0$FinalFullyAsyncSnapshot");

		put("org.apache.flink.contrib.streaming.state.RocksDBStateBackend$FinalSemiAsyncSnapshot",
			"org.apache.flink.migration.v0.runtime.rocksdb.RocksDBStateBackendV0$FinalSemiAsyncSnapshot");
	}};

	/** The savepoint version. */
	public static final int VERSION = 0;

	/** The checkpoint ID */
	private final long checkpointId;

	/** The task states */
	private final Collection<TaskStateV0> taskStates;

	public SavepointV0(long checkpointId, Collection<TaskStateV0> taskStates) {
		this.checkpointId = checkpointId;
		this.taskStates = Preconditions.checkNotNull(taskStates, "Task States");
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public long getCheckpointId() {
		return checkpointId;
	}

	@Override
	public Collection<TaskState> getTaskStates() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void dispose() throws Exception {
		//NOP
	}

	public Collection<TaskStateV0> getOldTaskStates() {
		return taskStates;
	}

	@Override
	public String toString() {
		return "Savepoint(version=" + VERSION + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SavepointV0 that = (SavepointV0) o;
		return checkpointId == that.checkpointId && getTaskStates().equals(that.getTaskStates());
	}

	@Override
	public int hashCode() {
		int result = (int) (checkpointId ^ (checkpointId >>> 32));
		result = 31 * result + taskStates.hashCode();
		return result;
	}
}

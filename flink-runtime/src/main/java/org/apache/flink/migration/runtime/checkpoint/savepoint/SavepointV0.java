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

package org.apache.flink.migration.runtime.checkpoint.savepoint;

import org.apache.flink.migration.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

/**
 * Savepoint version 0.
 *
 * <p>This format was introduced with Flink 1.1.0.
 */
@SuppressWarnings("deprecation")
public class SavepointV0 implements Savepoint {

	/** The savepoint version. */
	public static final int VERSION = 0;

	/** The checkpoint ID */
	private final long checkpointId;

	/** The task states */
	private final Collection<TaskState> taskStates;

	public SavepointV0(long checkpointId, Collection<TaskState> taskStates) {
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
	public Collection<org.apache.flink.runtime.checkpoint.TaskState> getTaskStates() {
		// since checkpoints are never deserialized into this format,
		// this method should never be called
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<MasterState> getMasterStates() {
		// since checkpoints are never deserialized into this format,
		// this method should never be called
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<OperatorState> getOperatorStates() {
		return null;
	}

	@Override
	public void dispose() throws Exception {
		//NOP
	}


	public Collection<TaskState> getOldTaskStates() {
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

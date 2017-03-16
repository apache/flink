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

package org.apache.flink.runtime.checkpoint.savepoint;

import java.util.Collection;
import java.util.Objects;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.util.Preconditions;

/**
 * A base savepoint class implementing the simple accessors of the Savepoint
 * interface.
 */
abstract class AbstractSavepoint implements Savepoint {

	/** The checkpoint ID */
	private final long checkpointId;

	/** The task states */
	private final Collection<TaskState> taskStates;

	AbstractSavepoint(long checkpointId, Collection<TaskState> taskStates) {
		this.checkpointId = checkpointId;
		this.taskStates = Preconditions.checkNotNull(taskStates, "Task States");
	}

	/**
	 * Returns the savepoint version.
	 *
	 * @return Savepoint version.
	 */
	abstract public int getVersion();

	@Override
	public long getCheckpointId() {
		return checkpointId;
	}

	@Override
	public Collection<TaskState> getTaskStates() {
		return taskStates;
	}

	@Override
	public void dispose() throws Exception {
		for (TaskState taskState : taskStates) {
			taskState.discardState();
		}
		taskStates.clear();
	}

	@Override
	public String toString() {
		return "Savepoint(version=" + getVersion() + ")";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || !(o instanceof Savepoint)) {
			return false;
		}

		Savepoint that = (Savepoint) o;
		return getVersion() == that.getVersion()
			&& checkpointId == that.getCheckpointId()
			&& getTaskStates().equals(that.getTaskStates());
	}

	@Override
	public int hashCode() {
		return Objects.hash(getCheckpointId(), getVersion(), taskStates);
	}

}

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

import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Savepoint version 0.
 *
 * <p>This format was introduced with Flink 1.1.0.
 */
public class SavepointV0 implements Savepoint {

	/** The savepoint version. */
	public static final int VERSION = 0;

	/** The checkpoint ID */
	private final long checkpointId;

	/** The task states */
	private final Collection<TaskState> taskStates = new ArrayList();

	public SavepointV0(long checkpointId, Collection<TaskState> taskStates) {
		this.checkpointId = checkpointId;
		this.taskStates.addAll(taskStates);
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
		return taskStates;
	}

	@Override
	public void dispose(ClassLoader classLoader) throws Exception {
		Preconditions.checkNotNull(classLoader, "Class loader");
		for (TaskState taskState : taskStates) {
			taskState.discard(classLoader);
		}
		taskStates.clear();
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

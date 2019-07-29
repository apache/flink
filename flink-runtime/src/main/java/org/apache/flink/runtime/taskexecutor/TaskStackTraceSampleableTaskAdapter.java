/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.Task;

import static java.util.Objects.requireNonNull;

/**
 * Adapts {@link Task} to {@link StackTraceSampleableTask}.
 */
class TaskStackTraceSampleableTaskAdapter implements StackTraceSampleableTask {

	private final Task task;

	static TaskStackTraceSampleableTaskAdapter fromTask(final Task task) {
		return new TaskStackTraceSampleableTaskAdapter(task);
	}

	private TaskStackTraceSampleableTaskAdapter(final Task task) {
		this.task = requireNonNull(task, "task must not be null");
	}

	@Override
	public boolean isRunning() {
		return task.getExecutionState() == ExecutionState.RUNNING;
	}

	@Override
	public StackTraceElement[] getStackTrace() {
		return task.getStackTraceOfExecutingThread();
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return task.getExecutionId();
	}

}

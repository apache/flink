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

package org.apache.flink.api.common;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Encapsulates task-specific information: name, index of subtask, parallelism and attempt number.
 */
public class TaskInfo {

	private final String taskName;
	private final String taskNameWithSubtasks;
	private final int indexOfSubtask;
	private final int numberOfParallelSubtasks;
	private final int attemptNumber;

	public TaskInfo(String taskName, int indexOfSubtask, int numberOfParallelSubtasks, int attemptNumber) {
		checkArgument(indexOfSubtask >= 0, "Task index must be a non-negative number.");
		checkArgument(numberOfParallelSubtasks >= 1, "Parallelism must be a positive number.");
		checkArgument(indexOfSubtask < numberOfParallelSubtasks, "Task index must be less than parallelism.");
		checkArgument(attemptNumber >= 0, "Attempt number must be a non-negative number.");
		this.taskName = checkNotNull(taskName, "Task Name must not be null.");
		this.indexOfSubtask = indexOfSubtask;
		this.numberOfParallelSubtasks = numberOfParallelSubtasks;
		this.attemptNumber = attemptNumber;
		this.taskNameWithSubtasks = taskName + " (" + (indexOfSubtask + 1) + '/' + numberOfParallelSubtasks + ')';
	}

	/**
	 * Returns the name of the task
	 *
	 * @return The name of the task
	 */
	public String getTaskName() {
		return this.taskName;
	}

	/**
	 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
	 * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
	 *
	 * @return The index of the parallel subtask.
	 */
	public int getIndexOfThisSubtask() {
		return this.indexOfSubtask;
	}

	/**
	 * Gets the parallelism with which the parallel task runs.
	 *
	 * @return The parallelism with which the parallel task runs.
	 */
	public int getNumberOfParallelSubtasks() {
		return this.numberOfParallelSubtasks;
	}

	/**
	 * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
	 * The attempt number corresponds to the number of times this task has been restarted(after
	 * failure/cancellation) since the job was initially started.
	 *
	 * @return Attempt number of the subtask.
	 */
	public int getAttemptNumber() {
		return this.attemptNumber;
	}

	/**
	 * Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
	 * where 3 would be ({@link #getIndexOfThisSubtask()} + 1), and 6 would be
	 * {@link #getNumberOfParallelSubtasks()}.
	 *
	 * @return The name of the task, with subtask indicator.
	 */
	public String getTaskNameWithSubtasks() {
		return this.taskNameWithSubtasks;
	}
}

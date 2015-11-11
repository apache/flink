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

/**
 * Encapsulation of runtime information for a Task, including Task Name, sub-task index, etc.
 *
 */
public class TaskRuntimeInfo implements java.io.Serializable {

	/** Task Name */
	private final String taskName;

	/** Index of this task in the parallel task group. Goes from 0 to {@link #numParallelTasks} - 1 */
	private final int subTaskIndex;

	/** Number of parallel running instances of this task */
	private final int numParallelTasks;

	/** Attempt number of this task */
	private final int attemptNumber;

	/** Task Name along with information on index of task in group */
	private final String taskNameWithSubTaskIndex;

	/**
	 * Constructor for TaskRuntimeInfo
	 *
	 * @param taskName Task Name
	 * @param subTaskIndex Index of sub task
	 * @param numParallelTasks Number of parallel running tasks
	 * @param attemptNumber Attempt number of task
	 *
	 */
	public TaskRuntimeInfo(String taskName, int subTaskIndex, int numParallelTasks, int attemptNumber) {
		this.taskName = taskName;
		this.subTaskIndex = subTaskIndex;
		this.numParallelTasks = numParallelTasks;
		this.attemptNumber = attemptNumber;
		this.taskNameWithSubTaskIndex = String.format("%s (%d/%d)", taskName, subTaskIndex + 1, numParallelTasks);
	}

	/**
	 * Gets the name of the task
	 *
	 * @return Name of the task
	 */
	public String getTaskName() {
		return taskName;
	}

	/**
	 * Gets the index of task in group
	 *
	 * @return Index of task in group
	 */
	public int getSubTaskIndex() {
		return subTaskIndex;
	}

	/**
	 * Gets the number of parallel running tasks
	 *
	 * @return Number of parallel running tasks
	 */
	public int getNumParallelTasks() {
		return numParallelTasks;
	}

	/**
	 * Gets the attempt number of the task
	 *
	 * @return Attempt number of the task
	 */
	public int getAttemptNumber() {
		return attemptNumber;
	}

	/**
	 * Gets the name of the task along with sub-task index
	 *
	 * @return Name of the task along with sub-task index
	 */
	public String getTaskNameWithSubTaskIndex() {
		return taskNameWithSubTaskIndex;
	}
}

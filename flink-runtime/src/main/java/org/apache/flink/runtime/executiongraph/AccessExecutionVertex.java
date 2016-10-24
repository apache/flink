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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Common interface for the runtime {@link ExecutionVertex} and {@link ArchivedExecutionVertex}.
 */
public interface AccessExecutionVertex {
	/**
	 * Returns the name of this execution vertex in the format "myTask (2/7)".
	 *
	 * @return name of this execution vertex
	 */
	String getTaskNameWithSubtaskIndex();

	/**
	 * Returns the subtask index of this execution vertex.
	 *
	 * @return subtask index of this execution vertex.
	 */
	int getParallelSubtaskIndex();

	/**
	 * Returns the current execution for this execution vertex.
	 *
	 * @return current execution
	 */
	AccessExecution getCurrentExecutionAttempt();

	/**
	 * Returns the current {@link ExecutionState} for this execution vertex.
	 *
	 * @return execution state for this execution vertex
	 */
	ExecutionState getExecutionState();

	/**
	 * Returns the timestamp for the given {@link ExecutionState}.
	 *
	 * @param state state for which the timestamp should be returned
	 * @return timestamp for the given state
	 */
	long getStateTimestamp(ExecutionState state);

	/**
	 * Returns the exception that caused the job to fail. This is the first root exception
	 * that was not recoverable and triggered job failure.
	 *
	 * @return failure exception as a string, or {@code "(null)"}
	 */
	String getFailureCauseAsString();

	/**
	 * Returns the {@link TaskManagerLocation} for this execution vertex.
	 *
	 * @return taskmanager location for this execution vertex.
	 */
	TaskManagerLocation getCurrentAssignedResourceLocation();

	/**
	 * Returns the execution for the given attempt number.
	 *
	 * @param attemptNumber attempt number of execution to be returned
	 * @return execution for the given attempt number
	 */
	AccessExecution getPriorExecutionAttempt(int attemptNumber);
}

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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;

/** Interface for the communication of the {@link Task} with the {@link TaskExecutor}. */
public interface TaskManagerActions {

    /**
     * Notifies the task manager about a fatal error occurred in the task.
     *
     * @param message Message to report
     * @param cause Cause of the fatal error
     */
    void notifyFatalError(String message, Throwable cause);

    /**
     * Tells the task manager to fail the given task.
     *
     * @param executionAttemptID Execution attempt ID of the task to fail
     * @param cause Cause of the failure
     */
    void failTask(ExecutionAttemptID executionAttemptID, Throwable cause);

    /**
     * Notifies the task manager about the task execution state update.
     *
     * @param taskExecutionState Task execution state update
     */
    void updateTaskExecutionState(TaskExecutionState taskExecutionState);

    /**
     * Notifies that the task has reached the end of data.
     *
     * @param executionAttemptID Execution attempt ID of the task.
     */
    void notifyEndOfData(ExecutionAttemptID executionAttemptID);
}

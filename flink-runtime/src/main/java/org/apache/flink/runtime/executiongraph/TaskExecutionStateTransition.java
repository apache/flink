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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Wraps {@link TaskExecutionState}, along with actions to take if it is FAILED state. */
public class TaskExecutionStateTransition {

    private final TaskExecutionState taskExecutionState;

    /**
     * Indicating whether to send a RPC call to remove task from TaskManager. True if the failure is
     * fired by JobManager and the execution is already deployed. Otherwise it should be false.
     */
    private final boolean cancelTask;

    private final boolean releasePartitions;

    public TaskExecutionStateTransition(final TaskExecutionState taskExecutionState) {
        this(taskExecutionState, false, false);
    }

    public TaskExecutionStateTransition(
            final TaskExecutionState taskExecutionState,
            final boolean cancelTask,
            final boolean releasePartitions) {

        this.taskExecutionState = checkNotNull(taskExecutionState);
        this.cancelTask = cancelTask;
        this.releasePartitions = releasePartitions;
    }

    public Throwable getError(ClassLoader userCodeClassloader) {
        return taskExecutionState.getError(userCodeClassloader);
    }

    public ExecutionAttemptID getID() {
        return taskExecutionState.getID();
    }

    public ExecutionState getExecutionState() {
        return taskExecutionState.getExecutionState();
    }

    public JobID getJobID() {
        return taskExecutionState.getJobID();
    }

    public AccumulatorSnapshot getAccumulators() {
        return taskExecutionState.getAccumulators();
    }

    public IOMetrics getIOMetrics() {
        return taskExecutionState.getIOMetrics();
    }

    public boolean getCancelTask() {
        return cancelTask;
    }

    public boolean getReleasePartitions() {
        return releasePartitions;
    }
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@code ArchivedExecutionVertex} is a readonly representation of {@link ExecutionVertex}. */
public class ArchivedExecutionVertex implements AccessExecutionVertex, Serializable {

    private static final long serialVersionUID = -6708241535015028576L;

    private final int subTaskIndex;

    private final ExecutionHistory executionHistory;

    /** The name in the format "myTask (2/7)", cached to avoid frequent string concatenations. */
    private final String taskNameWithSubtask;

    private final ArchivedExecution currentExecution; // this field must never be null

    private final Collection<AccessExecution> currentExecutions;

    // ------------------------------------------------------------------------

    public ArchivedExecutionVertex(ExecutionVertex vertex) {
        this.subTaskIndex = vertex.getParallelSubtaskIndex();
        this.executionHistory = getCopyOfExecutionHistory(vertex);
        this.taskNameWithSubtask = vertex.getTaskNameWithSubtaskIndex();

        Execution vertexCurrentExecution = vertex.getCurrentExecutionAttempt();
        ArrayList<AccessExecution> currentExecutionList =
                new ArrayList<>(vertex.getCurrentExecutions().size());
        currentExecution = vertexCurrentExecution.archive();
        currentExecutionList.add(currentExecution);
        for (Execution execution : vertex.getCurrentExecutions()) {
            if (execution != vertexCurrentExecution) {
                currentExecutionList.add(execution.archive());
            }
        }
        currentExecutions = Collections.unmodifiableList(currentExecutionList);
    }

    @VisibleForTesting
    public ArchivedExecutionVertex(
            int subTaskIndex,
            String taskNameWithSubtask,
            ArchivedExecution currentExecution,
            ExecutionHistory executionHistory) {
        this.subTaskIndex = subTaskIndex;
        this.taskNameWithSubtask = checkNotNull(taskNameWithSubtask);
        this.currentExecution = checkNotNull(currentExecution);
        this.executionHistory = checkNotNull(executionHistory);
        this.currentExecutions = Collections.singletonList(currentExecution);
    }

    // --------------------------------------------------------------------------------------------
    //   Accessors
    // --------------------------------------------------------------------------------------------

    @Override
    public String getTaskNameWithSubtaskIndex() {
        return this.taskNameWithSubtask;
    }

    @Override
    public int getParallelSubtaskIndex() {
        return this.subTaskIndex;
    }

    @Override
    public ArchivedExecution getCurrentExecutionAttempt() {
        return currentExecution;
    }

    @Override
    public Collection<AccessExecution> getCurrentExecutions() {
        return currentExecutions;
    }

    @Override
    public ExecutionState getExecutionState() {
        return currentExecution.getState();
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return currentExecution.getStateTimestamp(state);
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return currentExecution.getFailureInfo();
    }

    @Override
    public TaskManagerLocation getCurrentAssignedResourceLocation() {
        return currentExecution.getAssignedResourceLocation();
    }

    @Override
    public ExecutionHistory getExecutionHistory() {
        return executionHistory;
    }

    static ExecutionHistory getCopyOfExecutionHistory(ExecutionVertex executionVertex) {
        return new ExecutionHistory(executionVertex.getExecutionHistory());
    }
}

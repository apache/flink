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

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;

import java.io.Serializable;

public class ArchivedExecution implements AccessExecution, Serializable {
    private static final long serialVersionUID = 4817108757483345173L;
    // --------------------------------------------------------------------------------------------

    private final ExecutionAttemptID attemptId;

    private final long[] stateTimestamps;

    private final int attemptNumber;

    private final ExecutionState state;

    private final String failureCause; // once assigned, never changes

    private final TaskManagerLocation assignedResourceLocation; // for the archived execution

    private final AllocationID assignedAllocationID;

    /* Continuously updated map of user-defined accumulators */
    private final StringifiedAccumulatorResult[] userAccumulators;

    private final int parallelSubtaskIndex;

    private final IOMetrics ioMetrics;

    public ArchivedExecution(Execution execution) {
        this(
                execution.getUserAccumulatorsStringified(),
                execution.getIOMetrics(),
                execution.getAttemptId(),
                execution.getAttemptNumber(),
                execution.getState(),
                ExceptionUtils.stringifyException(execution.getFailureCause()),
                execution.getAssignedResourceLocation(),
                execution.getAssignedAllocationID(),
                execution.getVertex().getParallelSubtaskIndex(),
                execution.getStateTimestamps());
    }

    public ArchivedExecution(
            StringifiedAccumulatorResult[] userAccumulators,
            IOMetrics ioMetrics,
            ExecutionAttemptID attemptId,
            int attemptNumber,
            ExecutionState state,
            String failureCause,
            TaskManagerLocation assignedResourceLocation,
            AllocationID assignedAllocationID,
            int parallelSubtaskIndex,
            long[] stateTimestamps) {
        this.userAccumulators = userAccumulators;
        this.ioMetrics = ioMetrics;
        this.failureCause = failureCause;
        this.assignedResourceLocation = assignedResourceLocation;
        this.attemptNumber = attemptNumber;
        this.attemptId = attemptId;
        this.state = state;
        this.stateTimestamps = stateTimestamps;
        this.parallelSubtaskIndex = parallelSubtaskIndex;
        this.assignedAllocationID = assignedAllocationID;
    }

    // --------------------------------------------------------------------------------------------
    //   Accessors
    // --------------------------------------------------------------------------------------------

    @Override
    public ExecutionAttemptID getAttemptId() {
        return attemptId;
    }

    @Override
    public int getAttemptNumber() {
        return attemptNumber;
    }

    @Override
    public long[] getStateTimestamps() {
        return stateTimestamps;
    }

    @Override
    public ExecutionState getState() {
        return state;
    }

    @Override
    public TaskManagerLocation getAssignedResourceLocation() {
        return assignedResourceLocation;
    }

    public AllocationID getAssignedAllocationID() {
        return assignedAllocationID;
    }

    @Override
    public String getFailureCauseAsString() {
        return failureCause;
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        return this.stateTimestamps[state.ordinal()];
    }

    @Override
    public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
        return userAccumulators;
    }

    @Override
    public int getParallelSubtaskIndex() {
        return parallelSubtaskIndex;
    }

    @Override
    public IOMetrics getIOMetrics() {
        return ioMetrics;
    }
}

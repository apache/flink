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

package org.apache.flink.runtime.scheduler.exceptionhistory;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;

/** Mock of an {@link AccessExecution}. */
public class TestingAccessExecution implements AccessExecution {

    private final ExecutionAttemptID executionAttemptID;
    private final ExecutionState state;
    @Nullable private final ErrorInfo failureInfo;
    @Nullable private final TaskManagerLocation taskManagerLocation;

    private TestingAccessExecution(
            ExecutionAttemptID executionAttemptID,
            ExecutionState state,
            @Nullable ErrorInfo failureInfo,
            @Nullable TaskManagerLocation taskManagerLocation) {
        this.executionAttemptID = executionAttemptID;
        this.state = state;
        this.failureInfo = failureInfo;
        this.taskManagerLocation = taskManagerLocation;
    }

    @Override
    public ExecutionAttemptID getAttemptId() {
        return executionAttemptID;
    }

    @Override
    public TaskManagerLocation getAssignedResourceLocation() {
        return taskManagerLocation;
    }

    @Override
    public Optional<ErrorInfo> getFailureInfo() {
        return Optional.ofNullable(failureInfo);
    }

    @Override
    public int getAttemptNumber() {
        throw new UnsupportedOperationException("getAttemptNumber should not be called.");
    }

    @Override
    public long[] getStateTimestamps() {
        throw new UnsupportedOperationException("getStateTimestamps should not be called.");
    }

    @Override
    public long[] getStateEndTimestamps() {
        throw new UnsupportedOperationException("getStateTimestamps should not be called.");
    }

    @Override
    public ExecutionState getState() {
        return state;
    }

    @Override
    public long getStateTimestamp(ExecutionState state) {
        throw new UnsupportedOperationException("getStateTimestamp should not be called.");
    }

    @Override
    public long getStateEndTimestamp(ExecutionState state) {
        throw new UnsupportedOperationException("getStateTimestamp should not be called.");
    }

    @Override
    public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
        throw new UnsupportedOperationException(
                "getUserAccumulatorsStringified should not be called.");
    }

    @Override
    public int getParallelSubtaskIndex() {
        throw new UnsupportedOperationException("getParallelSubtaskIndex should not be called.");
    }

    @Override
    public IOMetrics getIOMetrics() {
        throw new UnsupportedOperationException("getIOMetrics should not be called.");
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder for {@link TestingAccessExecution}. */
    public static class Builder {

        private ExecutionState state = ExecutionState.CREATED;
        private ExecutionAttemptID attemptId = createExecutionAttemptId();
        @Nullable private ErrorInfo failureInfo = null;
        @Nullable private TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

        private Builder() {}

        public Builder withAttemptId(ExecutionAttemptID attemptId) {
            this.attemptId = attemptId;
            return this;
        }

        public Builder withExecutionState(ExecutionState state) {
            this.state = state;
            return this;
        }

        public Builder withErrorInfo(ErrorInfo failureInfo) {
            this.failureInfo = failureInfo;
            return this;
        }

        public Builder withTaskManagerLocation(@Nullable TaskManagerLocation taskManagerLocation) {
            this.taskManagerLocation = taskManagerLocation;
            return this;
        }

        public TestingAccessExecution build() {
            return new TestingAccessExecution(attemptId, state, failureInfo, taskManagerLocation);
        }
    }
}

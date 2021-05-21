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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.runtime.scheduler.exceptionhistory.ArchivedTaskManagerLocationMatcher.isArchivedTaskManagerLocation;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** {@code ExceptionHistoryEntryTest} tests the creation of {@link ExceptionHistoryEntry}. */
public class ExceptionHistoryEntryTest extends TestLogger {

    @Test
    public void testCreate() {
        final Throwable failure = new RuntimeException("Expected exception");
        final long timestamp = System.currentTimeMillis();
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final AccessExecution execution =
                new TestAccessExecution(
                        new ExecutionAttemptID(), failure, timestamp, taskManagerLocation);
        final String taskName = "task name";

        final ExceptionHistoryEntry entry = ExceptionHistoryEntry.create(execution, taskName);

        assertThat(
                entry.getException().deserializeError(ClassLoader.getSystemClassLoader()),
                is(failure));
        assertThat(entry.getTimestamp(), is(timestamp));
        assertThat(entry.getFailingTaskName(), is(taskName));
        assertThat(
                entry.getTaskManagerLocation(), isArchivedTaskManagerLocation(taskManagerLocation));
        assertThat(entry.isGlobal(), is(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreationFailure() {
        ExceptionHistoryEntry.create(
                TestAccessExecution.createExecutionWithoutFailure(
                        new ExecutionAttemptID(), new LocalTaskManagerLocation()),
                "task name");
    }

    @Test(expected = NullPointerException.class)
    public void testNullExecution() {
        ExceptionHistoryEntry.create(null, "task name");
    }

    @Test(expected = NullPointerException.class)
    public void testNullTaskName() {
        ExceptionHistoryEntry.create(
                new TestAccessExecution(
                        new ExecutionAttemptID(),
                        new Exception("Expected failure"),
                        System.currentTimeMillis(),
                        new LocalTaskManagerLocation()),
                null);
    }

    @Test
    public void testWithMissingTaskManagerLocation() {
        final Exception failure = new Exception("Expected failure");
        final long timestamp = System.currentTimeMillis();
        final String taskName = "task name";

        final ExceptionHistoryEntry entry =
                ExceptionHistoryEntry.create(
                        new TestAccessExecution(new ExecutionAttemptID(), failure, timestamp, null),
                        taskName);

        assertThat(
                entry.getException().deserializeError(ClassLoader.getSystemClassLoader()),
                is(failure));
        assertThat(entry.getTimestamp(), is(timestamp));
        assertThat(entry.getFailingTaskName(), is(taskName));
        assertThat(entry.getTaskManagerLocation(), is(nullValue()));
        assertThat(entry.isGlobal(), is(false));
    }

    private static class TestAccessExecution implements AccessExecution {

        private final ExecutionAttemptID executionAttemptID;
        private final ErrorInfo failureInfo;
        private final TaskManagerLocation taskManagerLocation;

        private static TestAccessExecution createExecutionWithoutFailure(
                ExecutionAttemptID executionAttemptID, TaskManagerLocation taskManagerLocation) {
            return new TestAccessExecution(executionAttemptID, null, 0L, taskManagerLocation);
        }

        private TestAccessExecution(
                ExecutionAttemptID executionAttemptID,
                @Nullable Throwable failure,
                long timestamp,
                TaskManagerLocation taskManagerLocation) {
            this.executionAttemptID = executionAttemptID;
            this.failureInfo = failure == null ? null : new ErrorInfo(failure, timestamp);
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

        // -- unsupported methods

        @Override
        public int getAttemptNumber() {
            throw new UnsupportedOperationException("getAttemptNumber should not be called.");
        }

        @Override
        public long[] getStateTimestamps() {
            throw new UnsupportedOperationException("getStateTimestamps should not be called.");
        }

        @Override
        public ExecutionState getState() {
            throw new UnsupportedOperationException("getState should not be called.");
        }

        @Override
        public long getStateTimestamp(ExecutionState state) {
            throw new UnsupportedOperationException("getStateTimestamp should not be called.");
        }

        @Override
        public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
            throw new UnsupportedOperationException(
                    "getUserAccumulatorsStringified should not be called.");
        }

        @Override
        public int getParallelSubtaskIndex() {
            throw new UnsupportedOperationException(
                    "getParallelSubtaskIndex should not be called.");
        }

        @Override
        public IOMetrics getIOMetrics() {
            throw new UnsupportedOperationException("getIOMetrics should not be called.");
        }
    }
}

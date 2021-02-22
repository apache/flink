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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.runtime.scheduler.ExceptionHistoryEntry.ArchivedTaskManagerLocation.fromTaskManagerLocation;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** {@code ExceptionHistoryEntryTest} tests the instantiation of {@link ExceptionHistoryEntry}. */
public class ExceptionHistoryEntryTest extends TestLogger {

    @Test
    public void testFromGlobalFailure() {
        final Throwable failureCause = new RuntimeException("failure cause");
        final long timestamp = System.currentTimeMillis();

        final ExceptionHistoryEntry testInstance =
                ExceptionHistoryEntry.fromGlobalFailure(failureCause, timestamp);

        assertThat(
                testInstance.getException().deserializeError(ClassLoader.getSystemClassLoader()),
                is(failureCause));
        assertThat(testInstance.getTimestamp(), is(timestamp));
        assertThat(testInstance.getFailingTaskName(), is(nullValue()));
        assertThat(testInstance.getTaskManagerLocation(), is(nullValue()));
    }

    @Test
    public void testFromFailedExecution() {
        final Throwable failureCause = new RuntimeException("Expected failure");
        final long failureTimestamp = System.currentTimeMillis();
        final String taskNameWithSubTaskIndex = "task name";
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

        final AccessExecution failedExecution =
                new TestingExecution(
                        new ErrorInfo(failureCause, failureTimestamp), taskManagerLocation);
        final ExceptionHistoryEntry testInstance =
                ExceptionHistoryEntry.fromFailedExecution(
                        failedExecution, taskNameWithSubTaskIndex);

        assertThat(
                testInstance.getException().deserializeError(ClassLoader.getSystemClassLoader()),
                is(failureCause));
        assertThat(testInstance.getTimestamp(), is(failureTimestamp));
        assertThat(testInstance.getFailingTaskName(), is(taskNameWithSubTaskIndex));
        assertThat(
                testInstance.getTaskManagerLocation(),
                isArchivedTaskManagerLocation(fromTaskManagerLocation(taskManagerLocation)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromFailedExecutionWithoutFailure() {
        final AccessExecution executionWithoutFailure =
                new TestingExecution(null, new LocalTaskManagerLocation());
        ExceptionHistoryEntry.fromFailedExecution(executionWithoutFailure, "task name");
    }

    /**
     * {@code TestingExecution} mocks {@link AccessExecution} to provide the relevant methods for
     * testing {@link ExceptionHistoryEntry#fromFailedExecution(AccessExecution, String)}.
     */
    private static class TestingExecution implements AccessExecution {

        private final ErrorInfo failureInfo;
        private final TaskManagerLocation taskManagerLocation;

        private TestingExecution(
                @Nullable ErrorInfo failureInfo,
                @Nullable TaskManagerLocation taskManagerLocation) {
            this.failureInfo = failureInfo;
            this.taskManagerLocation = taskManagerLocation;
        }

        @Override
        public Optional<ErrorInfo> getFailureInfo() {
            return Optional.ofNullable(failureInfo);
        }

        @Override
        public TaskManagerLocation getAssignedResourceLocation() {
            return taskManagerLocation;
        }

        @Override
        public long getStateTimestamp(ExecutionState state) {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public int getParallelSubtaskIndex() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public IOMetrics getIOMetrics() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public ExecutionAttemptID getAttemptId() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public int getAttemptNumber() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public long[] getStateTimestamps() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }

        @Override
        public ExecutionState getState() {
            throw new UnsupportedOperationException("Method should not be triggered.");
        }
    }

    public static Matcher<ExceptionHistoryEntry.ArchivedTaskManagerLocation>
            isArchivedTaskManagerLocation(
                    ExceptionHistoryEntry.ArchivedTaskManagerLocation actualLocation) {
        return new ArchivedTaskManagerLocationMatcher(actualLocation);
    }

    private static class ArchivedTaskManagerLocationMatcher
            extends TypeSafeDiagnosingMatcher<ExceptionHistoryEntry.ArchivedTaskManagerLocation> {

        private final ExceptionHistoryEntry.ArchivedTaskManagerLocation expectedLocation;

        public ArchivedTaskManagerLocationMatcher(
                ExceptionHistoryEntry.ArchivedTaskManagerLocation expectedLocation) {
            this.expectedLocation = expectedLocation;
        }

        @Override
        protected boolean matchesSafely(
                ExceptionHistoryEntry.ArchivedTaskManagerLocation actual, Description description) {
            if (actual == null) {
                return expectedLocation == null;
            }

            boolean match = true;
            if (!Objects.equals(actual.getAddress(), expectedLocation.getAddress())) {
                description.appendText(" address=").appendText(actual.getAddress());
                match = false;
            }

            if (!Objects.equals(actual.getFQDNHostname(), expectedLocation.getFQDNHostname())) {
                description.appendText(" FQDNHostname=").appendText(actual.getFQDNHostname());
                match = false;
            }

            if (!Objects.equals(actual.getHostname(), expectedLocation.getHostname())) {
                description.appendText(" hostname=").appendText(actual.getHostname());
                match = false;
            }

            if (!Objects.equals(actual.getResourceID(), expectedLocation.getResourceID())) {
                description
                        .appendText(" resourceID=")
                        .appendText(actual.getResourceID().toString());
                match = false;
            }

            if (!Objects.equals(actual.getPort(), expectedLocation.getPort())) {
                description.appendText(" port=").appendText(String.valueOf(actual.getPort()));
                match = false;
            }

            return match;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(String.valueOf(expectedLocation));
        }
    }
}

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

import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.scheduler.exceptionhistory.ArchivedTaskManagerLocationMatcher.isArchivedTaskManagerLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code ExceptionHistoryEntryTest} tests the creation of {@link ExceptionHistoryEntry}. */
class ExceptionHistoryEntryTest {

    @Test
    void testCreate() {
        final Throwable failure = new RuntimeException("Expected exception");
        final long timestamp = System.currentTimeMillis();
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final AccessExecution execution =
                TestingAccessExecution.newBuilder()
                        .withErrorInfo(new ErrorInfo(failure, timestamp))
                        .withTaskManagerLocation(taskManagerLocation)
                        .build();
        final String taskName = "task name";
        final Map<String, String> failureLabels = Collections.singletonMap("key", "value");

        final ExceptionHistoryEntry entry =
                ExceptionHistoryEntry.create(
                        execution, taskName, CompletableFuture.completedFuture(failureLabels));

        assertThat(entry.getException().deserializeError(ClassLoader.getSystemClassLoader()))
                .isEqualTo(failure);
        assertThat(entry.getTimestamp()).isEqualTo(timestamp);
        assertThat(entry.getFailingTaskName()).isEqualTo(taskName);
        assertThat(entry.getTaskManagerLocation())
                .matches(isArchivedTaskManagerLocation(taskManagerLocation));
        assertThat(entry.isGlobal()).isFalse();
        assertThat(entry.getFailureLabels()).isEqualTo(failureLabels);
    }

    @Test
    void testCreationFailure() {
        assertThatThrownBy(
                        () ->
                                ExceptionHistoryEntry.create(
                                        TestingAccessExecution.newBuilder()
                                                .withTaskManagerLocation(
                                                        new LocalTaskManagerLocation())
                                                .build(),
                                        "task name",
                                        FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNullExecution() {
        assertThatThrownBy(
                        () ->
                                ExceptionHistoryEntry.create(
                                        null,
                                        "task name",
                                        FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testNullTaskName() {
        assertThatThrownBy(
                        () ->
                                ExceptionHistoryEntry.create(
                                        TestingAccessExecution.newBuilder()
                                                .withErrorInfo(
                                                        new ErrorInfo(
                                                                new Exception("Expected failure"),
                                                                System.currentTimeMillis()))
                                                .withTaskManagerLocation(
                                                        new LocalTaskManagerLocation())
                                                .build(),
                                        null,
                                        FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testWithMissingTaskManagerLocation() {
        final Exception failure = new Exception("Expected failure");
        final long timestamp = System.currentTimeMillis();
        final String taskName = "task name";

        final ExceptionHistoryEntry entry =
                ExceptionHistoryEntry.create(
                        TestingAccessExecution.newBuilder()
                                .withTaskManagerLocation(null)
                                .withErrorInfo(new ErrorInfo(failure, timestamp))
                                .build(),
                        taskName,
                        FailureEnricherUtils.EMPTY_FAILURE_LABELS);

        assertThat(entry.getException().deserializeError(ClassLoader.getSystemClassLoader()))
                .isEqualTo(failure);
        assertThat(entry.getTimestamp()).isEqualTo(timestamp);
        assertThat(entry.getFailingTaskName()).isEqualTo(taskName);
        assertThat(entry.getTaskManagerLocation()).isNull();
        assertThat(entry.isGlobal()).isFalse();
    }
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@code RootExceptionHistoryEntry} extending {@link ExceptionHistoryEntry} by providing a list of
 * {@code ExceptionHistoryEntry} instances to store concurrently caught failures.
 */
public class RootExceptionHistoryEntry extends ExceptionHistoryEntry {

    private static final long serialVersionUID = -7647332765867297434L;

    private final Set<ExceptionHistoryEntry> concurrentExceptions;

    /**
     * Creates a {@code RootExceptionHistoryEntry} based on the passed {@link
     * FailureHandlingResultSnapshot}.
     *
     * @param snapshot The reason for the failure.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code cause} or {@code failingTaskName} are {@code null}.
     * @throws IllegalArgumentException if the {@code timestamp} of the passed {@code
     *     FailureHandlingResult} is not bigger than {@code 0}.
     */
    public static RootExceptionHistoryEntry fromFailureHandlingResultSnapshot(
            FailureHandlingResultSnapshot snapshot) {
        String failingTaskName = null;
        TaskManagerLocation taskManagerLocation = null;
        if (snapshot.getRootCauseExecution().isPresent()) {
            final Execution rootCauseExecution = snapshot.getRootCauseExecution().get();
            failingTaskName = rootCauseExecution.getVertexWithAttempt();
            taskManagerLocation = rootCauseExecution.getAssignedResourceLocation();
        }

        return createRootExceptionHistoryEntry(
                snapshot.getRootCause(),
                snapshot.getTimestamp(),
                failingTaskName,
                taskManagerLocation,
                snapshot.getConcurrentlyFailedExecution());
    }

    /**
     * Creates a {@code RootExceptionHistoryEntry} representing a global failure from the passed
     * {@code Throwable} and timestamp. If any of the passed {@link Execution Executions} failed, it
     * will be added to the {@code RootExceptionHistoryEntry}'s concurrently caught failures.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @param executions The {@link Execution} instances that shall be analyzed for failures.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code failure} is {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    public static RootExceptionHistoryEntry fromGlobalFailure(
            Throwable cause, long timestamp, Iterable<Execution> executions) {
        return createRootExceptionHistoryEntry(cause, timestamp, null, null, executions);
    }

    /**
     * Creates a {@code RootExceptionHistoryEntry} based on the passed {@link ErrorInfo}. No
     * concurrent failures will be added.
     *
     * @param errorInfo The failure information that shall be used to initialize the {@code
     *     RootExceptionHistoryEntry}.
     * @return The {@code RootExceptionHistoryEntry} instance.
     * @throws NullPointerException if {@code errorInfo} is {@code null} or the passed info does not
     *     contain a {@code Throwable}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    public static RootExceptionHistoryEntry fromGlobalFailure(ErrorInfo errorInfo) {
        Preconditions.checkNotNull(errorInfo, "errorInfo");
        return fromGlobalFailure(
                errorInfo.getException(), errorInfo.getTimestamp(), Collections.emptyList());
    }

    private static RootExceptionHistoryEntry createRootExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            @Nullable String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation,
            Iterable<Execution> executions) {
        return new RootExceptionHistoryEntry(
                cause,
                timestamp,
                failingTaskName,
                taskManagerLocation,
                StreamSupport.stream(executions.spliterator(), false)
                        .filter(execution -> execution.getFailureInfo().isPresent())
                        .map(
                                execution ->
                                        ExceptionHistoryEntry.create(
                                                execution, execution.getVertexWithAttempt()))
                        .collect(Collectors.toSet()));
    }

    /**
     * Instantiates a {@code RootExceptionHistoryEntry}.
     *
     * @param cause The reason for the failure.
     * @param timestamp The time the failure was caught.
     * @param failingTaskName The name of the task that failed.
     * @param taskManagerLocation The host the task was running on.
     * @throws NullPointerException if {@code cause} is {@code null}.
     * @throws IllegalArgumentException if the passed {@code timestamp} is not bigger than {@code
     *     0}.
     */
    @VisibleForTesting
    public RootExceptionHistoryEntry(
            Throwable cause,
            long timestamp,
            @Nullable String failingTaskName,
            @Nullable TaskManagerLocation taskManagerLocation,
            Set<ExceptionHistoryEntry> concurrentExceptions) {
        super(cause, timestamp, failingTaskName, taskManagerLocation);
        this.concurrentExceptions = concurrentExceptions;
    }

    public Iterable<ExceptionHistoryEntry> getConcurrentExceptions() {
        return concurrentExceptions;
    }
}

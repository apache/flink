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
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailureHandlingResult;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@code FailureHandlingResultSnapshot} creates a snapshot of a {@link FailureHandlingResult}
 * providing the actual {@link Execution Executions}.
 */
public class FailureHandlingResultSnapshot {

    @Nullable private final Execution rootCauseExecution;
    private final Throwable rootCause;
    private final long timestamp;
    private final Set<Execution> concurrentlyFailedExecutions;

    /**
     * Creates a {@code FailureHandlingResultSnapshot} based on the passed {@link
     * FailureHandlingResult} and {@link ExecutionVertex ExecutionVertices}.
     *
     * @param failureHandlingResult The {@code FailureHandlingResult} that is used for extracting
     *     the failure information.
     * @param latestExecutionLookup The look-up function for retrieving the latest {@link Execution}
     *     instance for a given {@link ExecutionVertexID}.
     * @return The {@code FailureHandlingResultSnapshot}.
     */
    public static FailureHandlingResultSnapshot create(
            FailureHandlingResult failureHandlingResult,
            Function<ExecutionVertexID, Execution> latestExecutionLookup) {
        final Execution rootCauseExecution =
                failureHandlingResult
                        .getExecutionVertexIdOfFailedTask()
                        .map(latestExecutionLookup)
                        .orElse(null);
        Preconditions.checkArgument(
                rootCauseExecution == null || rootCauseExecution.getFailureInfo().isPresent(),
                String.format(
                        "The execution %s didn't provide a failure info even though the corresponding ExecutionVertex %s is marked as having handled the root cause of this failure.",
                        // the "(null)" values should never be used due to the condition - it's just
                        // added to make the compiler happy
                        rootCauseExecution != null ? rootCauseExecution.getAttemptId() : "(null)",
                        failureHandlingResult
                                .getExecutionVertexIdOfFailedTask()
                                .map(Objects::toString)
                                .orElse("(null)")));

        final ExecutionVertexID rootCauseExecutionVertexId =
                failureHandlingResult.getExecutionVertexIdOfFailedTask().orElse(null);
        final Set<Execution> concurrentlyFailedExecutions =
                failureHandlingResult.getVerticesToRestart().stream()
                        .filter(
                                executionVertexId ->
                                        !executionVertexId.equals(rootCauseExecutionVertexId))
                        .map(latestExecutionLookup)
                        .filter(execution -> execution.getFailureInfo().isPresent())
                        .collect(Collectors.toSet());

        return new FailureHandlingResultSnapshot(
                rootCauseExecution,
                ErrorInfo.handleMissingThrowable(failureHandlingResult.getError()),
                failureHandlingResult.getTimestamp(),
                concurrentlyFailedExecutions);
    }

    @VisibleForTesting
    FailureHandlingResultSnapshot(
            @Nullable Execution rootCauseExecution,
            Throwable rootCause,
            long timestamp,
            Set<Execution> concurrentlyFailedExecutions) {
        Preconditions.checkArgument(
                rootCauseExecution == null
                        || !concurrentlyFailedExecutions.contains(rootCauseExecution),
                "The rootCauseExecution should not be part of the concurrentlyFailedExecutions map.");

        this.rootCauseExecution = rootCauseExecution;
        this.rootCause = Preconditions.checkNotNull(rootCause);
        this.timestamp = timestamp;
        this.concurrentlyFailedExecutions =
                Preconditions.checkNotNull(concurrentlyFailedExecutions);
    }

    /**
     * Returns the {@link Execution} that handled the root cause for this failure. An empty {@code
     * Optional} will be returned if it's a global failure.
     *
     * @return The {@link Execution} that handled the root cause for this failure.
     */
    public Optional<Execution> getRootCauseExecution() {
        return Optional.ofNullable(rootCauseExecution);
    }

    /**
     * The actual failure that is handled.
     *
     * @return The {@code Throwable}.
     */
    public Throwable getRootCause() {
        return rootCause;
    }

    /**
     * The time the failure occurred.
     *
     * @return The time of the failure.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * All {@link Execution Executions} that failed and are planned to be restarted as part of this
     * failure handling.
     *
     * @return The concurrently failed {@code Executions}.
     */
    public Iterable<Execution> getConcurrentlyFailedExecution() {
        return Collections.unmodifiableSet(concurrentlyFailedExecutions);
    }
}

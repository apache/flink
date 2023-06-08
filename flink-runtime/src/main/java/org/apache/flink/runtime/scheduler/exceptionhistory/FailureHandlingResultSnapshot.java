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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@code FailureHandlingResultSnapshot} creates a snapshot of a {@link FailureHandlingResult}
 * providing the actual {@link Execution Executions}.
 */
public class FailureHandlingResultSnapshot {

    @Nullable private final Execution rootCauseExecution;
    private final Throwable rootCause;
    private final CompletableFuture<Map<String, String>> failureLabels;
    private final long timestamp;
    private final Set<Execution> concurrentlyFailedExecutions;

    /**
     * Creates a {@code FailureHandlingResultSnapshot} based on the passed {@link
     * FailureHandlingResult} and {@link ExecutionVertex ExecutionVertices}.
     *
     * @param failureHandlingResult The {@code FailureHandlingResult} that is used for extracting
     *     the failure information.
     * @param currentExecutionsLookup The look-up function for retrieving all the current {@link
     *     Execution} instances for a given {@link ExecutionVertexID}.
     * @return The {@code FailureHandlingResultSnapshot}.
     */
    public static FailureHandlingResultSnapshot create(
            FailureHandlingResult failureHandlingResult,
            Function<ExecutionVertexID, Collection<Execution>> currentExecutionsLookup) {
        final Execution rootCauseExecution =
                failureHandlingResult.getFailedExecution().orElse(null);

        if (rootCauseExecution != null && !rootCauseExecution.getFailureInfo().isPresent()) {
            throw new IllegalArgumentException(
                    String.format(
                            "The failed execution %s didn't provide a failure info.",
                            rootCauseExecution.getAttemptId()));
        }

        final Set<Execution> concurrentlyFailedExecutions =
                failureHandlingResult.getVerticesToRestart().stream()
                        .flatMap(id -> currentExecutionsLookup.apply(id).stream())
                        .filter(execution -> execution != rootCauseExecution)
                        .filter(execution -> execution.getFailureInfo().isPresent())
                        .collect(Collectors.toSet());

        return new FailureHandlingResultSnapshot(
                rootCauseExecution,
                ErrorInfo.handleMissingThrowable(failureHandlingResult.getError()),
                failureHandlingResult.getTimestamp(),
                failureHandlingResult.getFailureLabels(),
                concurrentlyFailedExecutions);
    }

    @VisibleForTesting
    FailureHandlingResultSnapshot(
            @Nullable Execution rootCauseExecution,
            Throwable rootCause,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            Set<Execution> concurrentlyFailedExecutions) {
        Preconditions.checkArgument(
                rootCauseExecution == null
                        || !concurrentlyFailedExecutions.contains(rootCauseExecution),
                "The rootCauseExecution should not be part of the concurrentlyFailedExecutions map.");

        this.rootCauseExecution = rootCauseExecution;
        this.failureLabels = failureLabels;
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
     * Returns the labels future associated with the failure.
     *
     * @return the CompletableFuture map of String labels
     */
    public CompletableFuture<Map<String, String>> getFailureLabels() {
        return failureLabels;
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

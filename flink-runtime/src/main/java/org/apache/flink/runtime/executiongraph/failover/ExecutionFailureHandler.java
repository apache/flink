/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TraceOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.FailureEnricher.Context;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.traces.Span;
import org.apache.flink.traces.SpanBuilder;
import org.apache.flink.util.IterableUtils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This handler deals with task failures to return a {@link FailureHandlingResult} which contains
 * tasks to restart to recover from failures.
 */
public class ExecutionFailureHandler {

    public static final String FAILURE_LABEL_ATTRIBUTE_PREFIX = "failureLabel.";

    private final SchedulingTopology schedulingTopology;

    /** Strategy to judge which tasks should be restarted. */
    private final FailoverStrategy failoverStrategy;

    /** Strategy to judge whether and when a restarting should be done. */
    private final RestartBackoffTimeStrategy restartBackoffTimeStrategy;

    /** Number of all restarts happened since this job is submitted. */
    private long numberOfRestarts;

    private final Context taskFailureCtx;
    private final Context globalFailureCtx;
    private final Collection<FailureEnricher> failureEnrichers;
    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final MetricGroup metricGroup;

    private final boolean reportEventsAsSpans;

    /**
     * Creates the handler to deal with task failures.
     *
     * @param schedulingTopology contains the topology info for failover
     * @param failoverStrategy helps to decide tasks to restart on task failures
     * @param restartBackoffTimeStrategy helps to decide whether to restart failed tasks and the
     *     restarting delay
     * @param mainThreadExecutor the main thread executor of the job master
     * @param failureEnrichers a collection of {@link FailureEnricher} that enrich failures
     * @param taskFailureCtx Task failure Context used by FailureEnrichers
     * @param globalFailureCtx Global failure Context used by FailureEnrichers
     */
    public ExecutionFailureHandler(
            final Configuration jobMasterConfig,
            final SchedulingTopology schedulingTopology,
            final FailoverStrategy failoverStrategy,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final Collection<FailureEnricher> failureEnrichers,
            final Context taskFailureCtx,
            final Context globalFailureCtx,
            final MetricGroup metricGroup) {

        this.schedulingTopology = checkNotNull(schedulingTopology);
        this.failoverStrategy = checkNotNull(failoverStrategy);
        this.restartBackoffTimeStrategy = checkNotNull(restartBackoffTimeStrategy);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
        this.failureEnrichers = checkNotNull(failureEnrichers);
        this.taskFailureCtx = taskFailureCtx;
        this.globalFailureCtx = globalFailureCtx;
        this.metricGroup = metricGroup;
        this.reportEventsAsSpans = jobMasterConfig.get(TraceOptions.REPORT_EVENTS_AS_SPANS);
    }

    /**
     * Return result of failure handling. Can be a set of task vertices to restart and a delay of
     * the restarting. Or that the failure is not recoverable and the reason for it.
     *
     * @param failedExecution is the failed execution
     * @param cause of the task failure
     * @param timestamp of the task failure
     * @return result of the failure handling
     */
    public FailureHandlingResult getFailureHandlingResult(
            Execution failedExecution, Throwable cause, long timestamp) {
        return handleFailureAndReport(
                failedExecution,
                cause,
                timestamp,
                failoverStrategy.getTasksNeedingRestart(failedExecution.getVertex().getID(), cause),
                false);
    }

    /**
     * Return result of failure handling on a global failure. Can be a set of task vertices to
     * restart and a delay of the restarting. Or that the failure is not recoverable and the reason
     * for it.
     *
     * @param cause of the task failure
     * @param timestamp of the task failure
     * @return result of the failure handling
     */
    public FailureHandlingResult getGlobalFailureHandlingResult(
            final Throwable cause, long timestamp) {
        return handleFailureAndReport(
                null,
                cause,
                timestamp,
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet()),
                true);
    }

    private CompletableFuture<Map<String, String>> labelFailure(Throwable cause, boolean isGlobal) {
        if (failureEnrichers.isEmpty()) {
            return FailureEnricherUtils.EMPTY_FAILURE_LABELS;
        }
        final Context ctx = isGlobal ? globalFailureCtx : taskFailureCtx;
        return FailureEnricherUtils.labelFailure(cause, ctx, mainThreadExecutor, failureEnrichers);
    }

    private FailureHandlingResult handleFailureAndReport(
            @Nullable final Execution failedExecution,
            final Throwable cause,
            long timestamp,
            final Set<ExecutionVertexID> verticesToRestart,
            final boolean globalFailure) {

        FailureHandlingResult failureHandlingResult =
                handleFailure(failedExecution, cause, timestamp, verticesToRestart, globalFailure);

        if (reportEventsAsSpans) {
            // TODO: replace with reporting as event once events are supported.
            // Add reporting as callback for when the failure labeling is completed.
            failureHandlingResult
                    .getFailureLabels()
                    .thenAcceptAsync(
                            labels -> reportFailureHandling(failureHandlingResult, labels),
                            mainThreadExecutor);
        }

        return failureHandlingResult;
    }

    private void reportFailureHandling(
            FailureHandlingResult failureHandlingResult, Map<String, String> failureLabels) {

        // Add base attributes
        SpanBuilder spanBuilder =
                Span.builder(ExecutionFailureHandler.class, "JobFailure")
                        .setStartTsMillis(failureHandlingResult.getTimestamp())
                        .setEndTsMillis(failureHandlingResult.getTimestamp())
                        .setAttribute(
                                "canRestart", String.valueOf(failureHandlingResult.canRestart()))
                        .setAttribute(
                                "isGlobalFailure",
                                String.valueOf(failureHandlingResult.isGlobalFailure()));

        // Add all failure labels
        for (Map.Entry<String, String> entry : failureLabels.entrySet()) {
            spanBuilder.setAttribute(
                    FAILURE_LABEL_ATTRIBUTE_PREFIX + entry.getKey(), entry.getValue());
        }
        metricGroup.addSpan(spanBuilder);
    }

    private FailureHandlingResult handleFailure(
            @Nullable final Execution failedExecution,
            final Throwable cause,
            long timestamp,
            final Set<ExecutionVertexID> verticesToRestart,
            final boolean globalFailure) {

        final CompletableFuture<Map<String, String>> failureLabels =
                labelFailure(cause, globalFailure);

        if (isUnrecoverableError(cause)) {
            return FailureHandlingResult.unrecoverable(
                    failedExecution,
                    new JobException("The failure is not recoverable", cause),
                    timestamp,
                    failureLabels,
                    globalFailure,
                    true);
        }

        boolean isNewAttempt = restartBackoffTimeStrategy.notifyFailure(cause);
        if (restartBackoffTimeStrategy.canRestart()) {
            if (isNewAttempt) {
                numberOfRestarts++;
            }

            return FailureHandlingResult.restartable(
                    failedExecution,
                    cause,
                    timestamp,
                    failureLabels,
                    verticesToRestart,
                    restartBackoffTimeStrategy.getBackoffTime(),
                    globalFailure,
                    isNewAttempt);
        } else {
            return FailureHandlingResult.unrecoverable(
                    failedExecution,
                    new JobException(
                            "Recovery is suppressed by " + restartBackoffTimeStrategy, cause),
                    timestamp,
                    failureLabels,
                    globalFailure,
                    isNewAttempt);
        }
    }

    public static boolean isUnrecoverableError(Throwable cause) {
        Optional<Throwable> unrecoverableError =
                ThrowableClassifier.findThrowableOfThrowableType(
                        cause, ThrowableType.NonRecoverableError);
        return unrecoverableError.isPresent();
    }

    public long getNumberOfRestarts() {
        return numberOfRestarts;
    }
}

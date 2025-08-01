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

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.events.Events;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * Metrics and event that captures how long a job has all it's subtask in running or finished state.
 *
 * <p>This is only interesting for streaming jobs, and we don't register for batch.
 */
public class AllSubTasksRunningOrFinishedStateTimeMetrics
        implements ExecutionStatusMetricsRegistrar, StateTimeMetric {
    public enum Status {
        ALL_RUNNING_OR_FINISHED,
        NOT_ALL_RUNNING_OR_FINISHED
    }

    public static final String STATUS_ATTRIBUTE = "status";
    private static final long NOT_STARTED = -1L;
    private final MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings;
    private final Clock clock;

    /**
     * This set keeps track of all executions for which we require them all to be in the RUNNING
     * state at once.
     */
    private final Set<ExecutionAttemptID> expectedRunningVertices = new HashSet<>();

    private int numRunningVertices = 0;

    /** Tracks how long we have been in the all running/finished state in the current epoch. */
    private long currentAllRunningOrFinishedStartTime = NOT_STARTED;

    /**
     * Tracks how long we have been in the all running/finished state in the all previous epochs.
     */
    private long allRunningOrFinishedAccumulatedTime = 0L;

    @Nullable private MetricGroup registeredMetricGroup;

    public AllSubTasksRunningOrFinishedStateTimeMetrics(
            JobType semantic, MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings) {
        this(semantic, stateTimeMetricsSettings, SystemClock.getInstance());
    }

    @VisibleForTesting
    AllSubTasksRunningOrFinishedStateTimeMetrics(
            JobType jobType,
            MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings,
            Clock clock) {
        Preconditions.checkState(
                jobType == JobType.STREAMING,
                "This metric should only be created and registered for streaming jobs!");
        this.stateTimeMetricsSettings = stateTimeMetricsSettings;
        this.clock = clock;
        this.registeredMetricGroup = null;
    }

    @Override
    public long getCurrentTime() {
        return currentAllRunningOrFinishedStartTime == NOT_STARTED
                ? 0L
                : Math.max(0, clock.absoluteTimeMillis() - currentAllRunningOrFinishedStartTime);
    }

    @Override
    public long getTotalTime() {
        return getCurrentTime() + allRunningOrFinishedAccumulatedTime;
    }

    @Override
    public long getBinary() {
        return currentAllRunningOrFinishedStartTime == NOT_STARTED ? 0L : 1L;
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        StateTimeMetric.register(
                stateTimeMetricsSettings, metricGroup, this, "allSubTasksRunningOrFinished");
        // Remember the metric group to which we registered so that events can be reported to it.
        this.registeredMetricGroup = metricGroup;
    }

    public void onStateUpdate(
            Execution execution, ExecutionState previousState, ExecutionState newState) {
        onStateUpdate(execution.getAttemptId(), previousState, newState);
    }

    @Override
    public void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {

        switch (newState) {
            case SCHEDULED:
                expectedRunningVertices.add(execution);
                break;
            case DEPLOYING:
            case INITIALIZING:
                // no-op, wait for running
                break;
            case RUNNING:
                numRunningVertices++;
                if (!allVerticesWereRunningOrFinished()
                        && expectedRunningVertices.size() == numRunningVertices) {
                    markAllRequiredVerticesRunning();
                }
                break;
            case FINISHED:
                // a FINISHED vertex has effectively become irrelevant for checking future state
                // transitions, since we are either looking for all other vertices to be running
                // or any running vertex to transition to a non-finished state.
                expectedRunningVertices.remove(execution);
                break;
            default:
                if (allVerticesWereRunningOrFinished()) {
                    markAnyVertexNotRunningOrRunningOrFinished();
                }
                // the deployment started terminating
                expectedRunningVertices.remove(execution);
        }
        switch (previousState) {
            case RUNNING:
                numRunningVertices--;
                break;
        }
    }

    private boolean allVerticesWereRunningOrFinished() {
        return currentAllRunningOrFinishedStartTime != NOT_STARTED;
    }

    private void markAllRequiredVerticesRunning() {
        currentAllRunningOrFinishedStartTime = clock.absoluteTimeMillis();
        reportAllSubtaskStatusChangeEvent(true);
    }

    private void markAnyVertexNotRunningOrRunningOrFinished() {
        allRunningOrFinishedAccumulatedTime =
                Math.max(0, clock.absoluteTimeMillis() - currentAllRunningOrFinishedStartTime);
        currentAllRunningOrFinishedStartTime = NOT_STARTED;
        reportAllSubtaskStatusChangeEvent(false);
    }

    private void reportAllSubtaskStatusChangeEvent(boolean allRunningOrFinished) {
        final MetricGroup metricGroup = this.registeredMetricGroup;
        if (metricGroup != null) {
            metricGroup.addEvent(
                    Events.AllSubtasksStatusChangeEvent.builder(
                                    AllSubTasksRunningOrFinishedStateTimeMetrics.class)
                            .setSeverity("INFO")
                            .setObservedTsMillis(clock.absoluteTimeMillis())
                            .setAttribute(
                                    STATUS_ATTRIBUTE,
                                    allRunningOrFinished
                                            ? Status.ALL_RUNNING_OR_FINISHED.toString()
                                            : Status.NOT_ALL_RUNNING_OR_FINISHED.toString()));
        }
    }
}

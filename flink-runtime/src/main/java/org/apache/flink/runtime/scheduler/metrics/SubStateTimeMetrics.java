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

package org.apache.flink.runtime.scheduler.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.scheduler.metrics.utils.JobExecutionStatsHolder;
import org.apache.flink.runtime.scheduler.metrics.utils.MetricsPredicateProvider;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Defines metrics that capture different aspects of a job execution (time spent in deployment, time
 * spent in initialization etc). Specifics are passed in through the constructor and then.
 */
public class SubStateTimeMetrics
        implements ExecutionStateUpdateListener, StateTimeMetric, MetricsRegistrar {
    private static final long NOT_STARTED = -1L;

    private final String metricName;
    private final MetricOptions.JobStatusMetricsSettings stateTimeMetricSettings;

    private final EnumSet<ExecutionState> previousStates;
    private final ExecutionState currentState;
    private final EnumSet<ExecutionState> nextStates;

    private final Predicate<JobExecutionStatsHolder> metricStartPredicate;
    private final Predicate<JobExecutionStatsHolder> metricEndPredicate;

    private final Clock clock;

    // book-keeping
    private final Set<ExecutionAttemptID> expectedDeployments = new HashSet<>();
    private int pendingDeployments = 0;
    private int initializingDeployments = 0;
    private int completedDeployments = 0;

    // metrics state
    private long metricStart = NOT_STARTED;
    private long metricTimeTotal = 0L;

    public SubStateTimeMetrics(
            JobType semantic,
            MetricOptions.JobStatusMetricsSettings stateTimeMetricSettings,
            EnumSet<ExecutionState> previousStates,
            ExecutionState targetState,
            EnumSet<ExecutionState> nextStates,
            String name) {
        this(
                semantic,
                stateTimeMetricSettings,
                previousStates,
                targetState,
                nextStates,
                SystemClock.getInstance(),
                name);
    }

    @VisibleForTesting
    public SubStateTimeMetrics(
            JobType semantic,
            MetricOptions.JobStatusMetricsSettings stateTimeMetricSettings,
            EnumSet<ExecutionState> previousStates,
            ExecutionState targetState,
            EnumSet<ExecutionState> nextStates,
            Clock clock,
            String name) {
        this.stateTimeMetricSettings = stateTimeMetricSettings;
        this.previousStates = previousStates;
        this.currentState = targetState;
        this.nextStates = nextStates;
        this.clock = clock;
        this.metricName = name;

        metricStartPredicate =
                MetricsPredicateProvider.getStartPredicate(
                        semantic, targetState, previousStates, nextStates);
        metricEndPredicate =
                MetricsPredicateProvider.getEndPredicate(semantic, nextStates, expectedDeployments);
    }

    @Override
    public long getCurrentTime() {
        return metricStart == NOT_STARTED
                ? 0L
                : Math.max(0, clock.absoluteTimeMillis() - metricStart);
    }

    @Override
    public long getTotalTime() {
        return getCurrentTime() + metricTimeTotal;
    }

    @Override
    public long getBinary() {
        return metricStart == NOT_STARTED ? 0L : 1L;
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        StateTimeMetric.register(stateTimeMetricSettings, metricGroup, this, metricName);
    }

    @Override
    public void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {
        switch (newState) {
            case SCHEDULED:
                expectedDeployments.add(execution);
                break;
            case DEPLOYING:
                pendingDeployments++;
                break;
            case INITIALIZING:
                initializingDeployments++;
                break;
            case RUNNING:
                completedDeployments++;
                break;
            default:
                // the deployment started terminating
                expectedDeployments.remove(execution);
        }
        switch (previousState) {
            case DEPLOYING:
                pendingDeployments--;
                break;
            case INITIALIZING:
                initializingDeployments--;
                break;
            case RUNNING:
                completedDeployments--;
                break;
        }

        JobExecutionStatsHolder jobExecutionStatsHolder =
                new JobExecutionStatsHolder(
                        pendingDeployments, initializingDeployments, completedDeployments);

        if (metricStart == NOT_STARTED) {
            if (metricStartPredicate.test(jobExecutionStatsHolder)) {
                markMetricStart();
            }
        } else {
            if (metricEndPredicate.test(jobExecutionStatsHolder)
                    || expectedDeployments.size() == 0) {
                markMetricEnd();
            }
        }
    }

    private void markMetricStart() {
        metricStart = clock.absoluteTimeMillis();
    }

    private void markMetricEnd() {
        metricTimeTotal += Math.max(0, clock.absoluteTimeMillis() - metricStart);
        metricStart = NOT_STARTED;
    }

    @VisibleForTesting
    boolean hasCleanState() {
        return expectedDeployments.isEmpty()
                && pendingDeployments == 0
                && completedDeployments == 0
                && initializingDeployments == 0
                && metricStart == NOT_STARTED;
    }
}

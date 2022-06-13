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

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;


/**
 * Metrics that capture how long a job took in initialization
 *
 * <p>These metrics differentiate between batch & streaming use-cases:
 *
 * <p>Batch: Measures from the start of the first initialization until the first task has been deployed.
 * From that point the job is making progress.
 *
 * <p>Streaming: Measures from the start of the first initialization until all tasks have been deployed.
 * From that point on checkpoints can be triggered, and thus progress be made.
 */
public class InitializationStateTimeMetrics
        implements ExecutionStateUpdateListener, StateTimeMetric, MetricsRegistrar {
    private static final long NOT_STARTED = -1L;

    private final Predicate<Pair<Integer, Integer>> initializationStartPredicate;
    private final Predicate<Integer> initializationEndPredicate;
    private final MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings;
    private final Clock clock;

    // book-keeping
    private final Set<ExecutionAttemptID> expectedDeployments = new HashSet<>();
    private int pendingDeployments = 0;
    private int initializingDeployments = 0;
    private int completedDeployments = 0;

    // metrics state
    private long initializationStart = NOT_STARTED;
    private long initializationTimeTotal = 0L;

    public InitializationStateTimeMetrics(
            JobType semantic, MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings) {
        this(semantic, stateTimeMetricsSettings, SystemClock.getInstance());
    }

    @VisibleForTesting
    InitializationStateTimeMetrics(
            JobType semantic,
            MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings,
            Clock clock) {
        this.stateTimeMetricsSettings = stateTimeMetricsSettings;
        this.clock = clock;

        if (semantic == JobType.BATCH) {
            // For batch, if there is no task running and atleast one task is initializing, the
            // job is initializing
            initializationStartPredicate = deploymentPair -> deploymentPair.getLeft() == 0;
            initializationEndPredicate = completedDeployments -> completedDeployments > 0;
        } else {
            // For streaming, if there is no task which is deploying and atleast one task is
            // initializing, the job is initializing
            initializationStartPredicate = deploymentPair -> deploymentPair.getRight() == 0;
            initializationEndPredicate =
                    completedDeployments -> completedDeployments == expectedDeployments.size();
        }
    }

    @Override
    public long getCurrentTime() {
        return initializationStart == NOT_STARTED ? 0L
                : Math.max(0, clock.absoluteTimeMillis() - initializationStart);
    }

    @Override
    public long getTotalTime() {
        return getCurrentTime() + initializationTimeTotal;
    }

    @Override
    public long getBinary() {
        return initializationStart == NOT_STARTED ? 0L : 1L;
    }

    @Override
    public void registerMetrics(MetricGroup metricGroup) {
        StateTimeMetric.register(stateTimeMetricsSettings, metricGroup, this, "initializing");
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

        if (initializationStart == NOT_STARTED) {
            if (initializingDeployments > 0 && initializationStartPredicate.test(Pair.of(completedDeployments,
                    pendingDeployments))) {
                markInitializationStart();
            }
        } else {
            if (initializationEndPredicate.test(completedDeployments)
                    || expectedDeployments.isEmpty()) {
                markInitializationEnd();
            }
        }
    }

    private void markInitializationStart() {
        initializationStart = clock.absoluteTimeMillis();
    }

    private void markInitializationEnd() {
        initializationTimeTotal += Math.max(0, clock.absoluteTimeMillis() - initializationStart);
        initializationStart = NOT_STARTED;
    }

    @VisibleForTesting
    boolean hasCleanState() {
        return expectedDeployments.isEmpty() && pendingDeployments == 0 && completedDeployments == 0
                && initializingDeployments == 0 && initializationStart == NOT_STARTED;
    }
}

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

import java.util.EnumSet;

/** Over arching running substate time metrics which manages multiple state time metrics. */
public class RunningSubStateTimeMetrics implements ExecutionStateUpdateListener, MetricsRegistrar {
    private final SubStateTimeMetrics deploymentStateTimeMetrics;
    private final SubStateTimeMetrics initializingStateTimeMetrics;

    public RunningSubStateTimeMetrics(
            JobType semantic, MetricOptions.JobStatusMetricsSettings stateTimeMetricsSettings) {
        this.deploymentStateTimeMetrics =
                new SubStateTimeMetrics(
                        semantic,
                        stateTimeMetricsSettings,
                        EnumSet.noneOf(ExecutionState.class),
                        ExecutionState.DEPLOYING,
                        EnumSet.of(ExecutionState.INITIALIZING, ExecutionState.RUNNING),
                        "deploying");
        this.initializingStateTimeMetrics =
                new SubStateTimeMetrics(
                        semantic,
                        stateTimeMetricsSettings,
                        EnumSet.of(ExecutionState.DEPLOYING),
                        ExecutionState.INITIALIZING,
                        EnumSet.of(ExecutionState.RUNNING),
                        "initializing");
    }

    public void registerMetrics(MetricGroup metricGroup) {
        deploymentStateTimeMetrics.registerMetrics(metricGroup);
        initializingStateTimeMetrics.registerMetrics(metricGroup);
    }

    @Override
    public void onStateUpdate(
            ExecutionAttemptID execution, ExecutionState previousState, ExecutionState newState) {
        deploymentStateTimeMetrics.onStateUpdate(execution, previousState, newState);
        initializingStateTimeMetrics.onStateUpdate(execution, previousState, newState);
    }

    @VisibleForTesting
    SubStateTimeMetrics getDeploymentStateTimeMetrics() {
        return deploymentStateTimeMetrics;
    }

    @VisibleForTesting
    SubStateTimeMetrics getInitializingStateTimeMetrics() {
        return initializingStateTimeMetrics;
    }
}

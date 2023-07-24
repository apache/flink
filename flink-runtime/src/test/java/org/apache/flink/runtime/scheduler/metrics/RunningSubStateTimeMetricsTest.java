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

import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests combining DeploymentStateMetricsTest and InitializationStateTimeMetricsTest. */
public class RunningSubStateTimeMetricsTest {
    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    private static final ExecutionAttemptID DUMMY_ATTEMPT_ID = ExecutionAttemptID.randomId();

    @Test
    void testCombined_batch() {
        final RunningSubStateTimeMetrics metrics =
                new RunningSubStateTimeMetrics(JobType.BATCH, settings);

        updateMetrics(metrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(metrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(metrics, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(1L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(0L);

        updateMetrics(metrics, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(1L);

        updateMetrics(metrics, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(1L);

        updateMetrics(metrics, ExecutionState.INITIALIZING, ExecutionState.RUNNING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(0L);
    }

    @Test
    void testCombined_streaming() {
        final RunningSubStateTimeMetrics metrics =
                new RunningSubStateTimeMetrics(JobType.STREAMING, settings);

        updateMetrics(metrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(metrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(metrics, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(1L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(0L);

        updateMetrics(metrics, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(1L);

        updateMetrics(metrics, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(1L);

        updateMetrics(metrics, ExecutionState.INITIALIZING, ExecutionState.RUNNING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(1L);

        updateMetrics(metrics, ExecutionState.INITIALIZING, ExecutionState.RUNNING);

        assertThat(metrics.getDeploymentStateTimeMetrics().getBinary()).isEqualTo(0L);
        assertThat(metrics.getInitializingStateTimeMetrics().getBinary()).isEqualTo(0L);
    }

    private static void updateMetrics(
            RunningSubStateTimeMetrics runningSubStateTimeMetrics,
            ExecutionState previousState,
            ExecutionState currentState) {
        runningSubStateTimeMetrics.onStateUpdate(DUMMY_ATTEMPT_ID, previousState, currentState);
    }

    @Test
    void testCountBookKeeping() {
        final RunningSubStateTimeMetrics metrics =
                new RunningSubStateTimeMetrics(JobType.BATCH, settings);

        assertThat(metrics.getExecutionStateCounts())
                .satisfies(
                        counts -> {
                            assertThat(counts.getNumExecutionsInState(ExecutionState.DEPLOYING))
                                    .isZero();
                            assertThat(counts.getNumExecutionsInState(ExecutionState.INITIALIZING))
                                    .isZero();
                            assertThat(counts.getNumExecutionsInState(ExecutionState.RUNNING))
                                    .isZero();
                        });

        testCountBookKeeping(ExecutionState.DEPLOYING);
        testCountBookKeeping(ExecutionState.INITIALIZING);
        testCountBookKeeping(ExecutionState.RUNNING);

        assertThat(metrics.hasCleanState()).isTrue();
    }

    private static void testCountBookKeeping(ExecutionState state) {
        final RunningSubStateTimeMetrics metrics =
                new RunningSubStateTimeMetrics(JobType.BATCH, settings);

        ExecutionAttemptID executionAttemptId = ExecutionAttemptID.randomId();

        metrics.onStateUpdate(executionAttemptId, ExecutionState.CREATED, state);
        assertThat(metrics.getExecutionStateCounts().getNumExecutionsInState(state)).isEqualTo(1);

        metrics.onStateUpdate(executionAttemptId, ExecutionState.CREATED, state);
        assertThat(metrics.getExecutionStateCounts().getNumExecutionsInState(state)).isEqualTo(2);

        metrics.onStateUpdate(executionAttemptId, state, ExecutionState.FAILED);
        assertThat(metrics.getExecutionStateCounts().getNumExecutionsInState(state)).isEqualTo(1);

        metrics.onStateUpdate(executionAttemptId, state, ExecutionState.FAILED);
        assertThat(metrics.getExecutionStateCounts().getNumExecutionsInState(state)).isZero();
    }
}

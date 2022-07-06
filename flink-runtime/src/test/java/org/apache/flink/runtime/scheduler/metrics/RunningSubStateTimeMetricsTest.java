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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests combining DeploymentStateMetricsTest and InitializationStateTimeMetricsTest. */
public class RunningSubStateTimeMetricsTest {
    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    void testCombined_batch() {
        final RunningSubStateTimeMetrics runningSubStateTimeMetrics =
                new RunningSubStateTimeMetrics(JobType.BATCH, settings);

        final ExecutionAttemptID id1 = createExecutionAttemptId();
        final ExecutionAttemptID id2 = createExecutionAttemptId();

        updateMetrics(
                id1, runningSubStateTimeMetrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(
                id2, runningSubStateTimeMetrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(
                id1,
                runningSubStateTimeMetrics,
                ExecutionState.SCHEDULED,
                ExecutionState.DEPLOYING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(1L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(0L);

        updateMetrics(
                id1,
                runningSubStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(1L);

        updateMetrics(
                id2,
                runningSubStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(1L);

        updateMetrics(
                id2,
                runningSubStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(0L);
    }

    @Test
    void testCombined_streaming() {
        final RunningSubStateTimeMetrics runningSubStateTimeMetrics =
                new RunningSubStateTimeMetrics(JobType.STREAMING, settings);

        final ExecutionAttemptID id1 = createExecutionAttemptId();
        final ExecutionAttemptID id2 = createExecutionAttemptId();

        updateMetrics(
                id1, runningSubStateTimeMetrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(
                id2, runningSubStateTimeMetrics, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        updateMetrics(
                id1,
                runningSubStateTimeMetrics,
                ExecutionState.SCHEDULED,
                ExecutionState.DEPLOYING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(1L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(0L);

        updateMetrics(
                id1,
                runningSubStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(1L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(1L);

        updateMetrics(
                id2,
                runningSubStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(1L);

        updateMetrics(
                id2,
                runningSubStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(1L);

        updateMetrics(
                id2,
                runningSubStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(runningSubStateTimeMetrics.getDeploymentStateTimeMetrics().getBinary())
                .isEqualTo(0L);
        assertThat(runningSubStateTimeMetrics.getInitializingStateTimeMetrics().getBinary())
                .isEqualTo(0L);
    }

    private static void updateMetrics(
            ExecutionAttemptID id,
            RunningSubStateTimeMetrics runningSubStateTimeMetrics,
            ExecutionState previousState,
            ExecutionState currentState) {
        runningSubStateTimeMetrics.onStateUpdate(id, previousState, currentState);
    }
}

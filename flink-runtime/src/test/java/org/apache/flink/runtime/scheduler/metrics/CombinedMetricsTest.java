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

import org.junit.Test;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests combining DeploymentStateMetricsTest and
 * InitializationStateTimeMetricsTest
 */
public class CombinedMetricsTest {
    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    public void testCombined_batch() {
        final InitializationStateTimeMetrics initializationStateTimeMetrics =
                new InitializationStateTimeMetrics(JobType.BATCH, settings);
        final DeploymentStateTimeMetrics deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings);

        final ExecutionAttemptID id1 = createExecutionAttemptId();
        final ExecutionAttemptID id2 = createExecutionAttemptId();

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.CREATED,
                ExecutionState.SCHEDULED);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.CREATED,
                ExecutionState.SCHEDULED);

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.SCHEDULED,
                ExecutionState.DEPLOYING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(1L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(0L);

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(1L);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(1L);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(0L);
    }

    @Test
    public void testCombined_streaming() {
        final InitializationStateTimeMetrics initializationStateTimeMetrics =
                new InitializationStateTimeMetrics(JobType.STREAMING, settings);
        final DeploymentStateTimeMetrics deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(JobType.STREAMING, settings);

        final ExecutionAttemptID id1 = createExecutionAttemptId();
        final ExecutionAttemptID id2 = createExecutionAttemptId();

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.CREATED,
                ExecutionState.SCHEDULED);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.CREATED,
                ExecutionState.SCHEDULED);

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.SCHEDULED,
                ExecutionState.DEPLOYING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(1L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(0L);

        updateMetrics(
                id1,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(1L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(1L);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.DEPLOYING,
                ExecutionState.INITIALIZING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(1L);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(1L);

        updateMetrics(
                id2,
                initializationStateTimeMetrics,
                deploymentStateTimeMetrics,
                ExecutionState.INITIALIZING,
                ExecutionState.RUNNING);

        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
        assertThat(initializationStateTimeMetrics.getBinary()).isEqualTo(0L);
    }

    private static void updateMetrics(
            ExecutionAttemptID id,
            InitializationStateTimeMetrics initializationStateTimeMetrics,
            DeploymentStateTimeMetrics deploymentStateTimeMetrics,
            ExecutionState previousState,
            ExecutionState currentState) {
        initializationStateTimeMetrics.onStateUpdate(id, previousState, currentState);
        deploymentStateTimeMetrics.onStateUpdate(id, previousState, currentState);
    }
}

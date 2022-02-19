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
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.runtime.scheduler.metrics.StateTimeMetricTest.enable;
import static org.assertj.core.api.Assertions.assertThat;

class DeploymentStateTimeMetricsTest {

    private static final MetricOptions.JobStatusMetricsSettings settings =
            enable(
                    MetricOptions.JobStatusMetrics.STATE,
                    MetricOptions.JobStatusMetrics.CURRENT_TIME,
                    MetricOptions.JobStatusMetrics.TOTAL_TIME);

    @Test
    void testInitialValues() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        assertThat(deploymentStateTimeMetrics.getCurrentTime()).isEqualTo(0L);
        assertThat(deploymentStateTimeMetrics.getTotalTime()).isEqualTo(0L);
        assertThat(deploymentStateTimeMetrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testDeploymentStartsOnFirstDeploying() {
        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        assertThat(metrics.getBinary()).isEqualTo(0L);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        assertThat(metrics.getBinary()).isEqualTo(1L);
    }

    @Test
    void testDeploymentStart_batch_notTriggeredIfOneDeploymentIsRunning() {
        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testDeploymentEnd_batch() {
        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testDeploymentEnd_streaming() {
        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.STREAMING, settings);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testDeploymentEnd_streaming_ignoresTerminalDeployments() {
        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.STREAMING, settings);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.FINISHED);
        assertThat(metrics.getBinary()).isEqualTo(1L);

        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        assertThat(metrics.getBinary()).isEqualTo(0L);
    }

    @Test
    void testGetCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(5L);
    }

    @Test
    void testGetCurrentTimeResetOndDeployentEnd() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);

        assertThat(metrics.getCurrentTime()).isEqualTo(0L);
    }

    @Test
    void testGetCurrentTime_notResetOnSecondaryDeployment() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);

        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));

        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getCurrentTime()).isEqualTo(10L);
    }

    @Test
    void testGetTotalTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.FINISHED);
        assertThat(metrics.getTotalTime()).isEqualTo(5L);

        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id2, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id2, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        clock.advanceTime(Duration.ofMillis(5));
        metrics.onStateUpdate(id2, ExecutionState.DEPLOYING, ExecutionState.FINISHED);
        assertThat(metrics.getTotalTime()).isEqualTo(10L);
    }

    @Test
    void testGetTotalTimeIncludesCurrentTime() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);

        clock.advanceTime(Duration.ofMillis(5));
        assertThat(metrics.getTotalTime()).isEqualTo(5L);
    }

    @Test
    void testCleanStateAfterFullDeploymentCycle() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.INITIALIZING);
        metrics.onStateUpdate(id1, ExecutionState.INITIALIZING, ExecutionState.RUNNING);
        metrics.onStateUpdate(id1, ExecutionState.RUNNING, ExecutionState.CANCELING);
        metrics.onStateUpdate(id1, ExecutionState.CANCELING, ExecutionState.CANCELED);

        assertThat(metrics.hasCleanState()).isEqualTo(true);
    }

    @Test
    void testCleanStateAfterEarlyDeploymentFailure() {
        final ManualClock clock = new ManualClock(Duration.ofMillis(5).toNanos());

        final DeploymentStateTimeMetrics metrics =
                new DeploymentStateTimeMetrics(JobType.BATCH, settings, clock);

        final ExecutionAttemptID id1 = new ExecutionAttemptID();
        final ExecutionAttemptID id2 = new ExecutionAttemptID();

        metrics.onStateUpdate(id1, ExecutionState.CREATED, ExecutionState.SCHEDULED);
        metrics.onStateUpdate(id1, ExecutionState.SCHEDULED, ExecutionState.DEPLOYING);
        metrics.onStateUpdate(id1, ExecutionState.DEPLOYING, ExecutionState.FAILED);

        assertThat(metrics.hasCleanState()).isEqualTo(true);
    }
}

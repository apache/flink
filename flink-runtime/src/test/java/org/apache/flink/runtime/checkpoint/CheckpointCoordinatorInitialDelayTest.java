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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the initial checkpoint delay feature in {@link CheckpointCoordinator}.
 *
 * <p>This feature allows users to configure a delay before the first checkpoint is triggered, which
 * is useful for jobs that need time to warm up or catch up with backlogs before performing the
 * first checkpoint.
 */
class CheckpointCoordinatorInitialDelayTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @BeforeEach
    void setUp() {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    /** Tests that the initial checkpoint delay can be configured and retrieved correctly. */
    @Test
    void testInitialDelayConfiguration() {
        final long initialDelay = 60_000L; // 1 minute

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(initialDelay)
                        .build();

        assertThat(config.getInitialCheckpointDelay()).isEqualTo(initialDelay);
    }

    /** Tests that the default initial checkpoint delay is 0. */
    @Test
    void testDefaultInitialDelayIsZero() {
        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .build();

        assertThat(config.getInitialCheckpointDelay()).isZero();
    }

    /**
     * Tests that the checkpoint scheduler can start successfully with a configured initial delay.
     */
    @Test
    void testCheckpointSchedulerStartsWithInitialDelay() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final long initialDelay = 300_000L; // 5 minutes

        CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(initialDelay)
                        .build();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        try {
            checkpointCoordinator.startCheckpointScheduler();

            assertThat(checkpointCoordinator.isPeriodicCheckpointingStarted()).isTrue();
            assertThat(checkpointCoordinator.isPeriodicCheckpointingConfigured()).isTrue();
        } finally {
            checkpointCoordinator.stopCheckpointScheduler();
            checkpointCoordinator.shutdown();
        }
    }

    /**
     * Tests that the toString() method of CheckpointCoordinatorConfiguration includes the initial
     * delay.
     */
    @Test
    void testConfigurationToStringIncludesInitialDelay() {
        final long initialDelay = 120_000L; // 2 minutes

        CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(initialDelay)
                        .build();

        String configString = config.toString();
        assertThat(configString).contains("initialCheckpointDelay=" + initialDelay);
    }

    /**
     * Tests that two CheckpointCoordinatorConfiguration objects with different initial delays are
     * not equal.
     */
    @Test
    void testConfigurationEquality() {
        CheckpointCoordinatorConfiguration config1 =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(60_000L)
                        .build();

        CheckpointCoordinatorConfiguration config2 =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(120_000L)
                        .build();

        CheckpointCoordinatorConfiguration config3 =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10_000L)
                        .setCheckpointTimeout(200_000L)
                        .setInitialCheckpointDelay(60_000L)
                        .build();

        assertThat(config1).isNotEqualTo(config2);
        assertThat(config1).isEqualTo(config3);
        assertThat(config1.hashCode()).isEqualTo(config3.hashCode());
    }
}

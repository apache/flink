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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Iterator;

import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForAvailableSlots;
import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForRunningTasks;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the Adaptive Scheduler actively triggers a checkpoint to expedite a desired rescale,
 * controlled by {@link JobManagerOptions#SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED}.
 *
 * <p>The cluster is configured so that no periodic checkpoint can complete during a test (the
 * checkpointing interval is effectively infinite). Consequently the only checkpoint that can be
 * completed - and therefore the only event that can drive a rescale of a checkpointing job - is the
 * one actively triggered by the scheduler. With the feature disabled the rescale never happens and
 * the test times out.
 */
@ExtendWith(TestLoggerExtension.class)
class ActiveCheckpointToRescaleITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(ActiveCheckpointToRescaleITCase.class);
    private static final int NUMBER_OF_SLOTS = 4;
    private static final int BEFORE_RESCALE_PARALLELISM = NUMBER_OF_SLOTS;
    private static final int AFTER_RESCALE_PARALLELISM = NUMBER_OF_SLOTS - 1;
    private static final Duration REQUIREMENT_UPDATE_TO_CHECKPOINT_GAP = Duration.ofSeconds(2);

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration())
                            .setNumberSlotsPerTaskManager(NUMBER_OF_SLOTS)
                            .build());

    private static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(50L));
        configuration.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, Duration.ofMillis(50L));
        configuration.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, TestingUtils.infiniteDuration());
        configuration.set(
                JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_MAX_DELAY,
                TestingUtils.infiniteDuration());

        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);

        // the feature under test
        configuration.set(
                JobManagerOptions.SCHEDULER_RESCALE_TRIGGER_ACTIVE_CHECKPOINT_ENABLED, true);

        return configuration;
    }

    @Test
    @Timeout(120000)
    void testRescaleWithActiveCheckpointTrigger(
            @InjectMiniCluster MiniCluster miniCluster,
            @InjectClusterClient RestClusterClient<?> restClusterClient)
            throws Exception {
        final Configuration config = new Configuration();

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(BEFORE_RESCALE_PARALLELISM);
        env.enableCheckpointing(TestingUtils.infiniteDuration().toMillis());
        env.fromSequence(0, Integer.MAX_VALUE).sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        System.out.println(
                "DIAGNOSTIC effective checkpoint interval (ms) = "
                        + jobGraph.getCheckpointingSettings()
                                .getCheckpointCoordinatorConfiguration()
                                .getCheckpointInterval());
        final Iterator<JobVertex> jobVertexIterator = jobGraph.getVertices().iterator();
        assertThat(jobVertexIterator.hasNext()).isTrue();
        final JobVertexID jobVertexId = jobVertexIterator.next().getID();

        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(jobVertexId, 1, AFTER_RESCALE_PARALLELISM)
                        .build();

        restClusterClient.submitJob(jobGraph).join();

        final JobID jobId = jobGraph.getJobID();
        try {
            LOG.info(
                    "Waiting for job {} to reach parallelism of {} for vertex {}.",
                    jobId,
                    BEFORE_RESCALE_PARALLELISM,
                    jobVertexId);
            waitForRunningTasks(restClusterClient, jobId, BEFORE_RESCALE_PARALLELISM);

            LOG.info(
                    "Updating job {} resource requirements: parallelism {} -> {}.",
                    jobId,
                    BEFORE_RESCALE_PARALLELISM,
                    AFTER_RESCALE_PARALLELISM);
            restClusterClient.updateJobResourceRequirements(jobId, jobResourceRequirements).join();
            LOG.info(
                    "Waiting for job {} to rescale to parallelism {} via active checkpoint trigger.",
                    jobId,
                    AFTER_RESCALE_PARALLELISM);
            waitForRunningTasks(restClusterClient, jobId, AFTER_RESCALE_PARALLELISM);
            final int expectedFreeSlotCount = NUMBER_OF_SLOTS - AFTER_RESCALE_PARALLELISM;
            LOG.info(
                    "Waiting for {} slot(s) to become available after scale down.",
                    expectedFreeSlotCount);
            waitForAvailableSlots(restClusterClient, expectedFreeSlotCount);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    @Test
    @Timeout(12000)
    void testNoRescaleWithoutCheckpointingConfigured(
            @InjectMiniCluster MiniCluster miniCluster,
            @InjectClusterClient RestClusterClient<?> restClusterClient)
            throws Exception {
        final Configuration config = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(BEFORE_RESCALE_PARALLELISM);
        env.fromSequence(0, Integer.MAX_VALUE).sinkTo(new DiscardingSink<>());

        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        final Iterator<JobVertex> jobVertexIterator = jobGraph.getVertices().iterator();
        assertThat(jobVertexIterator.hasNext()).isTrue();
        final JobVertexID jobVertexId = jobVertexIterator.next().getID();

        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(jobVertexId, 1, AFTER_RESCALE_PARALLELISM)
                        .build();
        restClusterClient.submitJob(jobGraph).join();
        final JobID jobId = jobGraph.getJobID();
        try {
            waitForRunningTasks(restClusterClient, jobId, BEFORE_RESCALE_PARALLELISM);
            restClusterClient.updateJobResourceRequirements(jobId, jobResourceRequirements).join();
            Thread.sleep(REQUIREMENT_UPDATE_TO_CHECKPOINT_GAP.toMillis());
            waitForRunningTasks(restClusterClient, jobId, BEFORE_RESCALE_PARALLELISM);
            LOG.info("Verified: job {} did not rescale without checkpointing configured.", jobId);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }
}

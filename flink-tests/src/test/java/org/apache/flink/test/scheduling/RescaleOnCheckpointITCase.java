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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Iterator;

import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForAvailableSlots;
import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForRunningTasks;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestLoggerExtension.class)
class RescaleOnCheckpointITCase {

    // Scaling down is used here because scaling up is not supported by the NumberSequenceSource
    // that's used in this test.
    private static final int NUMBER_OF_SLOTS = 4;
    private static final int BEFORE_RESCALE_PARALLELISM = NUMBER_OF_SLOTS;
    private static final int AFTER_RESCALE_PARALLELISM = NUMBER_OF_SLOTS - 1;

    // This timeout is used to wait for any possible rescale after the JobRequirement
    // update (which shouldn't happen). A longer gap makes the test more reliable (it's hard to test
    // that something didn't happen) but also increases the runtime of the test.
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

        // speed the test suite up
        // - lower refresh interval -> controls how fast we invalidate ExecutionGraphCache
        // - lower slot idle timeout -> controls how fast we return idle slots to TM
        configuration.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(50L));
        configuration.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, Duration.ofMillis(50L));

        // no checkpoints shall be triggered by Flink itself
        configuration.set(
                CheckpointingOptions.CHECKPOINTING_INTERVAL, TestingUtils.infiniteDuration());

        // rescale shouldn't be triggered due to the timeout
        configuration.set(
                JobManagerOptions.MAXIMUM_DELAY_FOR_SCALE_TRIGGER, TestingUtils.infiniteDuration());

        // no cooldown to avoid delaying the test even more
        configuration.set(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN, Duration.ZERO);

        return configuration;
    }

    @Test
    void testRescaleOnCheckpoint(
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
        assertThat(jobVertexIterator.hasNext())
                .as("There needs to be at least one JobVertex.")
                .isTrue();
        final JobResourceRequirements jobResourceRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(
                                jobVertexIterator.next().getID(), 1, AFTER_RESCALE_PARALLELISM)
                        .build();
        assertThat(jobVertexIterator.hasNext())
                .as("This test expects to have only one JobVertex.")
                .isFalse();

        restClusterClient.submitJob(jobGraph).join();
        try {
            final JobID jobId = jobGraph.getJobID();

            waitForRunningTasks(restClusterClient, jobId, BEFORE_RESCALE_PARALLELISM);

            restClusterClient.updateJobResourceRequirements(jobId, jobResourceRequirements).join();

            // timeout to allow any unexpected rescaling to happen anyway
            Thread.sleep(REQUIREMENT_UPDATE_TO_CHECKPOINT_GAP.toMillis());

            // verify that the previous timeout didn't result in a change of parallelism
            waitForRunningTasks(restClusterClient, jobId, BEFORE_RESCALE_PARALLELISM);

            miniCluster.triggerCheckpoint(jobId);

            waitForRunningTasks(restClusterClient, jobId, AFTER_RESCALE_PARALLELISM);

            waitForAvailableSlots(restClusterClient, NUMBER_OF_SLOTS - AFTER_RESCALE_PARALLELISM);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }
}

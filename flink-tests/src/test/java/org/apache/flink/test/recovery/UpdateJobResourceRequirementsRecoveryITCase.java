/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;

/** Tests for recovering of rescaled jobs. */
@ExtendWith(TestLoggerExtension.class)
class UpdateJobResourceRequirementsRecoveryITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(UpdateJobResourceRequirementsRecoveryITCase.class);

    @RegisterExtension
    private static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    /** Tests that a rescaled job graph will be recovered with the latest parallelism. */
    @Test
    void testRescaledJobGraphsWillBeRecoveredCorrectly(@TempDir Path tmpFolder) throws Exception {
        final Configuration configuration = new Configuration();

        final JobVertex jobVertex = new JobVertex("operator");
        jobVertex.setParallelism(1);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);
        final JobID jobId = jobGraph.getJobID();

        // We need to have a restart strategy set, to prevent the job from failing during the first
        // cluster shutdown when TM disconnects.
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));

        // The test is only supposed to pass with AdaptiveScheduler enabled.
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        // High-Availability settings.
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOOKEEPER_EXTENSION.getCustomExtension().getConnectString());
        configuration.set(
                HighAvailabilityOptions.HA_STORAGE_PATH, tmpFolder.toFile().getAbsolutePath());

        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumSlotsPerTaskManager(2)
                        .build();

        final RestClusterClient<?> restClusterClient =
                new RestClusterClient<>(configuration, "foobar");

        final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
        miniCluster.start();

        assertThatFuture(restClusterClient.submitJob(jobGraph)).eventuallySucceeds();

        ClientUtils.waitUntilJobInitializationFinished(
                () -> restClusterClient.getJobStatus(jobId).get(),
                () -> restClusterClient.requestJobResult(jobId).get(),
                getClass().getClassLoader());

        assertThatFuture(
                        restClusterClient.updateJobResourceRequirements(
                                jobGraph.getJobID(),
                                JobResourceRequirements.newBuilder()
                                        .setParallelismForJobVertex(jobVertex.getID(), 1, 2)
                                        .build()))
                .eventuallySucceeds();
        assertThatFuture(miniCluster.closeAsyncWithoutCleaningHighAvailabilityData())
                .eventuallySucceeds();

        LOG.info("Start second mini cluster to recover the persisted job.");

        try (final MiniCluster recoveredMiniCluster = new MiniCluster(miniClusterConfiguration)) {
            recoveredMiniCluster.start();
            UpdateJobResourceRequirementsITCase.waitForRunningTasks(restClusterClient, jobId, 2);
        }
    }
}

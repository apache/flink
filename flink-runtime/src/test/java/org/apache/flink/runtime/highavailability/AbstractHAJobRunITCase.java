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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.core.testutils.EachCallbackWrapper;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterExtension;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code AbstractHAJobRunITCase} runs a job storing in HA mode and provides {@code abstract}
 * methods for initializing a specific {@link FileSystem}.
 */
@ExtendWith(TestLoggerExtension.class)
public abstract class AbstractHAJobRunITCase {

    @RegisterExtension
    private static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @RegisterExtension
    private final EachCallbackWrapper<MiniClusterExtension> miniClusterExtension =
            new EachCallbackWrapper<>(
                    new MiniClusterExtension(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(getFlinkConfiguration())
                                    .build()));

    private Configuration getFlinkConfiguration() {
        final Configuration config = createConfiguration();

        config.set(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOOKEEPER_EXTENSION.getCustomExtension().getConnectString());
        config.set(HighAvailabilityOptions.HA_STORAGE_PATH, getHAStoragePath());

        // getFlinkConfiguration() is called on each new instantiation of the MiniCluster which is
        // happening before each test run
        FileSystem.initialize(config, null);

        return config;
    }

    @AfterEach
    public void unsetFileSystem() {
        FileSystem.initialize(new Configuration(), null);
    }

    /**
     * Should return the path to the HA storage which will be injected into the Flink configuration.
     *
     * @see HighAvailabilityOptions#HA_STORAGE_PATH
     */
    protected abstract String getHAStoragePath();

    /** Initializes the {@link Configuration} used for the Flink cluster. */
    protected abstract Configuration createConfiguration();

    protected void runAfterJobTermination() throws Exception {}

    @Test
    public void testJobExecutionInHaMode() throws Exception {
        final MiniCluster flinkCluster = miniClusterExtension.getCustomExtension().getMiniCluster();

        final JobGraph jobGraph = JobGraphTestUtils.singleNoOpJobGraph();

        // providing a timeout helps making the test fail in case some issue occurred while
        // initializing the cluster
        flinkCluster.submitJob(jobGraph).get(30, TimeUnit.SECONDS);

        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(30));
        final JobStatus jobStatus =
                FutureUtils.retrySuccessfulWithDelay(
                                () -> flinkCluster.getJobStatus(jobGraph.getJobID()),
                                Time.milliseconds(10),
                                deadline,
                                status -> flinkCluster.isRunning() && status == JobStatus.FINISHED,
                                TestingUtils.defaultScheduledExecutor())
                        .get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

        assertThat(jobStatus).isEqualTo(JobStatus.FINISHED);

        runAfterJobTermination();
    }
}

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

package org.apache.flink.test.cancelling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Base class for testing job cancellation. */
@ExtendWith(TestLoggerExtension.class)
abstract class CancelingTestBase {

    private static final int MINIMUM_HEAP_SIZE_MB = 192;

    protected static final int PARALLELISM = 4;

    private static final Configuration configuration = getConfiguration();

    protected ClusterClient<?> clusterClient;

    // --------------------------------------------------------------------------------------------

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberTaskManagers(2)
                            .setNumberSlotsPerTaskManager(4)
                            .build());

    @BeforeEach
    void getClusterClient(@InjectClusterClient ClusterClient<?> clusterClient) {
        this.clusterClient = clusterClient;
    }

    // --------------------------------------------------------------------------------------------

    private static void verifyJvmOptions() {
        final long heap = Runtime.getRuntime().maxMemory() >> 20;
        assertThat(heap)
                .as(
                        "Insufficient java heap space "
                                + heap
                                + "mb - set JVM option: -Xmx"
                                + MINIMUM_HEAP_SIZE_MB
                                + "m")
                .isGreaterThan(MINIMUM_HEAP_SIZE_MB - 50);
    }

    private static Configuration getConfiguration() {
        verifyJvmOptions();
        Configuration config = new Configuration();
        config.set(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
        config.set(RpcOptions.ASK_TIMEOUT_DURATION, TestingUtils.DEFAULT_ASK_TIMEOUT);
        config.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.parse("4096"));

        return config;
    }

    // --------------------------------------------------------------------------------------------

    protected void runAndCancelJob(
            JobGraph jobGraph, final int msecsTillCanceling, int maxTimeTillCanceled)
            throws Exception {
        // submit job
        final long rpcTimeout = configuration.get(RpcOptions.ASK_TIMEOUT_DURATION).toMillis();

        JobID jobID = clusterClient.submitJob(jobGraph).get();

        Deadline submissionDeadLine = new FiniteDuration(2, TimeUnit.MINUTES).fromNow();

        JobStatus jobStatus =
                clusterClient.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        while (jobStatus != JobStatus.RUNNING && submissionDeadLine.hasTimeLeft()) {
            Thread.sleep(50);
            jobStatus = clusterClient.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        }
        if (jobStatus != JobStatus.RUNNING) {
            fail("Job not in state RUNNING.");
        }

        Thread.sleep(msecsTillCanceling);

        clusterClient.cancel(jobID).get();

        Deadline cancelDeadline =
                new FiniteDuration(maxTimeTillCanceled, TimeUnit.MILLISECONDS).fromNow();

        JobStatus jobStatusAfterCancel =
                clusterClient.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        while (jobStatusAfterCancel != JobStatus.CANCELED && cancelDeadline.hasTimeLeft()) {
            Thread.sleep(50);
            jobStatusAfterCancel =
                    clusterClient.getJobStatus(jobID).get(rpcTimeout, TimeUnit.MILLISECONDS);
        }
        assertThat(jobStatusAfterCancel).isEqualTo(JobStatus.CANCELED);
    }
}

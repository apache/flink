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

package org.apache.flink.client;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.WaitingCancelableInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for client's heartbeat. */
class ClientHeartbeatTest {
    private final long clientHeartbeatInterval = 50;
    private final long clientHeartbeatTimeout = 1000;

    private MiniCluster miniCluster;

    @AfterEach
    void teardown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    void testJobCancelledIfClientHeartbeatTimeout() throws Exception {
        JobClient jobClient = submitJob(createConfiguration(true));

        // The client doesn't report heartbeat to the dispatcher.

        FlinkAssertions.assertThatFuture(jobClient.getJobExecutionResult())
                .eventuallyFailsWith(ExecutionException.class)
                .withMessageContaining("Job was cancelled");

        assertThat(miniCluster.isRunning()).isFalse();
    }

    @Test
    void testJobRunningIfClientReportHeartbeat() throws Exception {
        JobClient jobClient = submitJob(createConfiguration(true));

        // The client reports heartbeat to the dispatcher.
        ScheduledExecutorService heartbeatService =
                ClientUtils.reportHeartbeatPeriodically(
                        jobClient, clientHeartbeatInterval, clientHeartbeatTimeout);

        Thread.sleep(2 * clientHeartbeatTimeout);
        assertThat(jobClient.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);

        heartbeatService.shutdown();
    }

    @Test
    void testJobRunningIfDisableClientHeartbeat() throws Exception {
        JobClient jobClient = submitJob(createConfiguration(false));

        Thread.sleep(2 * clientHeartbeatTimeout);
        assertThat(jobClient.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
    }

    private Configuration createConfiguration(boolean shutdownOnAttachedExit) {
        Configuration configuration = new Configuration();
        configuration.set(
                Dispatcher.CLIENT_ALIVENESS_CHECK_DURATION,
                Duration.ofMillis(clientHeartbeatInterval));
        if (shutdownOnAttachedExit) {
            configuration.setBoolean(DeploymentOptions.ATTACHED, true);
            configuration.setBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED, true);
        }
        return configuration;
    }

    // The dispatcher deals with client heartbeat only when shutdownOnAttachedExit is true;
    private JobClient submitJob(Configuration configuration) throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory =
                PerJobMiniClusterFactory.createWithFactory(
                        configuration,
                        config -> {
                            miniCluster = new MiniCluster(config);
                            return miniCluster;
                        });

        JobGraph cancellableJobGraph = getCancellableJobGraph();
        // Enable heartbeat only when both execution.attached and
        // execution.shutdown-on-attached-exit are true.
        if (configuration.getBoolean(DeploymentOptions.ATTACHED)
                && configuration.getBoolean(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
            cancellableJobGraph.setInitialClientHeartbeatTimeout(clientHeartbeatTimeout);
        }
        return perJobMiniClusterFactory
                .submitJob(cancellableJobGraph, ClassLoader.getSystemClassLoader())
                .get();
    }

    private static JobGraph getCancellableJobGraph() {
        JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(WaitingCancelableInvokable.class);
        jobVertex.setParallelism(1);
        return JobGraphTestUtils.streamingJobGraph(jobVertex);
    }
}

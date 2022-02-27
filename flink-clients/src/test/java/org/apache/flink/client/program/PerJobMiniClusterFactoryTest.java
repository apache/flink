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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.WaitingCancelableInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PerJobMiniClusterFactory}. */
class PerJobMiniClusterFactoryTest {

    private MiniCluster miniCluster;

    @AfterEach
    void teardown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    void testJobExecution() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();

        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();

        JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
        assertThat(jobExecutionResult).isNotNull();

        Map<String, Object> actual = jobClient.getAccumulators().get();
        assertThat(actual).isNotNull();

        assertThatMiniClusterIsShutdown();
    }

    @Test
    void testJobClient() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();

        JobGraph cancellableJobGraph = getCancellableJobGraph();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(cancellableJobGraph, ClassLoader.getSystemClassLoader())
                        .get();

        assertThat(jobClient.getJobID()).isEqualTo(cancellableJobGraph.getJobID());
        assertThat(jobClient.getJobStatus().get()).isIn(JobStatus.CREATED, JobStatus.RUNNING);

        jobClient.cancel().get();

        assertThat(jobClient.getJobExecutionResult())
                .failsWithin(Duration.ofSeconds(1))
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("Job was cancelled");

        assertThatMiniClusterIsShutdown();
    }

    @Test
    void testJobClientSavepoint() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getCancellableJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();

        while (jobClient.getJobStatus().get() != JobStatus.RUNNING) {
            Thread.sleep(50);
        }

        assertThatThrownBy(
                        () -> jobClient.triggerSavepoint(null, SavepointFormatType.DEFAULT).get(),
                        "is not a streaming job.")
                .isInstanceOf(ExecutionException.class);

        assertThat(jobClient.stopWithSavepoint(true, null, SavepointFormatType.DEFAULT))
                .failsWithin(Duration.ofSeconds(5L))
                .withThrowableOfType(ExecutionException.class)
                .withMessageContaining("is not a streaming job.");
    }

    @Test
    void testMultipleExecutions() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
        {
            JobClient jobClient =
                    perJobMiniClusterFactory
                            .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                            .get();
            jobClient.getJobExecutionResult().get();
            assertThatMiniClusterIsShutdown();
        }
        {
            JobClient jobClient =
                    perJobMiniClusterFactory
                            .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                            .get();
            jobClient.getJobExecutionResult().get();
            assertThatMiniClusterIsShutdown();
        }
    }

    @Test
    void testJobClientInteractionAfterShutdown() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();
        jobClient.getJobExecutionResult().get();
        assertThatMiniClusterIsShutdown();

        assertThatThrownBy(jobClient::cancel)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "MiniCluster is not yet running or has already been shut down.");
    }

    private PerJobMiniClusterFactory initializeMiniCluster() {
        return PerJobMiniClusterFactory.createWithFactory(
                new Configuration(),
                config -> {
                    miniCluster = new MiniCluster(config);
                    return miniCluster;
                });
    }

    private void assertThatMiniClusterIsShutdown() {
        assertThat(miniCluster.isRunning()).isFalse();
    }

    private static JobGraph getNoopJobGraph() {
        return JobGraphTestUtils.singleNoOpJobGraph();
    }

    private static JobGraph getCancellableJobGraph() {
        JobVertex jobVertex = new JobVertex("jobVertex");
        jobVertex.setInvokableClass(WaitingCancelableInvokable.class);
        jobVertex.setParallelism(1);
        return JobGraphTestUtils.streamingJobGraph(jobVertex);
    }
}

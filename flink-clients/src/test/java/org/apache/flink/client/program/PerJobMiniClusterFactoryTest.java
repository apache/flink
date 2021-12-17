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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.WaitingCancelableInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link PerJobMiniClusterFactory}. */
public class PerJobMiniClusterFactoryTest extends TestLogger {

    private MiniCluster miniCluster;

    @After
    public void teardown() throws Exception {
        if (miniCluster != null) {
            miniCluster.close();
        }
    }

    @Test
    public void testJobExecution() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();

        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();

        JobExecutionResult jobExecutionResult = jobClient.getJobExecutionResult().get();
        assertThat(jobExecutionResult, is(notNullValue()));

        Map<String, Object> actual = jobClient.getAccumulators().get();
        assertThat(actual, is(notNullValue()));

        assertThatMiniClusterIsShutdown();
    }

    @Test
    public void testJobClient() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();

        JobGraph cancellableJobGraph = getCancellableJobGraph();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(cancellableJobGraph, ClassLoader.getSystemClassLoader())
                        .get();

        assertThat(jobClient.getJobID(), is(cancellableJobGraph.getJobID()));
        assertThat(jobClient.getJobStatus().get(), is(JobStatus.RUNNING));

        jobClient.cancel().get();

        assertThrows(
                "Job was cancelled.",
                ExecutionException.class,
                () -> jobClient.getJobExecutionResult().get());

        assertThatMiniClusterIsShutdown();
    }

    @Test
    public void testJobClientSavepoint() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getCancellableJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();

        assertThrows(
                "is not a streaming job.",
                ExecutionException.class,
                () -> jobClient.triggerSavepoint(null).get());

        assertThrows(
                "is not a streaming job.",
                ExecutionException.class,
                () -> jobClient.stopWithSavepoint(true, null).get());
    }

    @Test
    public void testMultipleExecutions() throws Exception {
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
    public void testJobClientInteractionAfterShutdown() throws Exception {
        PerJobMiniClusterFactory perJobMiniClusterFactory = initializeMiniCluster();
        JobClient jobClient =
                perJobMiniClusterFactory
                        .submitJob(getNoopJobGraph(), ClassLoader.getSystemClassLoader())
                        .get();
        jobClient.getJobExecutionResult().get();
        assertThatMiniClusterIsShutdown();

        assertThrows(
                "MiniCluster is not yet running or has already been shut down.",
                IllegalStateException.class,
                jobClient::cancel);
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
        assertThat(miniCluster.isRunning(), is(false));
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

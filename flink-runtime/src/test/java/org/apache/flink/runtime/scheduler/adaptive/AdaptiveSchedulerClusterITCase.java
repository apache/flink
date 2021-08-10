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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;

/**
 * This class contains integration tests for the adaptive scheduler which start a {@link
 * org.apache.flink.runtime.minicluster.MiniCluster} per test case.
 */
public class AdaptiveSchedulerClusterITCase extends TestLogger {

    private static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    private static final int NUMBER_TASK_MANAGERS = 2;
    private static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    private final Configuration configuration = createConfiguration();

    @Rule
    public final MiniClusterResource miniClusterResource =
            new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(configuration)
                            .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                            .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                            .build());

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();

        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT, Duration.ofMillis(100L));

        return configuration;
    }

    @Before
    public void setUp() {
        OnceBlockingNoOpInvokable.reset();
    }

    @Test
    public void testAutomaticScaleDownInCaseOfLostSlots() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        final JobGraph jobGraph = createBlockingJobGraph(PARALLELISM);

        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> resultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS);

        miniCluster.terminateTaskManager(0);

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(),
                JOB_VERTEX_ID,
                NUMBER_SLOTS_PER_TASK_MANAGER * (NUMBER_TASK_MANAGERS - 1));
        OnceBlockingNoOpInvokable.unblock();

        final JobResult jobResult = resultFuture.join();

        assertTrue(jobResult.isSuccess());
    }

    @Test
    public void testAutomaticScaleUp() throws Exception {
        final MiniCluster miniCluster = miniClusterResource.getMiniCluster();
        int initialInstanceCount = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
        int targetInstanceCount = initialInstanceCount + NUMBER_SLOTS_PER_TASK_MANAGER;
        final JobGraph jobGraph = createBlockingJobGraph(targetInstanceCount);

        log.info(
                "Submitting job with parallelism of "
                        + targetInstanceCount
                        + ", to a cluster with only one TM.");
        miniCluster.submitJob(jobGraph).join();
        final CompletableFuture<JobResult> jobResultFuture =
                miniCluster.requestJobResult(jobGraph.getJobID());

        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(), JOB_VERTEX_ID, initialInstanceCount);

        log.info("Start additional TaskManager to scale up to the full parallelism.");
        miniCluster.startTaskManager();

        log.info("Waiting until Invokable is running with higher parallelism");
        waitUntilParallelismForVertexReached(
                jobGraph.getJobID(), JOB_VERTEX_ID, targetInstanceCount);
        OnceBlockingNoOpInvokable.unblock();

        assertTrue(jobResultFuture.join().isSuccess());
    }

    private JobGraph createBlockingJobGraph(int parallelism) throws IOException {
        final JobVertex blockingOperator = new JobVertex("Blocking operator", JOB_VERTEX_ID);

        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);

        blockingOperator.setParallelism(parallelism);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(blockingOperator);

        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0L));
        jobGraph.setExecutionConfig(executionConfig);

        return jobGraph;
    }

    private void waitUntilParallelismForVertexReached(
            JobID jobId, JobVertexID jobVertexId, int targetParallelism) throws Exception {

        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            miniClusterResource
                                    .getMiniCluster()
                                    .getArchivedExecutionGraph(jobId)
                                    .get();

                    if (archivedExecutionGraph.getState() == JobStatus.INITIALIZING) {
                        // parallelism was not yet determined
                        return false;
                    }

                    return archivedExecutionGraph.getAllVertices().get(jobVertexId).getParallelism()
                            == targetParallelism;
                },
                Deadline.fromNow(Duration.ofSeconds(10)));
    }
}

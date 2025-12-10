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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for parallelism overrides via {@code pipeline.jobvertex-parallelism-overrides}
 * configuration.
 *
 * <p>These tests verify that the fix for FLINK-38770 works correctly across different scheduler
 * types and submission modes.
 */
@ExtendWith(TestLoggerExtension.class)
public class ParallelismOverridesITCase {

    private static final int NUMBER_OF_SLOTS = 8;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(createConfiguration())
                            .setNumberSlotsPerTaskManager(NUMBER_OF_SLOTS)
                            .build());

    private static Configuration createConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
        return configuration;
    }

    private RestClusterClient<?> restClusterClient;
    private MiniCluster miniCluster;

    @BeforeEach
    void beforeEach(
            @InjectClusterClient RestClusterClient<?> restClusterClient,
            @InjectMiniCluster MiniCluster miniCluster) {
        this.restClusterClient = restClusterClient;
        this.miniCluster = miniCluster;
    }

    /**
     * Tests parallelism overrides with the Default scheduler. This verifies that JobGraph
     * submission (Session Mode scenario) correctly applies overrides.
     */
    @Test
    void testParallelismOverridesWithDefaultScheduler() throws Exception {
        JobVertex vertex = new JobVertex("test-vertex");
        vertex.setParallelism(1);
        vertex.setInvokableClass(BlockingNoOpInvokable.class);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);

        // Configure parallelism override to change parallelism from 1 to 4
        Map<String, String> overrides = new HashMap<>();
        overrides.put(vertex.getID().toHexString(), "4");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);

        try {
            restClusterClient.submitJob(jobGraph).join();
            JobID jobId = jobGraph.getJobID();

            // Wait for job to be running with 4 tasks
            waitForRunningTasks(restClusterClient, jobId, 4);

            // Verify actual parallelism is 4, not 1
            int actualParallelism = getVertexParallelism(restClusterClient, jobId, vertex.getID());
            assertThat(actualParallelism).as("Parallelism override should be applied").isEqualTo(4);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    /**
     * Tests that job-level configuration takes precedence over cluster-level configuration for
     * parallelism overrides.
     */
    @Test
    void testJobConfigurationTakesPrecedenceOverClusterConfiguration() throws Exception {
        JobVertex vertex = new JobVertex("test-vertex");
        vertex.setParallelism(1);
        vertex.setInvokableClass(BlockingNoOpInvokable.class);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex);

        // Set different override in job configuration
        Map<String, String> jobOverrides = new HashMap<>();
        jobOverrides.put(vertex.getID().toHexString(), "4");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, jobOverrides);

        try {
            restClusterClient.submitJob(jobGraph).join();
            JobID jobId = jobGraph.getJobID();

            // Wait for job to be running
            waitForRunningTasks(restClusterClient, jobId, 4);

            // Verify parallelism is 4 (from job config)
            int actualParallelism = getVertexParallelism(restClusterClient, jobId, vertex.getID());
            assertThat(actualParallelism).isEqualTo(4);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    /**
     * Tests that partial overrides work correctly - only specified vertices are modified, others
     * keep their original parallelism.
     */
    @Test
    void testPartialParallelismOverrides() throws Exception {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        vertex1.setInvokableClass(BlockingNoOpInvokable.class);

        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(2);
        vertex2.setInvokableClass(BlockingNoOpInvokable.class);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);

        // Only override vertex1, not vertex2
        Map<String, String> overrides = new HashMap<>();
        overrides.put(vertex1.getID().toHexString(), "4");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);

        try {
            restClusterClient.submitJob(jobGraph).join();
            JobID jobId = jobGraph.getJobID();

            // Wait for job to be running (4 + 2 = 6 tasks total)
            waitForRunningTasks(restClusterClient, jobId, 6);

            // Verify vertex1 has parallelism 4 (overridden)
            int parallelism1 = getVertexParallelism(restClusterClient, jobId, vertex1.getID());
            assertThat(parallelism1).isEqualTo(4);

            // Verify vertex2 has parallelism 2 (unchanged)
            int parallelism2 = getVertexParallelism(restClusterClient, jobId, vertex2.getID());
            assertThat(parallelism2).isEqualTo(2);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    /**
     * Tests parallelism overrides work correctly when overrides come from multiple sources (cluster
     * config and job config), verifying that job config takes precedence.
     */
    @Test
    void testOverridePrecedenceFromMultipleSources() throws Exception {
        JobVertex vertex1 = new JobVertex("vertex1");
        vertex1.setParallelism(1);
        vertex1.setInvokableClass(BlockingNoOpInvokable.class);

        JobVertex vertex2 = new JobVertex("vertex2");
        vertex2.setParallelism(1);
        vertex2.setInvokableClass(BlockingNoOpInvokable.class);

        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(vertex1, vertex2);

        // Set overrides in job configuration for both vertices
        Map<String, String> jobOverrides = new HashMap<>();
        jobOverrides.put(vertex1.getID().toHexString(), "3");
        jobOverrides.put(vertex2.getID().toHexString(), "2");
        jobGraph.getJobConfiguration().set(PipelineOptions.PARALLELISM_OVERRIDES, jobOverrides);

        try {
            restClusterClient.submitJob(jobGraph).join();
            JobID jobId = jobGraph.getJobID();

            // Wait for job to be running (3 + 2 = 5 tasks total)
            waitForRunningTasks(restClusterClient, jobId, 5);

            // Verify both vertices have their overridden parallelism
            int parallelism1 = getVertexParallelism(restClusterClient, jobId, vertex1.getID());
            assertThat(parallelism1).isEqualTo(3);

            int parallelism2 = getVertexParallelism(restClusterClient, jobId, vertex2.getID());
            assertThat(parallelism2).isEqualTo(2);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    private static int getVertexParallelism(
            RestClusterClient<?> client, JobID jobId, JobVertexID vertexId) {
        JobDetailsInfo jobDetails = client.getJobDetails(jobId).join();
        return jobDetails.getJobVertexInfos().stream()
                .filter(v -> v.getJobVertexID().equals(vertexId))
                .findFirst()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getParallelism)
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "Vertex " + vertexId + " not found in job details"));
    }

    private static void waitForRunningTasks(
            RestClusterClient<?> client, JobID jobId, int expectedRunningTasks) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> getNumberRunningTasks(client, jobId) == expectedRunningTasks);
    }

    private static int getNumberRunningTasks(RestClusterClient<?> client, JobID jobId) {
        JobDetailsInfo jobDetails = client.getJobDetails(jobId).join();
        return jobDetails.getJobVertexInfos().stream()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getTasksPerState)
                .map(tasksPerState -> tasksPerState.get(ExecutionState.RUNNING))
                .mapToInt(Integer::intValue)
                .sum();
    }
}

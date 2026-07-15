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

package org.apache.flink.runtime.scheduler.adaptive.timeline;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testtasks.OnceBlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.util.RestartStrategyUtils;

import org.junit.jupiter.api.AfterEach;

import java.time.Duration;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared fixture and helpers for the {@link RescaleTimeline} integration tests, reused by {@link
 * RescaleTimelineITCase} and {@link RescaleTimelineHistoryEnabledITCase}.
 */
abstract class RescaleTimelineITCaseBase {

    protected static final int NUMBER_SLOTS_PER_TASK_MANAGER = 2;
    protected static final int NUMBER_TASK_MANAGERS = 2;
    protected static final int PARALLELISM = NUMBER_SLOTS_PER_TASK_MANAGER * NUMBER_TASK_MANAGERS;
    protected static final JobVertexID JOB_VERTEX_ID = new JobVertexID();

    protected MiniClusterResource miniClusterResource;

    protected static Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(50));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                // Use the 0.1 seconds to trigger the long-time non-terminal rescale event after a
                // rescaling.
                Duration.ofMillis(100));
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ofMillis(50));
        configuration.set(
                JobManagerOptions.SCHEDULER_SUBMISSION_RESOURCE_WAIT_TIMEOUT,
                Duration.ofSeconds(2));
        return configuration;
    }

    /** Builds and starts the fixture cluster with the given configuration. */
    protected void startCluster(Configuration configuration) throws Exception {
        miniClusterResource =
                new MiniClusterResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberSlotsPerTaskManager(NUMBER_SLOTS_PER_TASK_MANAGER)
                                .setNumberTaskManagers(NUMBER_TASK_MANAGERS)
                                .build());
        miniClusterResource.before();
    }

    @AfterEach
    void tearDown() {
        if (miniClusterResource != null) {
            miniClusterResource.after();
        }
    }

    protected JobGraph createBlockingJobGraph(int parallelism) {
        final JobVertex blockingOperator = new JobVertex("Blocking operator", JOB_VERTEX_ID);
        SlotSharingGroup sharingGroup = new SlotSharingGroup();
        sharingGroup.setSlotSharingGroupName("slot-sharing-group-A");
        blockingOperator.setSlotSharingGroup(sharingGroup);
        blockingOperator.setInvokableClass(OnceBlockingNoOpInvokable.class);
        blockingOperator.setParallelism(parallelism);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(blockingOperator);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(jobGraph, 1, 0L);
        return jobGraph;
    }

    protected void waitForVertexParallelismReachedAndJobRunning(
            JobGraph jobGraph, JobVertexID jobVertexId, int targetParallelism) throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph archivedExecutionGraph =
                            miniClusterResource
                                    .getMiniCluster()
                                    .getArchivedExecutionGraph(jobGraph.getJobID())
                                    .get();
                    final AccessExecutionJobVertex executionJobVertex =
                            archivedExecutionGraph.getAllVertices().get(jobVertexId);
                    if (executionJobVertex == null) {
                        // parallelism was not yet determined
                        return false;
                    }
                    return executionJobVertex.getParallelism() == targetParallelism;
                });
        CommonTestUtils.waitUntilCondition(
                () ->
                        miniClusterResource.getMiniCluster().getJobStatus(jobGraph.getJobID()).get()
                                == JobStatus.RUNNING);
    }

    protected void updateJobResourceRequirements(
            MiniCluster miniCluster, JobGraph jobGraph, int lowerBound, int upperBound)
            throws ExecutionException, InterruptedException {
        miniCluster
                .updateJobResourceRequirements(
                        jobGraph.getJobID(),
                        JobResourceRequirements.newBuilder()
                                .setParallelismForJobVertex(
                                        jobGraph.getVertices().iterator().next().getID(),
                                        lowerBound,
                                        upperBound)
                                .build())
                .get();
    }

    protected static void assertTerminalRelatedFields(
            Rescale rescale, TerminatedReason expectedTerminatedReason) {
        TerminatedReason terminatedReason = rescale.getTerminatedReason();
        assertThat(terminatedReason).isEqualTo(expectedTerminatedReason);
        assertThat(terminatedReason.getTerminalState())
                .isEqualTo(rescale.getTerminalState())
                .isEqualTo(expectedTerminatedReason.getTerminalState());
    }
}

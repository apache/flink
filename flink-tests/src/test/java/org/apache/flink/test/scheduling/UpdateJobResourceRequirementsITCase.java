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
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.runtime.testtasks.BlockingNoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Tests for the manual rescaling of Flink jobs using the REST API. */
@ExtendWith(TestLoggerExtension.class)
public class UpdateJobResourceRequirementsITCase {

    private static final int NUMBER_OF_SLOTS = 4;

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
        configuration.set(WebOptions.REFRESH_INTERVAL, 50L);
        configuration.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, 50L);

        return configuration;
    }

    private RestClusterClient<?> restClusterClient;

    @BeforeEach
    void beforeEach(@InjectClusterClient RestClusterClient<?> restClusterClient) {
        this.restClusterClient = restClusterClient;
    }

    @Test
    void testManualUpScalingWithNewSlotAllocation() throws Exception {
        final JobVertex jobVertex = new JobVertex("Single operator");
        final int initialParallelism = 1;
        final int parallelismAfterRescaling = 2;
        jobVertex.setParallelism(initialParallelism);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);

        runRescalingTest(
                jobGraph,
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(jobVertex.getID(), 1, parallelismAfterRescaling)
                        .build(),
                initialParallelism,
                parallelismAfterRescaling,
                NUMBER_OF_SLOTS - parallelismAfterRescaling);
    }

    @Test
    void testManualUpScalingWithNoNewSlotAllocation() throws Exception {
        final int initialRunningTasks = 3;
        final int runningTasksAfterRescale = 4;

        final JobVertex jobVertex1 = new JobVertex("Operator1");
        jobVertex1.setParallelism(1);
        jobVertex1.setInvokableClass(BlockingNoOpInvokable.class);
        final JobVertex jobVertex2 = new JobVertex("Operator2");
        jobVertex2.setParallelism(2);
        jobVertex2.setInvokableClass(BlockingNoOpInvokable.class);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        jobVertex1.setSlotSharingGroup(slotSharingGroup);
        jobVertex2.setSlotSharingGroup(slotSharingGroup);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex1, jobVertex2);

        runRescalingTest(
                jobGraph,
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(jobVertex1.getID(), 1, 2)
                        .setParallelismForJobVertex(jobVertex2.getID(), 1, 2)
                        .build(),
                initialRunningTasks,
                runningTasksAfterRescale,
                NUMBER_OF_SLOTS - 2);
    }

    @Test
    void testManualUpScalingWithDifferentSlotSharingGroups() throws Exception {
        final int initialParallelism = 1;
        final int desiredParallelism = 2;

        // the job is going to have two slot sharing groups
        final int initialRunningTasks = 2 * initialParallelism;
        final int runningTasksAfterRescale = 2 * desiredParallelism;

        final JobVertex jobVertex1 = new JobVertex("Operator1");
        jobVertex1.setParallelism(initialParallelism);
        jobVertex1.setInvokableClass(BlockingNoOpInvokable.class);
        final JobVertex jobVertex2 = new JobVertex("Operator2");
        jobVertex2.setParallelism(initialParallelism);
        jobVertex2.setInvokableClass(BlockingNoOpInvokable.class);

        jobVertex1.setSlotSharingGroup(new SlotSharingGroup());
        jobVertex2.setSlotSharingGroup(new SlotSharingGroup());

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex1, jobVertex2);

        runRescalingTest(
                jobGraph,
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(
                                jobVertex1.getID(), initialParallelism, desiredParallelism)
                        .setParallelismForJobVertex(
                                jobVertex2.getID(), initialParallelism, desiredParallelism)
                        .build(),
                initialRunningTasks,
                runningTasksAfterRescale,
                NUMBER_OF_SLOTS - jobGraph.getSlotSharingGroups().size() * desiredParallelism);
    }

    @Test
    void testManualDownScaling() throws Exception {
        final int initialRunningTasks = 2;
        final int runningTasksAfterRescale = 1;

        final JobVertex jobVertex = new JobVertex("Operator");
        jobVertex.setParallelism(initialRunningTasks);
        jobVertex.setInvokableClass(BlockingNoOpInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(jobVertex);

        runRescalingTest(
                jobGraph,
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(jobVertex.getID(), 1, runningTasksAfterRescale)
                        .build(),
                initialRunningTasks,
                runningTasksAfterRescale,
                NUMBER_OF_SLOTS - runningTasksAfterRescale);
    }

    private void runRescalingTest(
            JobGraph jobGraph,
            JobResourceRequirements newJobVertexParallelism,
            int initialRunningTasks,
            int runningTasksAfterRescale,
            int freeSlotsAfterRescale)
            throws Exception {
        restClusterClient.submitJob(jobGraph).join();
        try {
            final JobID jobId = jobGraph.getJobID();

            waitForRunningTasks(restClusterClient, jobId, initialRunningTasks);

            restClusterClient.updateJobResourceRequirements(jobId, newJobVertexParallelism).join();

            waitForRunningTasks(restClusterClient, jobId, runningTasksAfterRescale);
            waitForAvailableSlots(restClusterClient, freeSlotsAfterRescale);
        } finally {
            restClusterClient.cancel(jobGraph.getJobID()).join();
        }
    }

    public static int getNumberRunningTasks(RestClusterClient<?> restClusterClient, JobID jobId) {
        final JobDetailsInfo jobDetailsInfo = restClusterClient.getJobDetails(jobId).join();
        return jobDetailsInfo.getJobVertexInfos().stream()
                .map(JobDetailsInfo.JobVertexDetailsInfo::getTasksPerState)
                .map(tasksPerState -> tasksPerState.get(ExecutionState.RUNNING))
                .mapToInt(Integer::intValue)
                .sum();
    }

    public static void waitForRunningTasks(
            RestClusterClient<?> restClusterClient, JobID jobId, int desiredNumberOfRunningTasks)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final int numberOfRunningTasks =
                            getNumberRunningTasks(restClusterClient, jobId);
                    return numberOfRunningTasks == desiredNumberOfRunningTasks;
                });
    }

    public static void waitForAvailableSlots(
            RestClusterClient<?> restClusterClient, int desiredNumberOfAvailableSlots)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ClusterOverviewWithVersion clusterOverview =
                            restClusterClient.getClusterOverview().join();
                    return clusterOverview.getNumSlotsAvailable() == desiredNumberOfAvailableSlots;
                });
    }
}

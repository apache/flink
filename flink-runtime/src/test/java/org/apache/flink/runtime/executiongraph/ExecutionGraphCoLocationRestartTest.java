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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import static org.apache.flink.api.common.JobStatus.FINISHED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests that co-location constraints work as expected in the case of task restarts. */
class ExecutionGraphCoLocationRestartTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final int NUM_TASKS = 31;

    @Test
    void testConstraintsAfterRestart() throws Exception {

        final long timeout = 5000L;

        JobVertex groupVertex = ExecutionGraphTestUtils.createNoOpVertex(NUM_TASKS);
        JobVertex groupVertex2 = ExecutionGraphTestUtils.createNoOpVertex(NUM_TASKS);
        groupVertex2.connectNewDataSetAsInput(
                groupVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        SlotSharingGroup sharingGroup = new SlotSharingGroup();
        groupVertex.setSlotSharingGroup(sharingGroup);
        groupVertex2.setSlotSharingGroup(sharingGroup);
        groupVertex.setStrictlyCoLocatedWith(groupVertex2);

        // initiate and schedule job
        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(groupVertex, groupVertex2);

        final ManuallyTriggeredScheduledExecutorService delayExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        TestingPhysicalSlotProvider.create(
                                                (ignored) ->
                                                        CompletableFuture.completedFuture(
                                                                TestingPhysicalSlot.builder()
                                                                        .build()))))
                        .setDelayExecutor(delayExecutor)
                        .setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategy
                                                .FixedDelayRestartBackoffTimeStrategyFactory(1, 0)
                                        .create())
                        .build();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        // enable the queued scheduling for the slot pool
        assertThat(eg.getState()).isEqualTo(JobStatus.CREATED);

        scheduler.startScheduling();

        Predicate<AccessExecution> isDeploying =
                ExecutionGraphTestUtils.isInExecutionState(ExecutionState.DEPLOYING);
        ExecutionGraphTestUtils.waitForAllExecutionsPredicate(eg, isDeploying, timeout);

        assertThat(eg.getState()).isEqualTo(JobStatus.RUNNING);

        // sanity checks
        validateConstraints(eg);

        eg.getAllExecutionVertices().iterator().next().fail(new FlinkException("Test exception"));

        assertThat(eg.getState()).isEqualTo(JobStatus.RESTARTING);

        // trigger registration of restartTasks(...) callback to cancelFuture before completing the
        // cancellation. This ensures the restarting actions to be performed in main thread.
        delayExecutor.triggerNonPeriodicScheduledTask();

        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            if (vertex.getExecutionState() == ExecutionState.CANCELING) {
                vertex.getCurrentExecutionAttempt().completeCancelling();
            }
        }

        // wait until we have restarted
        ExecutionGraphTestUtils.waitUntilJobStatus(eg, JobStatus.RUNNING, timeout);

        ExecutionGraphTestUtils.waitForAllExecutionsPredicate(eg, isDeploying, timeout);

        // checking execution vertex properties
        validateConstraints(eg);

        ExecutionGraphTestUtils.finishAllVertices(eg);

        assertThat(eg.getState()).isEqualTo(FINISHED);
    }

    private void validateConstraints(ExecutionGraph eg) {

        ExecutionJobVertex[] tasks =
                eg.getAllVertices().values().toArray(new ExecutionJobVertex[2]);

        for (int i = 0; i < NUM_TASKS; i++) {
            TaskManagerLocation taskManagerLocation0 =
                    tasks[0].getTaskVertices()[i].getCurrentAssignedResourceLocation();
            TaskManagerLocation taskManagerLocation1 =
                    tasks[1].getTaskVertices()[i].getCurrentAssignedResourceLocation();

            assertThat(taskManagerLocation0).isEqualTo(taskManagerLocation1);
        }
    }
}

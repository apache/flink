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

import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Execution}. */
class ExecutionTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    static final TestingComponentMainThreadExecutor.Extension MAIN_EXECUTOR_RESOURCE =
            new TestingComponentMainThreadExecutor.Extension();

    private final TestingComponentMainThreadExecutor testMainThreadUtil =
            MAIN_EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

    /**
     * Checks that the {@link Execution} termination future is only completed after the assigned
     * slot has been released.
     *
     * <p>NOTE: This test only fails spuriously without the fix of this commit. Thus, one has to
     * execute this test multiple times to see the failure.
     */
    @Test
    void testTerminationFutureIsCompletedAfterSlotRelease() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();
        final JobVertexID jobVertexId = jobVertex.getID();

        final TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.createWithLimitedAmountOfPhysicalSlots(1);
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        physicalSlotProvider))
                        .build();

        ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

        ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

        scheduler.startScheduling();

        Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

        CompletableFuture<? extends PhysicalSlot> returnedSlotFuture =
                physicalSlotProvider.getFirstResponseOrFail();
        CompletableFuture<?> terminationFuture = executionVertex.cancel();

        currentExecutionAttempt.completeCancelling();

        CompletableFuture<Boolean> restartFuture =
                terminationFuture.thenApply(
                        ignored -> {
                            assertThat(returnedSlotFuture).isDone();
                            return true;
                        });

        // check if the returned slot future was completed first
        restartFuture.get();
    }

    /**
     * Tests that the task restore state is nulled after the {@link Execution} has been deployed.
     * See FLINK-9693.
     */
    @Test
    void testTaskRestoreStateIsNulledAfterDeployment() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();
        final JobVertexID jobVertexId = jobVertex.getID();

        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        TestingPhysicalSlotProvider
                                                .createWithLimitedAmountOfPhysicalSlots(1)))
                        .build();

        ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

        ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

        final Execution execution = executionVertex.getCurrentExecutionAttempt();

        final JobManagerTaskRestore taskRestoreState =
                new JobManagerTaskRestore(1L, new TaskStateSnapshot());
        execution.setInitialState(taskRestoreState);

        assertThat(execution.getTaskRestore()).isNotNull();

        // schedule the execution vertex and wait for its deployment
        scheduler.startScheduling();

        assertThat(execution.getTaskRestore()).isNull();
    }

    @Test
    void testCanceledExecutionReturnsSlot() throws Exception {

        final JobVertex jobVertex = createNoOpJobVertex();
        final JobVertexID jobVertexId = jobVertex.getID();

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();

        TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.create(
                        (resourceProfile) ->
                                CompletableFuture.completedFuture(
                                        TestingPhysicalSlot.builder()
                                                .withTaskManagerGateway(taskManagerGateway)
                                                .build()));
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                testMainThreadUtil.getMainThreadExecutor(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        physicalSlotProvider))
                        .build();

        ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

        ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

        final Execution execution = executionVertex.getCurrentExecutionAttempt();

        taskManagerGateway.setCancelConsumer(
                executionAttemptID -> {
                    if (execution.getAttemptId().equals(executionAttemptID)) {
                        execution.completeCancelling();
                    }
                });

        testMainThreadUtil.execute(scheduler::startScheduling);

        // cancel the execution in case we could schedule the execution
        testMainThreadUtil.execute(execution::cancel);

        assertThat(physicalSlotProvider.getRequests().keySet())
                .isEqualTo(physicalSlotProvider.getCancellations().keySet());
    }

    /** Tests that a slot release will atomically release the assigned {@link Execution}. */
    @Test
    void testSlotReleaseAtomicallyReleasesExecution() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();

        final TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.createWithLimitedAmountOfPhysicalSlots(1);
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                testMainThreadUtil.getMainThreadExecutor(),
                                EXECUTOR_RESOURCE.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        physicalSlotProvider))
                        .build();

        final Execution execution =
                scheduler
                        .getExecutionJobVertex(jobVertex.getID())
                        .getTaskVertices()[0]
                        .getCurrentExecutionAttempt();

        testMainThreadUtil.execute(scheduler::startScheduling);

        // wait until the slot has been requested
        physicalSlotProvider.awaitAllSlotRequests();

        TestingPhysicalSlot physicalSlot = physicalSlotProvider.getFirstResponseOrFail().get();
        testMainThreadUtil.execute(
                () -> {
                    assertThat(execution.getAssignedAllocationID())
                            .isEqualTo(physicalSlot.getAllocationId());

                    physicalSlot.releasePayload(new FlinkException("Test exception"));

                    assertThat(execution.getReleaseFuture()).isDone();
                });
    }

    @Nonnull
    private JobVertex createNoOpJobVertex() {
        final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
        jobVertex.setInvokableClass(NoOpInvokable.class);

        return jobVertex;
    }
}

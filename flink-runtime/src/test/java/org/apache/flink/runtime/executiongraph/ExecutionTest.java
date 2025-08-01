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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
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

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
                                testMainThreadUtil.getMainThreadExecutor(),
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
        testMainThreadUtil.execute(scheduler::startScheduling);

        // After the execution has been deployed, the task restore state should be nulled.
        while (execution.getTaskRestore() != null) {
            // Using busy waiting because there is no `future` that would indicate the completion
            // of the deployment and I want to keep production code clean.
            Thread.sleep(10);
        }
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

    @Test
    void testExecutionCancelledDuringTaskRestoreOffload() {
        final CountDownLatch offloadLatch = new CountDownLatch(1);

        final Execution cancelledExecution =
                testMainThreadUtil.execute(
                        () -> {
                            final ConditionalLatchBlockingBlobWriter blobWriter =
                                    new ConditionalLatchBlockingBlobWriter(offloadLatch);

                            final JobVertex jobVertex = createNoOpJobVertex();
                            final JobGraph jobGraph =
                                    JobGraphTestUtils.streamingJobGraph(jobVertex);
                            final ExecutionGraph executionGraph =
                                    TestingDefaultExecutionGraphBuilder.newBuilder()
                                            .setJobGraph(jobGraph)
                                            .setBlobWriter(blobWriter)
                                            .build(EXECUTOR_RESOURCE.getExecutor());
                            executionGraph.start(testMainThreadUtil.getMainThreadExecutor());

                            final ExecutionJobVertex executionJobVertex =
                                    executionGraph.getJobVertex(jobVertex.getID());
                            final ExecutionVertex executionVertex =
                                    executionJobVertex.getTaskVertices()[0];
                            final Execution execution =
                                    executionVertex.getCurrentExecutionAttempt();

                            final JobManagerTaskRestore taskRestoreState =
                                    new JobManagerTaskRestore(1L, new TaskStateSnapshot());
                            execution.setInitialState(taskRestoreState);
                            execution.tryAssignResource(
                                    new TestingLogicalSlotBuilder().createTestingLogicalSlot());
                            execution.transitionState(ExecutionState.SCHEDULED);

                            blobWriter.enableBlocking();
                            execution.deploy();
                            execution.cancel();

                            return execution;
                        });

        offloadLatch.countDown();

        cancelledExecution
                .getTddCreationDuringDeployFuture()
                .handle(
                        (result, exception) -> {
                            assertThat(exception)
                                    .isNotNull()
                                    .describedAs("Expected IllegalStateException to be thrown");

                            final Throwable rootCause = exception.getCause();
                            assertThat(rootCause).isInstanceOf(IllegalStateException.class);
                            assertThat(rootCause.getMessage())
                                    .contains("execution state has switched");
                            assertThat(rootCause.getMessage())
                                    .contains("during task restore offload");
                            return null;
                        })
                .join();
    }

    /**
     * Custom BlobWriter that conditionally blocks putPermanent calls on a latch to simulate slow
     * offloading.
     */
    private static class ConditionalLatchBlockingBlobWriter extends TestingBlobWriter {
        private final CountDownLatch offloadLatch;
        private volatile boolean blockingEnabled = false;

        public ConditionalLatchBlockingBlobWriter(CountDownLatch offloadLatch) {
            super();
            this.offloadLatch = offloadLatch;
        }

        public void enableBlocking() {
            this.blockingEnabled = true;
        }

        @Override
        public PermanentBlobKey putPermanent(JobID jobId, InputStream inputStream)
                throws IOException {
            if (blockingEnabled) {
                try {
                    // Block until the latch is released
                    offloadLatch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for offload latch", e);
                }
            }
            // Delegate to parent implementation
            return super.putPermanent(jobId, inputStream);
        }
    }

    @Nonnull
    private JobVertex createNoOpJobVertex() {
        final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
        jobVertex.setInvokableClass(NoOpInvokable.class);

        return jobVertex;
    }
}

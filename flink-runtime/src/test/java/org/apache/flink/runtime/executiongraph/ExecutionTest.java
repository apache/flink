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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link Execution}. */
public class ExecutionTest extends TestLogger {

    @ClassRule
    public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
            new TestingComponentMainThreadExecutor.Resource();

    private final TestingComponentMainThreadExecutor testMainThreadUtil =
            EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

    /**
     * Checks that the {@link Execution} termination future is only completed after the assigned
     * slot has been released.
     *
     * <p>NOTE: This test only fails spuriously without the fix of this commit. Thus, one has to
     * execute this test multiple times to see the failure.
     */
    @Test
    public void testTerminationFutureIsCompletedAfterSlotRelease() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();
        final JobVertexID jobVertexId = jobVertex.getID();

        final TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.createWithLimitedAmountOfPhysicalSlots(1);
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
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
                            assertTrue(returnedSlotFuture.isDone());
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
    public void testTaskRestoreStateIsNulledAfterDeployment() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();
        final JobVertexID jobVertexId = jobVertex.getID();

        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
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

        assertThat(execution.getTaskRestore(), is(notNullValue()));

        // schedule the execution vertex and wait for its deployment
        scheduler.startScheduling();

        assertThat(execution.getTaskRestore(), is(nullValue()));
    }

    @Test
    public void testCanceledExecutionReturnsSlot() throws Exception {

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
                SchedulerTestingUtils.newSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                testMainThreadUtil.getMainThreadExecutor())
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

        assertThat(
                physicalSlotProvider.getRequests().keySet(),
                is(physicalSlotProvider.getCancellations().keySet()));
    }

    /** Tests that a slot release will atomically release the assigned {@link Execution}. */
    @Test
    public void testSlotReleaseAtomicallyReleasesExecution() throws Exception {
        final JobVertex jobVertex = createNoOpJobVertex();

        final TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.createWithLimitedAmountOfPhysicalSlots(1);
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(jobVertex),
                                testMainThreadUtil.getMainThreadExecutor())
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
                    assertThat(
                            execution.getAssignedAllocationID(),
                            is(physicalSlot.getAllocationId()));

                    physicalSlot.releasePayload(new FlinkException("Test exception"));

                    assertThat(execution.getReleaseFuture().isDone(), is(true));
                });
    }

    /**
     * Tests that incomplete futures returned by {@link ShuffleMaster#registerPartitionWithProducer}
     * are rejected.
     */
    @Test
    public void testIncompletePartitionRegistrationFutureIsRejected() throws Exception {
        final ShuffleMaster<ShuffleDescriptor> shuffleMaster = new TestingShuffleMaster();
        final JobVertex source = new JobVertex("source");
        final JobVertex target = new JobVertex("target");

        source.setInvokableClass(AbstractInvokable.class);
        target.setInvokableClass(AbstractInvokable.class);
        target.connectNewDataSetAsInput(source, POINTWISE, PIPELINED);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(source, target);
        ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setShuffleMaster(shuffleMaster)
                        .build();

        final ExecutionVertex sourceVertex =
                executionGraph.getAllVertices().get(source.getID()).getTaskVertices()[0];

        boolean incompletePartitionRegistrationRejected = false;
        try {
            Execution.registerProducedPartitions(
                    sourceVertex, new LocalTaskManagerLocation(), new ExecutionAttemptID(), false);
        } catch (IllegalStateException e) {
            incompletePartitionRegistrationRejected = true;
        }

        assertTrue(incompletePartitionRegistrationRejected);
    }

    private static class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

        @Override
        public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(
                PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
            return new CompletableFuture<>();
        }

        @Override
        public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {}
    }

    @Nonnull
    private JobVertex createNoOpJobVertex() {
        final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
        jobVertex.setInvokableClass(NoOpInvokable.class);

        return jobVertex;
    }
}

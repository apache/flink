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
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link Execution}.
 */
public class ExecutionTest extends TestLogger {

	@ClassRule
	public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
		new TestingComponentMainThreadExecutor.Resource();

	private final TestingComponentMainThreadExecutor testMainThreadUtil =
		EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

	private TestingLogicalSlot createTestingLogicalSlot(SlotOwner slotOwner) {
		return new TestingLogicalSlotBuilder()
			.setSlotOwner(slotOwner)
			.createTestingLogicalSlot();
	}

	/**
	 * Checks that the {@link Execution} termination future is only completed after the
	 * assigned slot has been released.
	 *
	 * <p>NOTE: This test only fails spuriously without the fix of this commit. Thus, one has
	 * to execute this test multiple times to see the failure.
	 */
	@Test
	public void testTerminationFutureIsCompletedAfterSlotRelease() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId),
			slotOwner);

		final SchedulerBase scheduler = SchedulerTestingUtils
			.newSchedulerBuilderWithDefaultSlotAllocator(new JobGraph(jobVertex), slotProvider)
			.build();

		scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		scheduler.startScheduling();

		Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();

		CompletableFuture<LogicalSlot> returnedSlotFuture = slotOwner.getReturnedSlotFuture();
		CompletableFuture<?> terminationFuture = executionVertex.cancel();

		currentExecutionAttempt.completeCancelling();

		CompletableFuture<Boolean> restartFuture = terminationFuture.thenApply(
			ignored -> {
				assertTrue(returnedSlotFuture.isDone());
				return true;
			});

		// check if the returned slot future was completed first
		restartFuture.get();
	}

	/**
	 * Tests that the task restore state is nulled after the {@link Execution} has been
	 * deployed. See FLINK-9693.
	 */
	@Test
	public void testTaskRestoreStateIsNulledAfterDeployment() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();
		final JobVertexID jobVertexId = jobVertex.getID();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId),
			slotOwner);

		final SchedulerBase scheduler = SchedulerTestingUtils
			.newSchedulerBuilderWithDefaultSlotAllocator(new JobGraph(jobVertex), slotProvider)
			.build();

		scheduler.initialize(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		final Execution execution = executionVertex.getCurrentExecutionAttempt();

		final JobManagerTaskRestore taskRestoreState = new JobManagerTaskRestore(1L, new TaskStateSnapshot());
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

		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();

		final CompletableFuture<SlotRequestId> slotRequestIdFuture = new CompletableFuture<>();
		final CompletableFuture<SlotRequestId> returnedSlotFuture = new CompletableFuture<>();

		final TestingSlotProvider slotProvider = new TestingSlotProvider(
			(SlotRequestId slotRequestId) -> {
				slotRequestIdFuture.complete(slotRequestId);
				return new CompletableFuture<>();
			});

		slotProvider.setSlotCanceller(returnedSlotFuture::complete);
		slotOwner.getReturnedSlotFuture().thenAccept(
			(LogicalSlot logicalSlot) -> returnedSlotFuture.complete(logicalSlot.getSlotRequestId()));

		final SchedulerBase scheduler = SchedulerTestingUtils
			.newSchedulerBuilderWithDefaultSlotAllocator(new JobGraph(jobVertex), slotProvider)
			.build();

		scheduler.initialize(testMainThreadUtil.getMainThreadExecutor());

		ExecutionJobVertex executionJobVertex = scheduler.getExecutionJobVertex(jobVertexId);

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		final Execution execution = executionVertex.getCurrentExecutionAttempt();

		taskManagerGateway.setCancelConsumer(
			executionAttemptID -> {
				if (execution.getAttemptId().equals(executionAttemptID)) {
					execution.completeCancelling();
				}
			}
		);

		slotRequestIdFuture.thenAcceptAsync(
			(SlotRequestId slotRequestId) -> {
				final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot(
					slotOwner,
					taskManagerGateway,
					slotRequestId);
				slotProvider.complete(slotRequestId, singleLogicalSlot);
			},
			testMainThreadUtil.getMainThreadExecutor());

		testMainThreadUtil.execute(scheduler::startScheduling);

		// cancel the execution in case we could schedule the execution
		testMainThreadUtil.execute(execution::cancel);

		assertThat(returnedSlotFuture.get(), is(equalTo(slotRequestIdFuture.get())));
	}

	/**
	 * Tests that a slot release will atomically release the assigned {@link Execution}.
	 */
	@Test
	public void testSlotReleaseAtomicallyReleasesExecution() throws Exception {
		final JobVertex jobVertex = createNoOpJobVertex();

		final SingleSlotTestingSlotOwner slotOwner = new SingleSlotTestingSlotOwner();
		final SingleLogicalSlot slot = createSingleLogicalSlot(
			slotOwner,
			new SimpleAckingTaskManagerGateway(),
			new SlotRequestId());
		final CompletableFuture<LogicalSlot> slotFuture = CompletableFuture.completedFuture(slot);

		final CountDownLatch slotRequestLatch = new CountDownLatch(1);
		final TestingSlotProvider slotProvider = new TestingSlotProvider(slotRequestId -> {
			slotRequestLatch.countDown();
			return slotFuture;
		});

		final SchedulerBase scheduler = SchedulerTestingUtils
			.newSchedulerBuilderWithDefaultSlotAllocator(new JobGraph(jobVertex), slotProvider)
			.build();

		final Execution execution = scheduler.getExecutionJobVertex(jobVertex.getID()).getTaskVertices()[0].getCurrentExecutionAttempt();

		scheduler.initialize(testMainThreadUtil.getMainThreadExecutor());
		testMainThreadUtil.execute(scheduler::startScheduling);

		// wait until the slot has been requested
		slotRequestLatch.await();

		testMainThreadUtil.execute(() -> {
			assertThat(execution.getAssignedResource(), is(sameInstance(slot)));

			slot.release(new FlinkException("Test exception"));

			assertThat(execution.getReleaseFuture().isDone(), is(true));
		});
	}

	/**
	 * Tests that incomplete futures returned by {@link ShuffleMaster#registerPartitionWithProducer} are rejected.
	 */
	@Test
	public void testIncompletePartitionRegistrationFutureIsRejected() throws Exception {
		final ShuffleMaster<ShuffleDescriptor> shuffleMaster = new TestingShuffleMaster();
		final JobGraph jobGraph = new JobGraph("job graph");
		final JobVertex source = new JobVertex("source");
		final JobVertex target = new JobVertex("target");

		source.setInvokableClass(AbstractInvokable.class);
		target.setInvokableClass(AbstractInvokable.class);
		target.connectNewDataSetAsInput(source, POINTWISE, PIPELINED);
		jobGraph.addVertex(source);
		jobGraph.addVertex(target);
		ExecutionGraph executionGraph = TestingExecutionGraphBuilder
			.newBuilder()
			.setJobGraph(jobGraph)
			.setShuffleMaster(shuffleMaster)
			.build();

		final ExecutionVertex sourceVertex = executionGraph.getAllVertices().get(source.getID()).getTaskVertices()[0];

		boolean incompletePartitionRegistrationRejected = false;
		try {
			Execution.registerProducedPartitions(sourceVertex, new LocalTaskManagerLocation(), new ExecutionAttemptID(), false);
		} catch (IllegalStateException e) {
			incompletePartitionRegistrationRejected = true;
		}

		assertTrue(incompletePartitionRegistrationRejected);
	}

	private static class TestingShuffleMaster implements ShuffleMaster<ShuffleDescriptor> {

		@Override
		public CompletableFuture<ShuffleDescriptor> registerPartitionWithProducer(PartitionDescriptor partitionDescriptor, ProducerDescriptor producerDescriptor) {
			return new CompletableFuture<>();
		}

		@Override
		public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
		}
	}

	@Nonnull
	private JobVertex createNoOpJobVertex() {
		final JobVertex jobVertex = new JobVertex("Test vertex", new JobVertexID());
		jobVertex.setInvokableClass(NoOpInvokable.class);

		return jobVertex;
	}

	@Nonnull
	private ProgrammedSlotProvider createProgrammedSlotProvider(
		int parallelism,
		Collection<JobVertexID> jobVertexIds,
		SlotOwner slotOwner) {
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);

		for (JobVertexID jobVertexId : jobVertexIds) {
			for (int i = 0; i < parallelism; i++) {
				final LogicalSlot slot = createTestingLogicalSlot(slotOwner);

				slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));
			}
		}

		return slotProvider;
	}

	static SingleLogicalSlot createSingleLogicalSlot(
			SlotOwner slotOwner,
			TaskManagerGateway taskManagerGateway,
			SlotRequestId slotRequestId) {

		TaskManagerLocation location = new TaskManagerLocation(
			ResourceID.generate(), InetAddress.getLoopbackAddress(), 12345);

		SimpleSlotContext slotContext = new SimpleSlotContext(
			new AllocationID(),
			location,
			0,
			taskManagerGateway);

		return new SingleLogicalSlot(
			slotRequestId,
			slotContext,
			null,
			Locality.LOCAL,
			slotOwner);
	}


	/**
	 * Slot owner which records the first returned slot.
	 */
	private static final class SingleSlotTestingSlotOwner implements SlotOwner {

		final CompletableFuture<LogicalSlot> returnedSlot = new CompletableFuture<>();

		public CompletableFuture<LogicalSlot> getReturnedSlotFuture() {
			return returnedSlot;
		}

		@Override
		public void returnLogicalSlot(LogicalSlot logicalSlot) {
			returnedSlot.complete(logicalSlot);
		}
	}
}

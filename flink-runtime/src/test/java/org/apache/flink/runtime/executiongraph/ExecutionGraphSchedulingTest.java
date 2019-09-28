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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.io.network.partition.NoOpPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TestingSlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.net.InetAddress;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for the scheduling of the execution graph. This tests that
 * for example the order of deployments is correct and that bulk slot allocation
 * works properly.
 */
public class ExecutionGraphSchedulingTest extends TestLogger {

	private final ScheduledExecutorService executor = new DirectScheduledExecutorService();

	@After
	public void shutdown() {
		executor.shutdownNow();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Tests that with scheduling futures and pipelined deployment, the target vertex will
	 * not deploy its task before the source vertex does.
	 */
	@Test
	public void testScheduleSourceBeforeTarget() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 1;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		jobGraph.setAllowQueuedScheduling(true);

		final CompletableFuture<LogicalSlot> sourceFuture = new CompletableFuture<>();
		final CompletableFuture<LogicalSlot> targetFuture = new CompletableFuture<>();

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlot(sourceVertex.getID(), 0, sourceFuture);
		slotProvider.addSlot(targetVertex.getID(), 0, targetFuture);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		//  set up two TaskManager gateways and slots

		final InteractionsCountingTaskManagerGateway gatewaySource = createTaskManager();
		final InteractionsCountingTaskManagerGateway gatewayTarget = createTaskManager();

		final LogicalSlot sourceSlot = createTestingLogicalSlot(gatewaySource);
		final LogicalSlot targetSlot = createTestingLogicalSlot(gatewayTarget);

		eg.scheduleForExecution();

		// job should be running
		assertEquals(JobStatus.RUNNING, eg.getState());

		// we fulfill the target slot before the source slot
		// that should not cause a deployment or deployment related failure
		targetFuture.complete(targetSlot);

		assertThat(gatewayTarget.getSubmitTaskCount(), is(0));
		assertEquals(JobStatus.RUNNING, eg.getState());

		// now supply the source slot
		sourceFuture.complete(sourceSlot);

		// by now, all deployments should have happened
		assertThat(gatewaySource.getSubmitTaskCount(), is(1));
		assertThat(gatewayTarget.getSubmitTaskCount(), is(1));

		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	private TestingLogicalSlot createTestingLogicalSlot(InteractionsCountingTaskManagerGateway gatewaySource) {
		return new TestingLogicalSlotBuilder()
			.setTaskManagerGateway(gatewaySource)
			.createTestingLogicalSlot();
	}

	/**
	 * This test verifies that before deploying a pipelined connected component, the
	 * full set of slots is available, and that not some tasks are deployed, and later the
	 * system realizes that not enough resources are available.
	 */
	@Test
	public void testDeployPipelinedConnectedComponentsTogether() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 8;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		jobGraph.setAllowQueuedScheduling(true);

		@SuppressWarnings({"unchecked", "rawtypes"})
		final CompletableFuture<LogicalSlot>[] sourceFutures = new CompletableFuture[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final CompletableFuture<LogicalSlot>[] targetFutures = new CompletableFuture[parallelism];

		//
		//  Create the slots, futures, and the slot provider

		final InteractionsCountingTaskManagerGateway[] sourceTaskManagers = new InteractionsCountingTaskManagerGateway[parallelism];
		final InteractionsCountingTaskManagerGateway[] targetTaskManagers = new InteractionsCountingTaskManagerGateway[parallelism];

		final LogicalSlot[] sourceSlots = new LogicalSlot[parallelism];
		final LogicalSlot[] targetSlots = new LogicalSlot[parallelism];

		for (int i = 0; i < parallelism; i++) {
			sourceTaskManagers[i] = createTaskManager();
			targetTaskManagers[i] = createTaskManager();

			sourceSlots[i] = createTestingLogicalSlot(sourceTaskManagers[i]);
			targetSlots[i] = createTestingLogicalSlot(targetTaskManagers[i]);

			sourceFutures[i] = new CompletableFuture<>();
			targetFutures[i] = new CompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(sourceVertex.getID(), sourceFutures);
		slotProvider.addSlots(targetVertex.getID(), targetFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		//
		//  we complete some of the futures

		for (int i = 0; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
		}

		//
		//  kick off the scheduling
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();

		verifyNothingDeployed(eg, sourceTaskManagers);

		//  complete the remaining sources
		for (int i = 1; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
		}
		verifyNothingDeployed(eg, sourceTaskManagers);

		//  complete the targets except for one
		for (int i = 1; i < parallelism; i++) {
			targetFutures[i].complete(targetSlots[i]);
		}
		verifyNothingDeployed(eg, targetTaskManagers);

		//  complete the last target slot future
		targetFutures[0].complete(targetSlots[0]);

		//
		//  verify that all deployments have happened

		for (InteractionsCountingTaskManagerGateway gateway : sourceTaskManagers) {
			assertThat(gateway.getSubmitTaskCount(), is(1));
		}
		for (InteractionsCountingTaskManagerGateway gateway : targetTaskManagers) {
			assertThat(gateway.getSubmitTaskCount(), is(1));
		}
	}

	/**
	 * This test verifies that if one slot future fails, the deployment will be aborted.
	 */
	@Test
	public void testOneSlotFailureAbortsDeploy() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 6;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		jobGraph.setAllowQueuedScheduling(true);

		//
		//  Create the slots, futures, and the slot provider

		final InteractionsCountingTaskManagerGateway taskManager = createTaskManager();
		final BlockingQueue<AllocationID> returnedSlots = new ArrayBlockingQueue<>(parallelism);
		final TestingSlotOwner slotOwner = new TestingSlotOwner();
		slotOwner.setReturnAllocatedSlotConsumer(
			(LogicalSlot logicalSlot) -> returnedSlots.offer(logicalSlot.getAllocationId()));

		final LogicalSlot[] sourceSlots = new LogicalSlot[parallelism];
		final LogicalSlot[] targetSlots = new LogicalSlot[parallelism];

		@SuppressWarnings({"unchecked", "rawtypes"})
		final CompletableFuture<LogicalSlot>[] sourceFutures = new CompletableFuture[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final CompletableFuture<LogicalSlot>[] targetFutures = new CompletableFuture[parallelism];

		for (int i = 0; i < parallelism; i++) {
			sourceSlots[i] = createSingleLogicalSlot(slotOwner, taskManager, new SlotRequestId());
			targetSlots[i] = createSingleLogicalSlot(slotOwner, taskManager, new SlotRequestId());

			sourceFutures[i] = new CompletableFuture<>();
			targetFutures[i] = new CompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(sourceVertex.getID(), sourceFutures);
		slotProvider.addSlots(targetVertex.getID(), targetFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		//
		//  we complete some of the futures

		for (int i = 0; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
			targetFutures[i].complete(targetSlots[i]);
		}

		//  kick off the scheduling
		eg.scheduleForExecution();

		// fail one slot
		sourceFutures[1].completeExceptionally(new TestRuntimeException());

		// wait until the job failed as a whole
		eg.getTerminationFuture().get(2000, TimeUnit.MILLISECONDS);

		// wait until all slots are back
		for (int i = 0; i < parallelism; i++) {
			returnedSlots.poll(2000L, TimeUnit.MILLISECONDS);
		}

		// no deployment calls must have happened
		assertThat(taskManager.getSubmitTaskCount(), is(0));

		// all completed futures must have been returns
		for (int i = 0; i < parallelism; i += 2) {
			assertFalse(sourceSlots[i].isAlive());
			assertFalse(targetSlots[i].isAlive());
		}
	}

	/**
	 * This tests makes sure that with eager scheduling no task is deployed if a single
	 * slot allocation fails. Moreover we check that allocated slots will be returned.
	 */
	@Test
	public void testEagerSchedulingWithSlotTimeout() throws Exception {

		//  we construct a simple graph:    (task)

		final int parallelism = 3;

		final JobVertex vertex = new JobVertex("task");
		vertex.setParallelism(parallelism);
		vertex.setInvokableClass(NoOpInvokable.class);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", vertex);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		jobGraph.setAllowQueuedScheduling(true);

		final BlockingQueue<AllocationID> returnedSlots = new ArrayBlockingQueue<>(2);
		final TestingSlotOwner slotOwner = new TestingSlotOwner();
		slotOwner.setReturnAllocatedSlotConsumer(
			(LogicalSlot logicalSlot) -> returnedSlots.offer(logicalSlot.getAllocationId()));

		final InteractionsCountingTaskManagerGateway taskManager = createTaskManager();
		final LogicalSlot[] slots = new LogicalSlot[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final CompletableFuture<LogicalSlot>[] slotFutures = new CompletableFuture[parallelism];

		for (int i = 0; i < parallelism; i++) {
			slots[i] = createSingleLogicalSlot(slotOwner, taskManager, new SlotRequestId());
			slotFutures[i] = new CompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(vertex.getID(), slotFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		//  we complete one future
		slotFutures[1].complete(slots[1]);

		//  kick off the scheduling
		eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		eg.scheduleForExecution();

		//  we complete another future
		slotFutures[2].complete(slots[2]);

		// check that the ExecutionGraph is not terminated yet
		assertThat(eg.getTerminationFuture().isDone(), is(false));

		// time out one of the slot futures
		slotFutures[0].completeExceptionally(new TimeoutException("Test time out"));

		assertThat(eg.getTerminationFuture().get(), is(JobStatus.FAILED));

		// wait until all slots are back
		for (int i = 0; i < parallelism - 1; i++) {
			returnedSlots.poll(2000, TimeUnit.MILLISECONDS);
		}

		//  verify that no deployments have happened
		assertThat(taskManager.getSubmitTaskCount(), is(0));
	}

	/**
	 * Tests that an ongoing scheduling operation does not fail the {@link ExecutionGraph}
	 * if it gets concurrently cancelled.
	 */
	@Test
	public void testSchedulingOperationCancellationWhenCancel() throws Exception {
		final JobVertex jobVertex = new JobVertex("NoOp JobVertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(2);
		final JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);
		jobGraph.setAllowQueuedScheduling(true);

		final CompletableFuture<LogicalSlot> slotFuture1 = new CompletableFuture<>();
		final CompletableFuture<LogicalSlot> slotFuture2 = new CompletableFuture<>();
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(2);
		slotProvider.addSlots(jobVertex.getID(), new CompletableFuture[]{slotFuture1, slotFuture2});
		final ExecutionGraph executionGraph = createExecutionGraph(jobGraph, slotProvider);

		executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		executionGraph.scheduleForExecution();

		final TestingLogicalSlot slot = createTestingSlot();
		final CompletableFuture<?> releaseFuture = slot.getReleaseFuture();
		slotFuture1.complete(slot);

		// cancel should change the state of all executions to CANCELLED
		executionGraph.cancel();

		// complete the now CANCELLED execution --> this should cause a failure
		slotFuture2.complete(new TestingLogicalSlotBuilder().createTestingLogicalSlot());

		Thread.sleep(1L);
		// release the first slot to finish the cancellation
		releaseFuture.complete(null);

		// NOTE: This test will only occasionally fail without the fix since there is
		// a race between the releaseFuture and the slotFuture2
		assertThat(executionGraph.getTerminationFuture().get(), is(JobStatus.CANCELED));
	}

	/**
	 * Tests that a partially completed eager scheduling operation fails if a
	 * completed slot is released. See FLINK-9099.
	 */
	@Test
	public void testSlotReleasingFailsSchedulingOperation() throws Exception {
		final int parallelism = 2;

		final JobVertex jobVertex = new JobVertex("Testing job vertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(parallelism);
		final JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);

		final LogicalSlot slot = createSingleLogicalSlot(new DummySlotOwner(), new SimpleAckingTaskManagerGateway(), new SlotRequestId());
		slotProvider.addSlot(jobVertex.getID(), 0, CompletableFuture.completedFuture(slot));

		final CompletableFuture<LogicalSlot> slotFuture = new CompletableFuture<>();
		slotProvider.addSlot(jobVertex.getID(), 1, slotFuture);

		final ExecutionGraph executionGraph = createExecutionGraph(jobGraph, slotProvider);

		executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		executionGraph.scheduleForExecution();

		assertThat(executionGraph.getState(), is(JobStatus.RUNNING));

		final ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertex.getID());
		final ExecutionVertex[] taskVertices = executionJobVertex.getTaskVertices();
		assertThat(taskVertices[0].getExecutionState(), is(ExecutionState.SCHEDULED));
		assertThat(taskVertices[1].getExecutionState(), is(ExecutionState.SCHEDULED));

		// fail the single allocated slot --> this should fail the scheduling operation
		slot.releaseSlot(new FlinkException("Test failure"));

		assertThat(executionGraph.getTerminationFuture().get(), is(JobStatus.FAILED));
	}

	/**
	 * Tests that all slots are being returned to the {@link SlotOwner} if the
	 * {@link ExecutionGraph} is being cancelled. See FLINK-9908
	 */
	@Test
	public void testCancellationOfIncompleteScheduling() throws Exception {
		final int parallelism = 10;

		final JobVertex jobVertex = new JobVertex("Test job vertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobVertex.setParallelism(parallelism);

		final JobGraph jobGraph = new JobGraph(jobVertex);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.setScheduleMode(ScheduleMode.EAGER);

		final TestingSlotOwner slotOwner = new TestingSlotOwner();
		final SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

		final ConcurrentMap<SlotRequestId, Integer> slotRequestIds = new ConcurrentHashMap<>(parallelism);

		final TestingSlotProvider slotProvider = new TestingSlotProvider(
			(SlotRequestId slotRequestId) -> {
				slotRequestIds.put(slotRequestId, 1);
				// return 50/50 fulfilled and unfulfilled requests
				return slotRequestIds.size() % 2 == 0 ?
					CompletableFuture.completedFuture(
						createSingleLogicalSlot(slotOwner, taskManagerGateway, slotRequestId)) :
					new CompletableFuture<>();
			});

		final ExecutionGraph executionGraph = createExecutionGraph(jobGraph, slotProvider);

		executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		final Set<SlotRequestId> slotRequestIdsToReturn = ConcurrentHashMap.newKeySet(slotRequestIds.size());

		executionGraph.scheduleForExecution();

		slotRequestIdsToReturn.addAll(slotRequestIds.keySet());

		slotOwner.setReturnAllocatedSlotConsumer(logicalSlot -> {
			slotRequestIdsToReturn.remove(logicalSlot.getSlotRequestId());
		});

		slotProvider.setSlotCanceller(slotRequestIdsToReturn::remove);

		// make sure that we complete cancellations of deployed tasks
		taskManagerGateway.setCancelConsumer(
			(ExecutionAttemptID executionAttemptId) -> {
				final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttemptId);

				// if the execution was cancelled in state SCHEDULING, then it might already have been removed
				if (execution != null) {
					execution.completeCancelling();
				}
			}
		);

		executionGraph.cancel();
		assertThat(slotRequestIdsToReturn, is(empty()));
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider) throws Exception {
		return createExecutionGraph(jobGraph, slotProvider, Time.minutes(10));
	}

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider, Time timeout) throws Exception {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			new Configuration(),
			executor,
			executor,
			slotProvider,
			getClass().getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			VoidBlobWriter.getInstance(),
			timeout,
			log,
			NettyShuffleMaster.INSTANCE,
			NoOpPartitionTracker.INSTANCE);
	}

	@Nonnull
	static SingleLogicalSlot createSingleLogicalSlot(SlotOwner slotOwner, TaskManagerGateway taskManagerGateway, SlotRequestId slotRequestId) {
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

	private static InteractionsCountingTaskManagerGateway createTaskManager() {
		return new InteractionsCountingTaskManagerGateway();
	}

	private static void verifyNothingDeployed(ExecutionGraph eg, InteractionsCountingTaskManagerGateway[] taskManagers) {
		// job should still be running
		assertEquals(JobStatus.RUNNING, eg.getState());

		// none of the TaskManager should have gotten a deployment call, yet
		for (InteractionsCountingTaskManagerGateway gateway : taskManagers) {
			assertThat(gateway.getSubmitTaskCount(), is(0));
		}
	}

	private static class TestRuntimeException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}

	@Nonnull
	private static TestingLogicalSlot createTestingSlot() {
		return new TestingLogicalSlotBuilder()
			.setAutomaticallyCompleteReleaseFuture(false)
			.createTestingLogicalSlot();
	}
}

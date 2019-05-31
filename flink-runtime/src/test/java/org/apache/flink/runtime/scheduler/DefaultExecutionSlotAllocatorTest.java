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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.TestingSlotProvider;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DefaultExecutionSlotAllocator}.
 */
public class DefaultExecutionSlotAllocatorTest extends TestLogger {

	private DefaultExecutionSlotAllocator executionSlotAllocator;

	private TestingSlotProvider slotProvider;

	private TestingInputsLocationsRetriever testingInputsLocationsRetriever;

	private List<ExecutionVertexSchedulingRequirements> schedulingRequirements;

	private Queue<CompletableFuture<LogicalSlot>> slotFutures;

	private List<SlotRequestId> receivedSlotRequestIds;

	private List<SlotRequestId> cancelledSlotRequestIds;

	private List<ExecutionVertexID> executionVertexIds;

	private int executionVerticesNum;

	@Before
	public void setUp() throws Exception {
		testingInputsLocationsRetriever = createSimpleInputsLocationsRetriever();

		executionVerticesNum = testingInputsLocationsRetriever.getTotalNumberOfVertices();
		receivedSlotRequestIds = new ArrayList<>(executionVerticesNum);
		cancelledSlotRequestIds = new ArrayList<>(executionVerticesNum);
		slotFutures = new ArrayDeque<>(executionVerticesNum);

		slotProvider = new TestingSlotProvider(slotRequestId -> {
			receivedSlotRequestIds.add(slotRequestId);
			return slotFutures.poll();
		});
		slotProvider.setSlotCanceller(slotRequestId -> cancelledSlotRequestIds.add(slotRequestId));

		executionSlotAllocator = new DefaultExecutionSlotAllocator(
				slotProvider,
				testingInputsLocationsRetriever,
				Time.seconds(10),
				true);

		schedulingRequirements = new ArrayList<>(executionVerticesNum);
		executionVertexIds = new ArrayList<>(executionVerticesNum);

		for (ExecutionVertexID executionVertexId : testingInputsLocationsRetriever.getAllExecutionVertices()) {
			schedulingRequirements.add(new ExecutionVertexSchedulingRequirements(
					executionVertexId,
					null,
					ResourceProfile.UNKNOWN,
					new SlotSharingGroupId(),
					null,
					null
			));
			executionVertexIds.add(executionVertexId);
			slotFutures.add(new CompletableFuture<>());
		}
	}

	/**
	 * Tests that it will allocate slots from slot provider and remove the slot assignments when request are fulfilled.
	 */
	@Test
	public void testAllocateSlotsFor() {
		List<CompletableFuture<LogicalSlot>> backupSlotFutures = new ArrayList<>(slotFutures);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), containsInAnyOrder(executionVertexIds.toArray()));

		completeProducerSlotRequest(backupSlotFutures);

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum));
	}

	/**
	 * Tests that when cancelling a slot request, the request to slot provider should also be cancelled.
	 */
	@Test
	public void testCancel() {
		List<CompletableFuture<LogicalSlot>> backupSlotFutures = new ArrayList<>(slotFutures);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		// cancel a non-existing execution vertex
		ExecutionVertexID inValidExecutionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		executionSlotAllocator.cancel(inValidExecutionVertexId);
		assertThat(cancelledSlotRequestIds, hasSize(0));

		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));

		completeProducerSlotRequest(backupSlotFutures);

		for (ExecutionVertexID executionVertexId : executionVertexIds) {
			executionSlotAllocator.cancel(executionVertexId);
		}

		// only the consumers slot request will be cancelled
		List<SlotRequestId> expectCancelledSlotRequestIds =
				receivedSlotRequestIds.subList(executionVerticesNum / 2, executionVerticesNum);
		assertThat(cancelledSlotRequestIds, containsInAnyOrder(expectCancelledSlotRequestIds.toArray()));
	}

	/**
	 * Tests that all unfulfilled slot requests will be cancelled when stopped.
	 */
	@Test
	public void testStop() {
		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		// only the producers will ask for slots as the preferred location futures of consumers are not fulfilled.
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(executionVerticesNum));
		assertThat(receivedSlotRequestIds, hasSize(executionVerticesNum / 2));

		executionSlotAllocator.stop().getNow(null);

		assertThat(cancelledSlotRequestIds, hasSize(executionVerticesNum / 2));
		assertThat(cancelledSlotRequestIds, containsInAnyOrder(receivedSlotRequestIds.toArray()));
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));
	}

	/**
	 * Tests that all prior allocation ids are computed by union all previous allocation ids in scheduling requirements.
	 */
	@Test
	public void testComputeAllPriorAllocationIds() {
		List<AllocationID> expectAllocationIds = Arrays.asList(new AllocationID(), new AllocationID());
		List<ExecutionVertexSchedulingRequirements> testSchedulingRequirements = Arrays.asList(
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(0),
						expectAllocationIds.get(0),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(1),
						expectAllocationIds.get(0),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(2),
						expectAllocationIds.get(1),
						ResourceProfile.UNKNOWN,
						null,
						null,
						null),
				new ExecutionVertexSchedulingRequirements(
						executionVertexIds.get(3),
						null,
						ResourceProfile.UNKNOWN,
						null,
						null,
						null)
		);

		Set<AllocationID> allPriorAllocationIds = executionSlotAllocator.computeAllPriorAllocationIds(testSchedulingRequirements);
		assertThat(allPriorAllocationIds, containsInAnyOrder(expectAllocationIds.toArray()));
	}

	/**
	 * Tests the calculation of preferred locations based on inputs for an execution.
	 */
	@Test
	public void testGetPreferredLocationsBasedOnInputs() throws Exception {
		JobVertex producer1 = new JobVertex("producer1");
		producer1.setInvokableClass(NoOpInvokable.class);
		producer1.setParallelism(10);

		JobVertex producer2 = new JobVertex("producer2");
		producer2.setInvokableClass(NoOpInvokable.class);
		producer2.setParallelism(10);

		JobVertex producer3 = new JobVertex("producer3");
		producer3.setInvokableClass(NoOpInvokable.class);
		producer3.setParallelism(10);

		JobVertex producer4 = new JobVertex("producer4");
		producer4.setInvokableClass(NoOpInvokable.class);
		producer4.setParallelism(5);

		JobVertex producer5 = new JobVertex("producer5");
		producer5.setInvokableClass(NoOpInvokable.class);
		producer5.setParallelism(3);

		JobVertex consumer1 = new JobVertex("consumer1");
		consumer1.setInvokableClass(NoOpInvokable.class);
		consumer1.setParallelism(1);

		JobVertex consumer2 = new JobVertex("consumer2");
		consumer2.setInvokableClass(NoOpInvokable.class);
		consumer2.setParallelism(5);

		consumer1.connectNewDataSetAsInput(producer1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer1.connectNewDataSetAsInput(producer2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer1.connectNewDataSetAsInput(producer3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer1.connectNewDataSetAsInput(producer4, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer1.connectNewDataSetAsInput(producer5, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		consumer2.connectNewDataSetAsInput(producer1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		TestingInputsLocationsRetriever locationsRetriever = new TestingInputsLocationsRetriever(
				producer1, producer2, producer3, producer4, producer5, consumer1, consumer2);

		// The first upstream has more than 10 different locations
		List<TaskManagerLocation> locationsOfProducer1 = new ArrayList<>(producer1.getParallelism());
		for (int i = 0; i < producer1.getParallelism(); i++) {
			TaskManagerLocation location = new LocalTaskManagerLocation();
			locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer1.getID(), i))
					.ifPresent((locationFuture -> locationFuture.complete(location)));
			locationsOfProducer1.add(location);
		}

		// The second upstream has 1 different locations with 7 uncompleted
		TaskManagerLocation location = new LocalTaskManagerLocation();
		for (int i = 0; i < 3; i++) {
			locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer2.getID(), i))
					.ifPresent((locationFuture -> locationFuture.complete(location)));
		}

		// The third upstream has 1 location with 9 uncompleted
		locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer3.getID(), 0))
					.ifPresent((locationFuture -> locationFuture.complete(new LocalTaskManagerLocation())));

		// The fourth upstream has 3 different locations
		for (int i = 0; i < producer4.getParallelism(); i++) {
			locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer4.getID(), i))
					.ifPresent((locationFuture -> locationFuture.complete(new LocalTaskManagerLocation())));
		}

		// Locations of producer2 and producer5 have not been all fulfilled.
		ExecutionVertexID idOfConsumer1 = new ExecutionVertexID(consumer1.getID(), 0);
		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsOfConsumer1 =
				DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(idOfConsumer1, locationsRetriever);
		assertFalse(preferredLocationsOfConsumer1.isDone());

		// Locations of producer5 have not been all fulfilled.
		for (int i = 3; i < producer2.getParallelism(); i++) {
			locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer2.getID(), i))
					.ifPresent((locationFuture -> locationFuture.complete(location)));
		}
		assertFalse(preferredLocationsOfConsumer1.isDone());

		for (int i = 0; i < producer5.getParallelism(); i++) {
			locationsRetriever.getTaskManagerLocation(new ExecutionVertexID(producer5.getID(), i))
					.ifPresent((locationFuture -> locationFuture.complete(new LocalTaskManagerLocation())));
		}
		// All locations of producers are fulfilled.
		assertTrue(preferredLocationsOfConsumer1.isDone());
		assertThat(preferredLocationsOfConsumer1.get(), contains(location));

		for (int i = 0; i < consumer2.getParallelism(); i++) {
			ExecutionVertexID idOfConsumer2 = new ExecutionVertexID(consumer2.getID(), i);
			CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsOfConsumer2 =
					DefaultExecutionSlotAllocator.getPreferredLocationsBasedOnInputs(idOfConsumer2, locationsRetriever);

			assertTrue(preferredLocationsOfConsumer2.isDone());
			assertThat(preferredLocationsOfConsumer2.get(),
					containsInAnyOrder(locationsOfProducer1.subList(2 * i, 2 * i + 2).toArray()));
		}
	}

	private void completeProducerSlotRequest(List<CompletableFuture<LogicalSlot>> totalSlotFutures) {
		// fulfill the producers and the location preferences of consumers, so the consumers will allocate slots
		for (int i = 0; i < executionVerticesNum / 2; i++) {
			LogicalSlot slot = new TestingLogicalSlot();
			totalSlotFutures.get(i).complete(slot);

		}
		for (ExecutionVertexID executionVertexId : testingInputsLocationsRetriever.getAllExecutionVertices()) {
			if (testingInputsLocationsRetriever.getConsumedResultPartitionsProducers(executionVertexId).isEmpty()) {
				testingInputsLocationsRetriever.getTaskManagerLocation(executionVertexId).ifPresent(
						(taskManagerLocationFuture) -> taskManagerLocationFuture.complete(new LocalTaskManagerLocation()));

			}
		}
	}

	static TestingInputsLocationsRetriever createSimpleInputsLocationsRetriever() {
		int parallelism = 3;

		JobVertex producer = new JobVertex("producer");
		producer.setInvokableClass(NoOpInvokable.class);
		producer.setParallelism(parallelism);

		JobVertex consumer = new JobVertex("consumer");
		consumer.setInvokableClass(NoOpInvokable.class);
		consumer.setParallelism(parallelism);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		return new TestingInputsLocationsRetriever(producer, consumer);
	}
}

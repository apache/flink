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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DefaultExecutionSlotAllocator}.
 */
public class DefaultExecutionSlotAllocatorTest extends TestLogger {

	private AllocationToggableSlotProvider slotProvider;

	@Before
	public void setUp() throws Exception {
		slotProvider = new AllocationToggableSlotProvider();
	}

	/**
	 * Tests that consumers will get slots after producers are fulfilled.
	 */
	@Test
	public void testConsumersAssignedToSlotsAfterProducers() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		final TestingInputsLocationsRetriever inputsLocationsRetriever = new TestingInputsLocationsRetriever.Builder()
				.connectConsumerToProducer(consumerId, producerId)
				.build();

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(inputsLocationsRetriever);

		inputsLocationsRetriever.markScheduled(producerId);
		inputsLocationsRetriever.markScheduled(consumerId);

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = createSchedulingRequirements(producerId, consumerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments = executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
		assertThat(slotExecutionVertexAssignments, hasSize(2));

		final SlotExecutionVertexAssignment producerSlotAssignment = findSlotAssignmentByExecutionVertexId(producerId, slotExecutionVertexAssignments);
		final SlotExecutionVertexAssignment consumerSlotAssignment = findSlotAssignmentByExecutionVertexId(consumerId, slotExecutionVertexAssignments);

		assertTrue(producerSlotAssignment.getLogicalSlotFuture().isDone());
		assertFalse(consumerSlotAssignment.getLogicalSlotFuture().isDone());

		inputsLocationsRetriever.assignTaskManagerLocation(producerId);

		assertTrue(consumerSlotAssignment.getLogicalSlotFuture().isDone());
		assertEquals(0, executionSlotAllocator.getNumberOfPendingSlotAssignments());
	}

	/**
	 * Tests that validate the parameters when calling allocateSlot in SlotProvider.
	 */
	@Test
	public void testAllocateSlotsParameters() {
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		final AllocationID allocationId = new AllocationID();
		final SlotSharingGroupId sharingGroupId = new SlotSharingGroupId();
		final ResourceProfile resourceProfile = new ResourceProfile(0.5, 250);
		final CoLocationConstraint coLocationConstraint = new CoLocationGroup().getLocationConstraint(0);
		final Collection<TaskManagerLocation> taskManagerLocations = Collections.singleton(new LocalTaskManagerLocation());

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = Arrays.asList(
				new ExecutionVertexSchedulingRequirements.Builder()
						.withExecutionVertexId(executionVertexId)
						.withPreviousAllocationId(allocationId)
						.withSlotSharingGroupId(sharingGroupId)
						.withPreferredLocations(taskManagerLocations)
						.withResourceProfile(resourceProfile)
						.withCoLocationConstraint(coLocationConstraint)
						.build()
		);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
		assertThat(slotProvider.getSlotAllocationRequests(), hasSize(1));

		ScheduledUnit expectedTask = slotProvider.getSlotAllocationRequests().get(0).f1;
		SlotProfile expectedSlotProfile = slotProvider.getSlotAllocationRequests().get(0).f2;

		assertEquals(sharingGroupId, expectedTask.getSlotSharingGroupId());
		assertEquals(coLocationConstraint, expectedTask.getCoLocationConstraint());
		assertThat(expectedSlotProfile.getPreferredAllocations(), contains(allocationId));
		assertThat(expectedSlotProfile.getPreviousExecutionGraphAllocations(), contains(allocationId));
		assertEquals(resourceProfile, expectedSlotProfile.getResourceProfile());
		assertThat(expectedSlotProfile.getPreferredLocations(), contains(taskManagerLocations.toArray()));
	}

	/**
	 * Tests that cancels an execution vertex which is not existed.
	 */
	@Test
	public void testCancelNonExistingExecutionVertex() {
		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		ExecutionVertexID inValidExecutionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		executionSlotAllocator.cancel(inValidExecutionVertexId);

		assertThat(slotProvider.getCancelledSlotRequestIds(), is(empty()));
	}

	/**
	 * Tests that cancels a slot request which has already been fulfilled.
	 */
	@Test
	public void testCancelFulfilledSlotRequest() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
				createSchedulingRequirements(producerId);
		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		executionSlotAllocator.cancel(producerId);

		assertThat(slotProvider.getCancelledSlotRequestIds(), is(empty()));
	}

	/**
	 * Tests that cancels a slot request which has not been fulfilled.
	 */
	@Test
	public void testCancelUnFulfilledSlotRequest() throws Exception {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		slotProvider.disableSlotAllocation();
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
				createSchedulingRequirements(producerId);
		Collection<SlotExecutionVertexAssignment> assignments = executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		executionSlotAllocator.cancel(producerId);

		assertThat(slotProvider.getCancelledSlotRequestIds(), hasSize(1));
		assertThat(slotProvider.getCancelledSlotRequestIds(), contains(slotProvider.getReceivedSlotRequestIds().toArray()));

		try {
			assignments.iterator().next().getLogicalSlotFuture().get();
			fail("Expect a CancellationException but got nothing.");
		} catch (CancellationException ignored) {
			// Expected exception
		}
	}

	/**
	 * Tests that all unfulfilled slot requests will be cancelled when stopped.
	 */
	@Test
	public void testStop() throws Exception {
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		slotProvider.disableSlotAllocation();
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
				createSchedulingRequirements(executionVertexId);
		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		executionSlotAllocator.stop().get();

		assertThat(slotProvider.getCancelledSlotRequestIds(), hasSize(1));
		assertThat(slotProvider.getCancelledSlotRequestIds(), contains(slotProvider.getReceivedSlotRequestIds().toArray()));
		assertEquals(0, executionSlotAllocator.getNumberOfPendingSlotAssignments());
	}

	/**
	 * Tests that all prior allocation ids are computed by union all previous allocation ids in scheduling requirements.
	 */
	@Test
	public void testComputeAllPriorAllocationIds() {
		List<AllocationID> expectAllocationIds = Arrays.asList(new AllocationID(), new AllocationID());
		List<ExecutionVertexSchedulingRequirements> testSchedulingRequirements = Arrays.asList(
				new ExecutionVertexSchedulingRequirements.Builder().
						withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0)).
						withPreviousAllocationId(expectAllocationIds.get(0)).
						build(),
				new ExecutionVertexSchedulingRequirements.Builder().
						withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 1)).
						withPreviousAllocationId(expectAllocationIds.get(0)).
						build(),
				new ExecutionVertexSchedulingRequirements.Builder().
						withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 2)).
						withPreviousAllocationId(expectAllocationIds.get(1)).
						build(),
				new ExecutionVertexSchedulingRequirements.Builder().
						withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 3)).
						build()
		);

		Set<AllocationID> allPriorAllocationIds = DefaultExecutionSlotAllocator.computeAllPriorAllocationIds(testSchedulingRequirements);
		assertThat(allPriorAllocationIds, containsInAnyOrder(expectAllocationIds.toArray()));
	}

	private DefaultExecutionSlotAllocator createExecutionSlotAllocator() {
		return createExecutionSlotAllocator(new TestingInputsLocationsRetriever.Builder().build());
	}

	private DefaultExecutionSlotAllocator createExecutionSlotAllocator(InputsLocationsRetriever inputsLocationsRetriever) {
		return new DefaultExecutionSlotAllocator(slotProvider, inputsLocationsRetriever, Time.seconds(10));
	}

	private List<ExecutionVertexSchedulingRequirements> createSchedulingRequirements(ExecutionVertexID... executionVertexIds) {
		List<ExecutionVertexSchedulingRequirements> schedulingRequirements = new ArrayList<>(executionVertexIds.length);

		for (ExecutionVertexID executionVertexId : executionVertexIds) {
			schedulingRequirements.add(new ExecutionVertexSchedulingRequirements.Builder()
					.withExecutionVertexId(executionVertexId).build());
		}
		return schedulingRequirements;
	}

	private SlotExecutionVertexAssignment findSlotAssignmentByExecutionVertexId(
			ExecutionVertexID executionVertexId,
			Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {
		return slotExecutionVertexAssignments.stream()
				.filter(slotExecutionVertexAssignment -> slotExecutionVertexAssignment.getExecutionVertexId().equals(executionVertexId))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(String.format(
						"SlotExecutionVertexAssignment with execution vertex id %s not found",
						executionVertexId)));
	}

	private static class AllocationToggableSlotProvider implements SlotProvider {

		private final List<Tuple3<SlotRequestId, ScheduledUnit, SlotProfile>> slotAllocationRequests = new ArrayList<>();

		private final List<SlotRequestId> cancelledSlotRequestIds = new ArrayList<>();

		private boolean slotAllocationDisabled;

		@Override
		public CompletableFuture<LogicalSlot> allocateSlot(
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				SlotProfile slotProfile,
				boolean allowQueued,
				Time timeout) {

			slotAllocationRequests.add(Tuple3.of(slotRequestId, task, slotProfile));
			if (slotAllocationDisabled) {
				return new CompletableFuture<>();
			} else {
				return CompletableFuture.completedFuture(new TestingLogicalSlotBuilder().createTestingLogicalSlot());
			}
		}

		@Override
		public void cancelSlotRequest(
				final SlotRequestId slotRequestId,
				@Nullable final SlotSharingGroupId slotSharingGroupId,
				final Throwable cause) {
			cancelledSlotRequestIds.add(slotRequestId);
		}

		public List<Tuple3<SlotRequestId, ScheduledUnit, SlotProfile>> getSlotAllocationRequests() {
			return Collections.unmodifiableList(slotAllocationRequests);
		}

		public List<SlotRequestId> getReceivedSlotRequestIds() {
			return slotAllocationRequests.stream()
					.map(requestTuple -> requestTuple.f0)
					.collect(Collectors.toList());
		}

		public List<SlotRequestId> getCancelledSlotRequestIds() {
			return Collections.unmodifiableList(cancelledSlotRequestIds);
		}

		public void disableSlotAllocation() {
			slotAllocationDisabled = true;
		}
	}
}

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
import org.apache.flink.runtime.executiongraph.SlotProviderStrategy;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
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
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorTestUtils.createSchedulingRequirements;
import static org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorTestUtils.findSlotAssignmentByExecutionVertexId;
import static org.hamcrest.Matchers.contains;
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

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(
			new TestingStateLocationRetriever(),
			inputsLocationsRetriever);

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
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));
	}

	/**
	 * Tests that validate the parameters when calling allocateSlot in SlotProvider.
	 */
	@Test
	public void testAllocateSlotsParameters() {
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		final AllocationID allocationId = new AllocationID();
		final SlotSharingGroupId sharingGroupId = new SlotSharingGroupId();
		final ResourceProfile taskResourceProfile = ResourceProfile.fromResources(0.5, 250);
		final ResourceProfile physicalSlotResourceProfile = ResourceProfile.fromResources(1.0, 300);
		final CoLocationConstraint coLocationConstraint = new CoLocationGroup().getLocationConstraint(0);
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		final TestingStateLocationRetriever stateLocationRetriever = new TestingStateLocationRetriever();
		stateLocationRetriever.setStateLocation(executionVertexId, taskManagerLocation);

		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(
			stateLocationRetriever,
			new TestingInputsLocationsRetriever.Builder().build());

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = Arrays.asList(
			new ExecutionVertexSchedulingRequirements.Builder()
				.withExecutionVertexId(executionVertexId)
				.withPreviousAllocationId(allocationId)
				.withSlotSharingGroupId(sharingGroupId)
				.withPhysicalSlotResourceProfile(physicalSlotResourceProfile)
				.withTaskResourceProfile(taskResourceProfile)
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
		assertEquals(taskResourceProfile, expectedSlotProfile.getTaskResourceProfile());
		assertEquals(physicalSlotResourceProfile, expectedSlotProfile.getPhysicalSlotResourceProfile());
		assertThat(expectedSlotProfile.getPreferredLocations(), contains(taskManagerLocation));
	}

	@Test
	public void testDuplicatedSlotAllocationIsNotAllowed() {
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);

		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();
		slotProvider.disableSlotAllocation();

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			createSchedulingRequirements(executionVertexId);
		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		try {
			executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
			fail("exception should happen");
		} catch (IllegalStateException e) {
			// IllegalStateException is expected
		}
	}

	@Test
	public void testSlotAssignmentIsProperlyRegistered() {
		final DefaultExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final ExecutionVertexID executionVertexID = new ExecutionVertexID(new JobVertexID(), 0);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			createSchedulingRequirements(executionVertexID);

		slotProvider.disableSlotAllocation();
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		final SlotExecutionVertexAssignment slotAssignment = slotExecutionVertexAssignments.iterator().next();

		assertThat(executionSlotAllocator.getPendingSlotAssignments().values(), contains(slotAssignment));

		executionSlotAllocator.cancel(executionVertexID);

		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));

		final SlotRequestId slotRequestId = slotProvider.slotAllocationRequests.get(0).f0;
		assertThat(slotProvider.getCancelledSlotRequestIds(), contains(slotRequestId));
	}

	private DefaultExecutionSlotAllocator createExecutionSlotAllocator() {
		return createExecutionSlotAllocator(
			new TestingStateLocationRetriever(),
			new TestingInputsLocationsRetriever.Builder().build());
	}

	private DefaultExecutionSlotAllocator createExecutionSlotAllocator(
			final StateLocationRetriever stateLocationRetriever,
			final InputsLocationsRetriever inputsLocationsRetriever) {
		return new DefaultExecutionSlotAllocator(
			SlotProviderStrategy.from(
				ScheduleMode.EAGER,
				slotProvider,
				Time.seconds(10)),
			new DefaultPreferredLocationsRetriever(stateLocationRetriever, inputsLocationsRetriever));
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

		public void disableSlotAllocation() {
			slotAllocationDisabled = true;
		}

		List<SlotRequestId> getCancelledSlotRequestIds() {
			return cancelledSlotRequestIds;
		}
	}
}

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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.BulkSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.createPhysicalSlot;
import static org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorTestUtils.createSchedulingRequirements;
import static org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorTestUtils.findSlotAssignmentByExecutionVertexId;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link OneSlotPerExecutionSlotAllocator}.
 */
public class OneSlotPerExecutionSlotAllocatorTest extends TestLogger {

	private TestingBulkSlotProvider slotProvider;

	@Before
	public void setUp() throws Exception {
		slotProvider = new TestingBulkSlotProvider();
	}

	@Test
	public void testSucceededSlotAllocation() {
		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			createSchedulingRequirements(executionVertexId);

		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		assertThat(slotExecutionVertexAssignments, hasSize(1));

		final SlotExecutionVertexAssignment slotAssignment = slotExecutionVertexAssignments.iterator().next();

		assertThat(slotAssignment.getExecutionVertexId(), equalTo(executionVertexId));
		assertThat(slotAssignment.getLogicalSlotFuture().isDone(), is(true));
		assertThat(slotAssignment.getLogicalSlotFuture().isCompletedExceptionally(), is(false));
	}

	@Test
	public void testFailedSlotAllocation() {
		final OneSlotPerExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			createSchedulingRequirements(executionVertexId);

		slotProvider.forceFailingSlotAllocation();
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		final SlotExecutionVertexAssignment slotAssignment = slotExecutionVertexAssignments.iterator().next();

		assertThat(slotAssignment.getLogicalSlotFuture().isCompletedExceptionally(), is(true));
		assertThat(executionSlotAllocator.getPendingSlotAssignments().keySet(), hasSize(0));

		final SlotRequestId slotRequestId = slotProvider.getSlotRequests().get(0).getSlotRequestId();
		assertThat(slotProvider.getCancelledSlotRequestIds(), contains(slotRequestId));
	}

	@Test
	public void testInterBulkInputLocationPreferencesAreRespected() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		final TestingInputsLocationsRetriever inputsLocationsRetriever = new TestingInputsLocationsRetriever.Builder()
			.connectConsumerToProducer(consumerId, producerId)
			.build();

		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(
			new TestingStateLocationRetriever(),
			inputsLocationsRetriever);

		inputsLocationsRetriever.markScheduled(producerId);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirementsForProducer =
			createSchedulingRequirements(producerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignmentsForProducer =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirementsForProducer);
		final SlotExecutionVertexAssignment producerSlotAssignment =
			findSlotAssignmentByExecutionVertexId(producerId, slotExecutionVertexAssignmentsForProducer);

		assertThat(producerSlotAssignment.getLogicalSlotFuture().isDone(), is(true));

		inputsLocationsRetriever.markScheduled(consumerId);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirementsForConsumer =
			createSchedulingRequirements(consumerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignmentsForConsumer =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirementsForConsumer);
		final SlotExecutionVertexAssignment consumerSlotAssignment =
			findSlotAssignmentByExecutionVertexId(consumerId, slotExecutionVertexAssignmentsForConsumer);

		assertThat(consumerSlotAssignment.getLogicalSlotFuture().isDone(), is(false));

		inputsLocationsRetriever.assignTaskManagerLocation(producerId);

		assertThat(consumerSlotAssignment.getLogicalSlotFuture().isDone(), is(true));
	}

	@Test
	public void testIntraBulkInputLocationPreferencesDoNotBlockAllocation() {
		final ExecutionVertexID producerId = new ExecutionVertexID(new JobVertexID(), 0);
		final ExecutionVertexID consumerId = new ExecutionVertexID(new JobVertexID(), 0);

		final TestingInputsLocationsRetriever inputsLocationsRetriever = new TestingInputsLocationsRetriever.Builder()
			.connectConsumerToProducer(consumerId, producerId)
			.build();

		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(
			new TestingStateLocationRetriever(),
			inputsLocationsRetriever);

		inputsLocationsRetriever.markScheduled(producerId);
		inputsLocationsRetriever.markScheduled(consumerId);

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
			createSchedulingRequirements(producerId, consumerId);
		final Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		assertThat(slotExecutionVertexAssignments, hasSize(2));

		final SlotExecutionVertexAssignment producerSlotAssignment =
			findSlotAssignmentByExecutionVertexId(producerId, slotExecutionVertexAssignments);
		final SlotExecutionVertexAssignment consumerSlotAssignment =
			findSlotAssignmentByExecutionVertexId(consumerId, slotExecutionVertexAssignments);

		assertThat(producerSlotAssignment.getLogicalSlotFuture().isDone(), is(true));
		assertThat(consumerSlotAssignment.getLogicalSlotFuture().isDone(), is(true));
	}

	@Test
	public void testCreatedSlotRequests() {
		final ExecutionVertexID executionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
		final AllocationID allocationId = new AllocationID();
		final SlotSharingGroupId sharingGroupId = new SlotSharingGroupId();
		final ResourceProfile taskResourceProfile = ResourceProfile.fromResources(0.5, 250);
		final ResourceProfile physicalSlotResourceProfile = ResourceProfile.fromResources(1.0, 300);
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		final TestingStateLocationRetriever stateLocationRetriever = new TestingStateLocationRetriever();
		stateLocationRetriever.setStateLocation(executionVertexId, taskManagerLocation);

		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator(
			stateLocationRetriever,
			new TestingInputsLocationsRetriever.Builder().build());

		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = Collections.singletonList(
			new ExecutionVertexSchedulingRequirements.Builder()
				.withExecutionVertexId(executionVertexId)
				.withPreviousAllocationId(allocationId)
				.withSlotSharingGroupId(sharingGroupId)
				.withPhysicalSlotResourceProfile(physicalSlotResourceProfile)
				.withTaskResourceProfile(taskResourceProfile)
				.build()
		);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
		assertThat(slotProvider.getSlotRequests(), hasSize(1));

		final SlotProfile requestSlotProfile = slotProvider.getSlotRequests().iterator().next().getSlotProfile();

		assertThat(requestSlotProfile.getPreferredAllocations(), contains(allocationId));
		assertThat(requestSlotProfile.getPreviousExecutionGraphAllocations(), contains(allocationId));
		assertThat(requestSlotProfile.getTaskResourceProfile(), equalTo(taskResourceProfile));
		assertThat(requestSlotProfile.getPreferredLocations(), contains(taskManagerLocation));
		// task resource profile is used instead of slot sharing group resource profile since slot sharing is ignored
		assertThat(requestSlotProfile.getPhysicalSlotResourceProfile(), equalTo(taskResourceProfile));
	}

	@Test(expected = IllegalStateException.class)
	public void testCoLocationConstraintThrowsException() {
		final ExecutionSlotAllocator executionSlotAllocator = createExecutionSlotAllocator();

		final CoLocationConstraint coLocationConstraint = new CoLocationGroup().getLocationConstraint(0);
		final List<ExecutionVertexSchedulingRequirements> schedulingRequirements = Collections.singletonList(
			new ExecutionVertexSchedulingRequirements.Builder()
				.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0))
				.withSlotSharingGroupId(new SlotSharingGroupId())
				.withCoLocationConstraint(coLocationConstraint)
				.build()
		);

		executionSlotAllocator.allocateSlotsFor(schedulingRequirements);
	}

	private OneSlotPerExecutionSlotAllocator createExecutionSlotAllocator() {
		return createExecutionSlotAllocator(
			new TestingStateLocationRetriever(),
			new TestingInputsLocationsRetriever.Builder().build());
	}

	private OneSlotPerExecutionSlotAllocator createExecutionSlotAllocator(
			final StateLocationRetriever stateLocationRetriever,
			final InputsLocationsRetriever inputsLocationsRetriever) {

		return new OneSlotPerExecutionSlotAllocator(
			slotProvider,
			new DefaultPreferredLocationsRetriever(stateLocationRetriever, inputsLocationsRetriever),
			true,
			Time.seconds(10));
	}

	private static class TestingBulkSlotProvider implements BulkSlotProvider {

		private final List<PhysicalSlotRequest> slotRequests = new ArrayList<>();

		private final List<SlotRequestId> cancelledSlotRequestIds = new ArrayList<>();

		private boolean forceFailingSlotAllocation = false;

		@Override
		public CompletableFuture<Collection<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
				final Collection<PhysicalSlotRequest> physicalSlotRequests,
				final Time timeout) {

			slotRequests.addAll(physicalSlotRequests);

			if (forceFailingSlotAllocation) {
				return FutureUtils.completedExceptionally(new Exception("Forced failure"));
			}

			final List<PhysicalSlotRequest.Result> results = new ArrayList<>(physicalSlotRequests.size());
			for (PhysicalSlotRequest request : physicalSlotRequests) {
				final PhysicalSlotRequest.Result result = new PhysicalSlotRequest.Result(
					request.getSlotRequestId(),
					createPhysicalSlot());
				results.add(result);
			}
			return CompletableFuture.completedFuture(results);
		}

		@Override
		public void cancelSlotRequest(
				final SlotRequestId slotRequestId,
				final Throwable cause) {
			cancelledSlotRequestIds.add(slotRequestId);
		}

		List<PhysicalSlotRequest> getSlotRequests() {
			return Collections.unmodifiableList(slotRequests);
		}

		List<SlotRequestId> getCancelledSlotRequestIds() {
			return Collections.unmodifiableList(cancelledSlotRequestIds);
		}

		void forceFailingSlotAllocation() {
			this.forceFailingSlotAllocation = true;
		}
	}

}

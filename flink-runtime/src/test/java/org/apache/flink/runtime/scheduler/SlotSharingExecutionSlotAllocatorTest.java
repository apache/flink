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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingPayload;
import org.apache.flink.runtime.jobmaster.slotpool.DummyPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulk;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.SharedSlotTestingUtils.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link SlotSharingExecutionSlotAllocator}.
 */
public class SlotSharingExecutionSlotAllocatorTest extends TestLogger {
	private static final Time ALLOCATION_TIMEOUT = Time.milliseconds(100L);
	private static final ResourceProfile RESOURCE_PROFILE = ResourceProfile.fromResources(3, 5);

	private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
	private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();
	private static final ExecutionVertexID EV3 = createRandomExecutionVertexId();
	private static final ExecutionVertexID EV4 = createRandomExecutionVertexId();

	@Test
	public void testSlotProfileRequestAskedBulkAndGroup() {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(EV1);

		context.allocateSlotsFor(EV1, EV2);

		List<Set<ExecutionVertexID>> askedBulks = context.getSlotProfileRetrieverFactory().getAskedBulks();
		assertThat(askedBulks, hasSize(1));
		assertThat(askedBulks.get(0), containsInAnyOrder(EV1, EV2));
		assertThat(context.getSlotProfileRetrieverFactory().getAskedGroups(), containsInAnyOrder(executionSlotSharingGroup));
	}

	@Test
	public void testSlotRequestProfile() {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2, EV3).build();
		ResourceProfile physicalsSlotResourceProfile = RESOURCE_PROFILE.multiply(3);

		context.allocateSlotsFor(EV1, EV2);

		Optional<PhysicalSlotRequest> slotRequest = context.getSlotProvider().getRequests().values().stream().findFirst();
		assertThat(slotRequest.isPresent(), is(true));
		slotRequest.ifPresent(r -> assertThat(r.getSlotProfile().getPhysicalSlotResourceProfile(), is(physicalsSlotResourceProfile)));
	}

	@Test
	public void testAllocatePhysicalSlotForNewSharedSlot() {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).addGroup(EV3, EV4).build();

		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(EV1, EV2, EV3, EV4);
		Collection<ExecutionVertexID> assignIds = getAssignIds(executionVertexAssignments);

		assertThat(assignIds, containsInAnyOrder(EV1, EV2, EV3, EV4));
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
	}

	@Test
	public void testAllocateLogicalSlotFromAvailableSharedSlot() {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();

		context.allocateSlotsFor(EV1);
		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(EV2);
		Collection<ExecutionVertexID> assignIds = getAssignIds(executionVertexAssignments);

		// execution 0 from the first allocateSlotsFor call and execution 1 from the second allocateSlotsFor call
		// share a slot, therefore only one physical slot allocation should happen
		assertThat(assignIds, containsInAnyOrder(EV2));
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));
	}

	@Test
	public void testDuplicateAllocationDoesNotRecreateLogicalSlotFuture() throws ExecutionException, InterruptedException {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1).build();

		SlotExecutionVertexAssignment assignment1 = context.allocateSlotsFor(EV1).get(0);
		SlotExecutionVertexAssignment assignment2 = context.allocateSlotsFor(EV1).get(0);

		assertThat(assignment1.getLogicalSlotFuture().get() == assignment2.getLogicalSlotFuture().get(), is(true));
	}

	@Test
	public void testFailedPhysicalSlotRequestFailsLogicalSlotFuturesAndRemovesSharedSlot() {
		AllocationContext context = AllocationContext
			.newBuilder()
			.addGroup(EV1)
			.withPhysicalSlotFutureCompletionMode(PhysicalSlotFutureCompletionMode.MANUAL)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = context.allocateSlotsFor(EV1).get(0).getLogicalSlotFuture();
		SlotRequestId slotRequestId = context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();

		assertThat(logicalSlotFuture.isDone(), is(false));
		context.getSlotProvider().failPhysicalSlotFutureFor(slotRequestId, new Throwable());
		assertThat(logicalSlotFuture.isCompletedExceptionally(), is(true));

		// next allocation allocates new shared slot
		context.allocateSlotsFor(EV1);
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
	}

	@Test
	public void testSlotWillBeOccupiedIndefinitelyFalse() throws ExecutionException, InterruptedException {
		testSlotWillBeOccupiedIndefinitely(false);
	}

	@Test
	public void testSlotWillBeOccupiedIndefinitelyTrue() throws ExecutionException, InterruptedException {
		testSlotWillBeOccupiedIndefinitely(true);
	}

	private static void testSlotWillBeOccupiedIndefinitely(boolean slotWillBeOccupiedIndefinitely) throws ExecutionException, InterruptedException {
		AllocationContext context = AllocationContext
			.newBuilder()
			.addGroup(EV1)
			.setSlotWillBeOccupiedIndefinitely(slotWillBeOccupiedIndefinitely)
			.build();
		context.allocateSlotsFor(EV1);

		PhysicalSlotRequest slotRequest = context.getSlotProvider().getFirstRequestOrFail();
		assertThat(slotRequest.willSlotBeOccupiedIndefinitely(), is(slotWillBeOccupiedIndefinitely));

		TestingPhysicalSlot physicalSlot = context.getSlotProvider().getResponses().get(slotRequest.getSlotRequestId()).get();
		assertThat(physicalSlot.getPayload(), notNullValue());
		assertThat(physicalSlot.getPayload().willOccupySlotIndefinitely(), is(slotWillBeOccupiedIndefinitely));
	}

	@Test
	public void testReturningLogicalSlotsRemovesSharedSlot() throws Exception {
		// physical slot request is completed and completes logical requests
		testLogicalSlotRequestCancellationOrRelease(
			false,
			true,
			(context, assignment) -> assignment.getLogicalSlotFuture().get().releaseSlot(null));
	}

	@Test
	public void testLogicalSlotCancelsPhysicalSlotRequestAndRemovesSharedSlot() throws Exception {
		// physical slot request is not completed and does not complete logical requests
		testLogicalSlotRequestCancellationOrRelease(
			true,
			true,
			(context, assignment) -> {
				context.getAllocator().cancel(assignment.getExecutionVertexId());
				try {
					assignment.getLogicalSlotFuture().get();
					fail("The logical future must finish with the cancellation exception");
				} catch (InterruptedException | ExecutionException e) {
					assertThat(e.getCause(), instanceOf(CancellationException.class));
				}
			});
	}

	@Test
	public void testCompletedLogicalSlotCancelationDoesNotCancelPhysicalSlotRequestAndDoesNotRemoveSharedSlot() throws Exception {
		// physical slot request is completed and completes logical requests
		testLogicalSlotRequestCancellationOrRelease(
			false,
			false,
			(context, assignment) -> {
				context.getAllocator().cancel(assignment.getExecutionVertexId());
				assignment.getLogicalSlotFuture().get();
			});
	}

	private static void testLogicalSlotRequestCancellationOrRelease(
			boolean completePhysicalSlotFutureManually,
			boolean cancelsPhysicalSlotRequestAndRemovesSharedSlot,
			BiConsumerWithException<AllocationContext, SlotExecutionVertexAssignment, Exception> cancelOrReleaseAction) throws Exception {
		AllocationContext context = AllocationContext
			.newBuilder()
			.addGroup(EV1, EV2, EV3)
			.withPhysicalSlotFutureCompletionMode(completePhysicalSlotFutureManually ? PhysicalSlotFutureCompletionMode.MANUAL : PhysicalSlotFutureCompletionMode.SUCCESS)
			.build();

		List<SlotExecutionVertexAssignment> assignments = context.allocateSlotsFor(EV1, EV2);
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel or release only one sharing logical slots
		cancelOrReleaseAction.accept(context, assignments.get(0));
		List<SlotExecutionVertexAssignment> assignmentsAfterOneCancellation = context.allocateSlotsFor(EV1, EV2);
		// there should be no more physical slot allocations, as the first logical slot reuses the previous shared slot
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel or release all sharing logical slots
		for (SlotExecutionVertexAssignment assignment : assignmentsAfterOneCancellation) {
			cancelOrReleaseAction.accept(context, assignment);
		}
		SlotRequestId slotRequestId = context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
		assertThat(context.getSlotProvider().getCancellations().containsKey(slotRequestId), is(cancelsPhysicalSlotRequestAndRemovesSharedSlot));

		context.allocateSlotsFor(EV3);
		// there should be one more physical slot allocation if the first allocation should be removed with all logical slots
		int expectedNumberOfRequests = cancelsPhysicalSlotRequestAndRemovesSharedSlot ? 2 : 1;
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(expectedNumberOfRequests));
	}

	@Test
	public void testPhysicalSlotReleaseLogicalSlots() throws ExecutionException, InterruptedException {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();
		List<SlotExecutionVertexAssignment> assignments = context.allocateSlotsFor(EV1, EV2);
		List<TestingPayload> payloads = assignments
			.stream()
			.map(assignment -> {
				TestingPayload payload = new TestingPayload();
				assignment.getLogicalSlotFuture().thenAccept(logicalSlot -> logicalSlot.tryAssignPayload(payload));
				return payload;
			})
			.collect(Collectors.toList());
		SlotRequestId slotRequestId = context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
		TestingPhysicalSlot physicalSlot = context.getSlotProvider().getFirstResponseOrFail().get();

		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(false));
		assertThat(physicalSlot.getPayload(), notNullValue());
		physicalSlot.getPayload().release(new Throwable());
		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(true));

		assertThat(context.getSlotProvider().getCancellations().containsKey(slotRequestId), is(true));

		context.allocateSlotsFor(EV1, EV2);
		// there should be one more physical slot allocation, as the first allocation should be removed after releasing all logical slots
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
	}

	@Test
	public void testSchedulePendingRequestBulkTimeoutCheck() {
		TestingPhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();
		AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

		context.allocateSlotsFor(EV1, EV3);
		PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

		assertThat(bulk.getPendingRequests(), hasSize(2));
		assertThat(bulk.getPendingRequests(), containsInAnyOrder(RESOURCE_PROFILE.multiply(2), RESOURCE_PROFILE));
		assertThat(bulk.getAllocationIdsOfFulfilledRequests(), hasSize(0));
		assertThat(bulkChecker.getTimeout(), is(ALLOCATION_TIMEOUT));
	}

	@Test
	public void testRequestFulfilledInBulk() {
		TestingPhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();
		AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

		context.allocateSlotsFor(EV1, EV3);
		AllocationID allocationId = new AllocationID();
		ResourceProfile pendingSlotResourceProfile = fulfilOneOfTwoSlotRequestsAndGetPendingProfile(context, allocationId);
		PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

		assertThat(bulk.getPendingRequests(), hasSize(1));
		assertThat(bulk.getPendingRequests(), containsInAnyOrder(pendingSlotResourceProfile));
		assertThat(bulk.getAllocationIdsOfFulfilledRequests(), hasSize(1));
		assertThat(bulk.getAllocationIdsOfFulfilledRequests(), containsInAnyOrder(allocationId));
	}

	@Test
	public void testRequestBulkCancel() {
		TestingPhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();
		AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

		// allocate 2 physical slots for 2 groups
		List<SlotExecutionVertexAssignment> assignments1 = context.allocateSlotsFor(EV1, EV3);
		fulfilOneOfTwoSlotRequestsAndGetPendingProfile(context, new AllocationID());
		PhysicalSlotRequestBulk bulk1 = bulkChecker.getBulk();
		List<SlotExecutionVertexAssignment> assignments2 = context.allocateSlotsFor(EV2);

		// cancelling of (EV1, EV3) releases assignments1 and only one physical slot for EV3
		// the second physical slot is held by sharing EV2 from the next bulk
		bulk1.cancel(new Throwable());

		// return completed logical slot to clear shared slot and release physical slot
		CompletableFuture<LogicalSlot> ev1slot = assignments1.get(0).getLogicalSlotFuture();
		boolean ev1failed = ev1slot.isCompletedExceptionally();
		CompletableFuture<LogicalSlot> ev3slot = assignments1.get(1).getLogicalSlotFuture();
		boolean ev3failed = ev3slot.isCompletedExceptionally();
		LogicalSlot slot = ev1failed ? ev3slot.join() : ev1slot.join();
		releaseLogicalSlot(slot);

		// EV3 needs again a physical slot, therefore there are 3 requests overall
		context.allocateSlotsFor(EV1, EV3);

		assertThat(context.getSlotProvider().getRequests().values(), hasSize(3));
		// either EV1 or EV3 logical slot future is fulfilled before cancellation
		assertThat(ev1failed != ev3failed, is(true));
		assertThat(assignments2.get(0).getLogicalSlotFuture().isCompletedExceptionally(), is(false));
	}

	private static void releaseLogicalSlot(LogicalSlot slot) {
		slot.tryAssignPayload(new DummyPayload(CompletableFuture.completedFuture(null)));
		slot.releaseSlot(new Throwable());
	}

	@Test
	public void testBulkClearIfPhysicalSlotRequestFails() {
		TestingPhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();
		AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

		context.allocateSlotsFor(EV1, EV3);
		SlotRequestId slotRequestId = context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
		context.getSlotProvider().failPhysicalSlotFutureFor(slotRequestId, new Throwable());
		PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

		assertThat(bulk.getPendingRequests(), hasSize(0));
	}

	@Test
	public void failLogicalSlotsIfPhysicalSlotIsFailed() {
		final TestingPhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();
		AllocationContext context = AllocationContext.newBuilder()
			.addGroup(EV1, EV2)
			.withBulkChecker(bulkChecker)
			.withPhysicalSlotFutureCompletionMode(PhysicalSlotFutureCompletionMode.FAILURE)
			.build();

		final List<SlotExecutionVertexAssignment> allocatedSlots = context.allocateSlotsFor(
			EV1,
			EV2);

		for (SlotExecutionVertexAssignment allocatedSlot : allocatedSlots) {
			assertTrue(allocatedSlot.getLogicalSlotFuture().isCompletedExceptionally());
		}

		assertThat(bulkChecker.getBulk().getPendingRequests(), is(empty()));

		final Set<SlotRequestId> requests = context.getSlotProvider().getRequests().keySet();
		assertThat(context.getSlotProvider().getCancellations().keySet(), is(requests));
	}

	private static List<ExecutionVertexID> getAssignIds(Collection<SlotExecutionVertexAssignment> assignments) {
		return assignments.stream().map(SlotExecutionVertexAssignment::getExecutionVertexId).collect(Collectors.toList());
	}

	private static AllocationContext createBulkCheckerContextWithEv12GroupAndEv3Group(PhysicalSlotRequestBulkChecker bulkChecker) {
		return AllocationContext
			.newBuilder()
			.addGroup(EV1, EV2)
			.addGroup(EV3)
			.withBulkChecker(bulkChecker)
			.withPhysicalSlotFutureCompletionMode(PhysicalSlotFutureCompletionMode.MANUAL)
			.build();
	}

	private static ResourceProfile fulfilOneOfTwoSlotRequestsAndGetPendingProfile(
		AllocationContext context,
		AllocationID allocationId) {
		Map<SlotRequestId, PhysicalSlotRequest> requests = context.getSlotProvider().getRequests();
		List<SlotRequestId> slotRequestIds = new ArrayList<>(requests.keySet());
		assertThat(slotRequestIds, hasSize(2));
		SlotRequestId slotRequestId1 = slotRequestIds.get(0);
		SlotRequestId slotRequestId2 = slotRequestIds.get(1);
		context.getSlotProvider().completePhysicalSlotFutureFor(slotRequestId1, allocationId);
		return requests.get(slotRequestId2).getSlotProfile().getPhysicalSlotResourceProfile();
	}

	private enum PhysicalSlotFutureCompletionMode {
		SUCCESS,
		FAILURE,
		MANUAL,
	}

	private static class AllocationContext {
		private final TestingPhysicalSlotProvider slotProvider;
		private final TestingSlotSharingStrategy slotSharingStrategy;
		private final SlotSharingExecutionSlotAllocator allocator;
		private final TestingSharedSlotProfileRetrieverFactory slotProfileRetrieverFactory;

		private AllocationContext(
				TestingPhysicalSlotProvider slotProvider,
				TestingSlotSharingStrategy slotSharingStrategy,
				SlotSharingExecutionSlotAllocator allocator,
				TestingSharedSlotProfileRetrieverFactory slotProfileRetrieverFactory) {
			this.slotProvider = slotProvider;
			this.slotSharingStrategy = slotSharingStrategy;
			this.allocator = allocator;
			this.slotProfileRetrieverFactory = slotProfileRetrieverFactory;
		}

		private SlotSharingExecutionSlotAllocator getAllocator() {
			return allocator;
		}

		private List<SlotExecutionVertexAssignment> allocateSlotsFor(ExecutionVertexID... ids) {
			List<ExecutionVertexSchedulingRequirements> requirements = Stream
				.of(ids)
				.map(id -> new ExecutionVertexSchedulingRequirements
					.Builder()
					.withExecutionVertexId(id)
					.withSlotSharingGroupId(new SlotSharingGroupId())
					.build())
				.collect(Collectors.toList());
			return allocator.allocateSlotsFor(requirements);
		}

		private TestingSlotSharingStrategy getSlotSharingStrategy() {
			return slotSharingStrategy;
		}

		private TestingPhysicalSlotProvider getSlotProvider() {
			return slotProvider;
		}

		private TestingSharedSlotProfileRetrieverFactory getSlotProfileRetrieverFactory() {
			return slotProfileRetrieverFactory;
		}

		private static Builder newBuilder() {
			return new Builder();
		}

		private static class Builder {
			private final Collection<ExecutionVertexID[]> groups = new ArrayList<>();
			private PhysicalSlotFutureCompletionMode physicalSlotFutureCompletion = PhysicalSlotFutureCompletionMode.SUCCESS;
			private boolean slotWillBeOccupiedIndefinitely = false;
			private PhysicalSlotRequestBulkChecker bulkChecker = new TestingPhysicalSlotRequestBulkChecker();

			@Nullable
			private PhysicalSlotProvider physicalSlotProvider = null;

			private Builder addGroup(ExecutionVertexID... group) {
				groups.add(group);
				return this;
			}

			private Builder withPhysicalSlotFutureCompletionMode(PhysicalSlotFutureCompletionMode physicalSlotFutureCompletion) {
				this.physicalSlotFutureCompletion = physicalSlotFutureCompletion;
				return this;
			}

			private Builder setSlotWillBeOccupiedIndefinitely(boolean slotWillBeOccupiedIndefinitely) {
				this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
				return this;
			}

			private Builder withBulkChecker(PhysicalSlotRequestBulkChecker bulkChecker) {
				this.bulkChecker = bulkChecker;
				return this;
			}

			private Builder withPhysicalSlotProvider(PhysicalSlotProvider physicalSlotProvider) {
				this.physicalSlotProvider = physicalSlotProvider;
				return this;
			}

			private AllocationContext build() {
				TestingPhysicalSlotProvider slotProvider = new TestingPhysicalSlotProvider(physicalSlotFutureCompletion);
				TestingSharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
					new TestingSharedSlotProfileRetrieverFactory();
				TestingSlotSharingStrategy slotSharingStrategy = TestingSlotSharingStrategy.createWithGroups(groups);
				SlotSharingExecutionSlotAllocator allocator = new SlotSharingExecutionSlotAllocator(
					slotProvider,
					slotWillBeOccupiedIndefinitely,
					slotSharingStrategy,
					sharedSlotProfileRetrieverFactory,
					bulkChecker,
					ALLOCATION_TIMEOUT,
					executionVertexID -> RESOURCE_PROFILE);
				return new AllocationContext(
					slotProvider,
					slotSharingStrategy,
					allocator,
					sharedSlotProfileRetrieverFactory);
			}
		}
	}

	private static class TestingPhysicalSlotProvider implements PhysicalSlotProvider {
		private final Map<SlotRequestId, PhysicalSlotRequest> requests;
		private final Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> responses;
		private final Map<SlotRequestId, Throwable> cancellations;
		private final PhysicalSlotFutureCompletionMode physicalSlotFutureCompletion;

		private TestingPhysicalSlotProvider(PhysicalSlotFutureCompletionMode physicalSlotFutureCompletion) {
			this.physicalSlotFutureCompletion = physicalSlotFutureCompletion;
			this.requests = new HashMap<>();
			this.responses = new HashMap<>();
			this.cancellations = new HashMap<>();
		}

		@Override
		public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(PhysicalSlotRequest physicalSlotRequest) {
			SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
			requests.put(slotRequestId, physicalSlotRequest);
			CompletableFuture<TestingPhysicalSlot> resultFuture = new CompletableFuture<>();
			responses.put(slotRequestId, resultFuture);

			switch (physicalSlotFutureCompletion) {
				case SUCCESS:
					completePhysicalSlotFutureFor(slotRequestId, new AllocationID());
					break;
				case FAILURE:
					resultFuture.completeExceptionally(new FlinkException("Test failure."));
					break;
			}

			return resultFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
		}

		private void completePhysicalSlotFutureFor(SlotRequestId slotRequestId, AllocationID allocationID) {
			ResourceProfile resourceProfile = requests.get(slotRequestId).getSlotProfile().getPhysicalSlotResourceProfile();
			TestingPhysicalSlot physicalSlot = new TestingPhysicalSlot(resourceProfile, allocationID);
			responses.get(slotRequestId).complete(physicalSlot);
		}

		private void failPhysicalSlotFutureFor(SlotRequestId slotRequestId, Throwable cause) {
			responses.get(slotRequestId).completeExceptionally(cause);
		}

		@Override
		public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
			cancellations.put(slotRequestId, cause);
		}

		private PhysicalSlotRequest getFirstRequestOrFail() {
			return getFirstElementOrFail(requests.values());
		}

		private Map<SlotRequestId, PhysicalSlotRequest> getRequests() {
			return Collections.unmodifiableMap(requests);
		}

		private CompletableFuture<TestingPhysicalSlot> getFirstResponseOrFail() {
			return getFirstElementOrFail(responses.values());
		}

		private Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> getResponses() {
			return Collections.unmodifiableMap(responses);
		}

		private Map<SlotRequestId, Throwable> getCancellations() {
			return Collections.unmodifiableMap(cancellations);
		}

		private static <T> T getFirstElementOrFail(Collection<T> collection) {
			Optional<T> element = collection.stream().findFirst();
			Preconditions.checkState(element.isPresent());
			return element.get();
		}
	}

	private static class TestingSlotSharingStrategy implements SlotSharingStrategy {
		private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups;

		private TestingSlotSharingStrategy(Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups) {
			this.executionSlotSharingGroups = executionSlotSharingGroups;
		}

		@Override
		public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(ExecutionVertexID executionVertexId) {
			return executionSlotSharingGroups.get(executionVertexId);
		}

		@Override
		public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
			return new HashSet<>(executionSlotSharingGroups.values());
		}

		private static TestingSlotSharingStrategy createWithGroups(Iterable<ExecutionVertexID[]> groups) {
			Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups = new HashMap<>();
			for (ExecutionVertexID[] group : groups) {
				ExecutionSlotSharingGroup executionSlotSharingGroup = new ExecutionSlotSharingGroup();
				for (ExecutionVertexID executionVertexId : group) {
					executionSlotSharingGroup.addVertex(executionVertexId);
					executionSlotSharingGroups.put(executionVertexId, executionSlotSharingGroup);
				}
			}
			return new TestingSlotSharingStrategy(executionSlotSharingGroups);
		}
	}

	private static class TestingSharedSlotProfileRetrieverFactory implements SharedSlotProfileRetrieverFactory {
		private final List<Set<ExecutionVertexID>> askedBulks;
		private final List<ExecutionSlotSharingGroup> askedGroups;

		private TestingSharedSlotProfileRetrieverFactory() {
			this.askedBulks = new ArrayList<>();
			this.askedGroups = new ArrayList<>();
		}

		@Override
		public SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk) {
			askedBulks.add(bulk);
			return (group, resourceProfile) -> {
				askedGroups.add(group);
				return SlotProfile.noLocality(resourceProfile);
			};
		}

		private List<Set<ExecutionVertexID>> getAskedBulks() {
			return Collections.unmodifiableList(askedBulks);
		}

		private List<ExecutionSlotSharingGroup> getAskedGroups() {
			return Collections.unmodifiableList(askedGroups);
		}
	}

	private static class TestingPhysicalSlotRequestBulkChecker implements PhysicalSlotRequestBulkChecker {
		private PhysicalSlotRequestBulk bulk;
		private Time timeout;

		@Override
		public void start(ComponentMainThreadExecutor mainThreadExecutor) {

		}

		@Override
		public void schedulePendingRequestBulkTimeoutCheck(PhysicalSlotRequestBulk bulk, Time timeout) {
			this.bulk = bulk;
			this.timeout = timeout;
		}

		private PhysicalSlotRequestBulk getBulk() {
			return bulk;
		}

		private Time getTimeout() {
			return timeout;
		}
	}
}

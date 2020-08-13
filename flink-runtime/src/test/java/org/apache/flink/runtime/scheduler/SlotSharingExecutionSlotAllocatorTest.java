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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.junit.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link SlotSharingExecutionSlotAllocator}.
 */
public class SlotSharingExecutionSlotAllocatorTest {
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
	public void testSlotRequestCompletionAfterProfileCompletion() {
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).completeSlotProfileFutureManually().build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(EV1);

		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(EV1, EV2);

		executionVertexAssignments.forEach(assignment -> assertThat(assignment.getLogicalSlotFuture().isDone(), is(false)));
		context.getSlotProfileRetrieverFactory().completeSlotProfileFutureFor(executionSlotSharingGroup);
		executionVertexAssignments.forEach(assignment -> assertThat(assignment.getLogicalSlotFuture().isDone(), is(true)));
	}

	@Test
	public void testSlotRequestProfile() {
		ResourceProfile physicalsSlotResourceProfile = ResourceProfile.fromResources(3, 5);
		AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(EV1);
		context.getSlotProfileRetrieverFactory().addGroupResourceProfile(executionSlotSharingGroup, physicalsSlotResourceProfile);

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
			.completePhysicalSlotFutureManually()
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
	public void testReturningLogicalSlotsRemovesSharedSlot() {
		testLogicalSlotRequestCancellation(
			false,
			(context, assignment) -> {
				try {
					assignment.getLogicalSlotFuture().get().releaseSlot(null);
				} catch (InterruptedException | ExecutionException e) {
					throw new FlinkRuntimeException("Unexpected", e);
				}
			});
	}

	@Test
	public void testLogicalSlotCancelsPhysicalSlotRequestAndRemovesSharedSlot() {
		testLogicalSlotRequestCancellation(
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

	private static void testLogicalSlotRequestCancellation(
			boolean completePhysicalSlotFutureManually,
			BiConsumer<AllocationContext, SlotExecutionVertexAssignment> cancelAction) {
		AllocationContext context = AllocationContext
			.newBuilder()
			.addGroup(EV1, EV2)
			.completePhysicalSlotFutureManually(completePhysicalSlotFutureManually)
			.build();

		List<SlotExecutionVertexAssignment> assignments = context.allocateSlotsFor(EV1, EV2);
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel only one sharing logical slots
		cancelAction.accept(context, assignments.get(0));
		List<SlotExecutionVertexAssignment> assignmentsAfterOneCancellation = context.allocateSlotsFor(EV1, EV2);
		// there should be no more physical slot allocations, as the first logical slot reuses the previous shared slot
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel all sharing logical slots
		for (SlotExecutionVertexAssignment assignment : assignmentsAfterOneCancellation) {
			cancelAction.accept(context, assignment);
		}
		SlotRequestId slotRequestId = context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
		assertThat(context.getSlotProvider().getCancellations().containsKey(slotRequestId), is(true));

		context.allocateSlotsFor(EV1, EV2);
		// there should be one more physical slot allocation, as the first allocation should be removed after releasing all logical slots
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
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
		TestingPhysicalSlot physicalSlot = context.getSlotProvider().getFirstResponseOrFail().get();

		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(false));
		assertThat(physicalSlot.getPayload(), notNullValue());
		physicalSlot.getPayload().release(new Throwable());
		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(true));
	}

	private static ExecutionVertexID createRandomExecutionVertexId() {
		return new ExecutionVertexID(new JobVertexID(), 0);
	}

	private static List<ExecutionVertexID> getAssignIds(Collection<SlotExecutionVertexAssignment> assignments) {
		return assignments.stream().map(SlotExecutionVertexAssignment::getExecutionVertexId).collect(Collectors.toList());
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
			private boolean completePhysicalSlotFutureManually = false;
			private boolean completeSlotProfileFutureManually = false;
			private boolean slotWillBeOccupiedIndefinitely = false;

			private Builder addGroup(ExecutionVertexID... group) {
				groups.add(group);
				return this;
			}

			private Builder completePhysicalSlotFutureManually() {
				completePhysicalSlotFutureManually(true);
				return this;
			}

			private Builder completePhysicalSlotFutureManually(boolean value) {
				this.completePhysicalSlotFutureManually = value;
				return this;
			}

			private Builder completeSlotProfileFutureManually() {
				this.completeSlotProfileFutureManually = true;
				return this;
			}

			private Builder setSlotWillBeOccupiedIndefinitely(boolean slotWillBeOccupiedIndefinitely) {
				this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
				return this;
			}

			private AllocationContext build() {
				TestingPhysicalSlotProvider slotProvider = new TestingPhysicalSlotProvider(completePhysicalSlotFutureManually);
				TestingSharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
					new TestingSharedSlotProfileRetrieverFactory(completeSlotProfileFutureManually);
				TestingSlotSharingStrategy slotSharingStrategy = TestingSlotSharingStrategy.createWithGroups(groups);
				SlotSharingExecutionSlotAllocator allocator = new SlotSharingExecutionSlotAllocator(
					slotProvider,
					slotWillBeOccupiedIndefinitely,
					slotSharingStrategy,
					sharedSlotProfileRetrieverFactory);
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
		private final boolean completePhysicalSlotFutureManually;

		private TestingPhysicalSlotProvider(boolean completePhysicalSlotFutureManually) {
			this.completePhysicalSlotFutureManually = completePhysicalSlotFutureManually;
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
			if (!completePhysicalSlotFutureManually) {
				completePhysicalSlotFutureFor(slotRequestId);
			}
			return resultFuture.thenApply(physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
		}

		private void completePhysicalSlotFutureFor(SlotRequestId slotRequestId) {
			ResourceProfile resourceProfile = requests.get(slotRequestId).getSlotProfile().getPhysicalSlotResourceProfile();
			TestingPhysicalSlot physicalSlot = new TestingPhysicalSlot(resourceProfile);
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
		private final Map<ExecutionSlotSharingGroup, ResourceProfile> resourceProfiles;
		private final Map<ExecutionSlotSharingGroup, CompletableFuture<SlotProfile>> slotProfileFutures;
		private final boolean completeSlotProfileFutureManually;

		private TestingSharedSlotProfileRetrieverFactory(boolean completeSlotProfileFutureManually) {
			this.completeSlotProfileFutureManually = completeSlotProfileFutureManually;
			this.askedBulks = new ArrayList<>();
			this.askedGroups = new ArrayList<>();
			this.resourceProfiles = new IdentityHashMap<>();
			this.slotProfileFutures = new IdentityHashMap<>();
		}

		@Override
		public SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk) {
			askedBulks.add(bulk);
			return group -> {
				askedGroups.add(group);
				CompletableFuture<SlotProfile> slotProfileFuture =
					slotProfileFutures.computeIfAbsent(group, g -> new CompletableFuture<>());
				if (!completeSlotProfileFutureManually) {
					completeSlotProfileFutureFor(group);
				}
				return slotProfileFuture;
			};
		}

		private void addGroupResourceProfile(ExecutionSlotSharingGroup group, ResourceProfile resourceProfile) {
			resourceProfiles.put(group, resourceProfile);
		}

		private void completeSlotProfileFutureFor(ExecutionSlotSharingGroup group) {
			slotProfileFutures.get(group).complete(SlotProfile.noLocality(resourceProfiles.getOrDefault(group, ResourceProfile.ANY)));
		}

		private List<Set<ExecutionVertexID>> getAskedBulks() {
			return Collections.unmodifiableList(askedBulks);
		}

		private List<ExecutionSlotSharingGroup> getAskedGroups() {
			return Collections.unmodifiableList(askedGroups);
		}
	}

	private static class TestingPhysicalSlot extends SimpleSlotContext implements PhysicalSlot {
		@Nullable
		private Payload payload;

		private TestingPhysicalSlot(ResourceProfile resourceProfile) {
			super(
				new AllocationID(),
				new LocalTaskManagerLocation(),
				0,
				new SimpleAckingTaskManagerGateway(),
				resourceProfile);
		}

		@Override
		public boolean tryAssignPayload(Payload payload) {
			this.payload = payload;
			return true;
		}

		@Nullable
		private Payload getPayload() {
			return payload;
		}
	}
}

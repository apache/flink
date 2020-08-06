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
import org.powermock.core.IdentityHashSet;

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
import java.util.stream.IntStream;

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
	@Test
	public void testSlotProfileRequestAskedBulkAndGroup() {
		AllocationContext context = AllocationContext.newBuilder().setGroups(2).build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(context.getReqIds(0, 1).get(0));

		context.allocateSlotsFor(0, 2);

		Set<ExecutionVertexID> ids = new HashSet<>(context.getReqIds(0, 2));
		assertThat(context.getSlotProfileRetrieverFactory().getAskedBulks(), containsInAnyOrder(ids));
		assertThat(context.getSlotProfileRetrieverFactory().getAskedGroups(), containsInAnyOrder(executionSlotSharingGroup));
	}

	@Test
	public void testSlotRequestCompletionAfterProfileCompletion() {
		AllocationContext context = AllocationContext.newBuilder().setGroups(2).completeSlotProfileFutureManually().build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(context.getReqIds(0, 1).get(0));

		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(0, 2);

		executionVertexAssignments.forEach(assignment -> assertThat(assignment.getLogicalSlotFuture().isDone(), is(false)));
		context.getSlotProfileRetrieverFactory().completeSlotProfileFutureFor(executionSlotSharingGroup);
		executionVertexAssignments.forEach(assignment -> assertThat(assignment.getLogicalSlotFuture().isDone(), is(true)));
	}

	@Test
	public void testSlotRequestProfile() {
		ResourceProfile physicalsSlotResourceProfile = ResourceProfile.fromResources(3, 5);
		AllocationContext context = AllocationContext.newBuilder().setGroups(2).build();
		ExecutionSlotSharingGroup executionSlotSharingGroup =
			context.getSlotSharingStrategy().getExecutionSlotSharingGroup(context.getReqIds(0, 1).get(0));
		context.getSlotProfileRetrieverFactory().addGroupResourceProfile(executionSlotSharingGroup, physicalsSlotResourceProfile);

		context.allocateSlotsFor(0, 2);

		Optional<PhysicalSlotRequest> slotRequest = context.getSlotProvider().getRequests().values().stream().findFirst();
		assertThat(slotRequest.isPresent(), is(true));
		slotRequest.ifPresent(r -> assertThat(r.getSlotProfile().getPhysicalSlotResourceProfile(), is(physicalsSlotResourceProfile)));
	}

	@Test
	public void testNewAllocatePhysicalSlotForSharedSlot() {
		AllocationContext context = AllocationContext.newBuilder().setGroups(2, 2).build();

		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(0, 4);
		Collection<ExecutionVertexID> assignIds = getAssignIds(executionVertexAssignments);

		assertThat(assignIds, containsInAnyOrder(context.getReqIds(0, 4).toArray()));
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
	}

	@Test
	public void testAllocateLogicalSlotFromAvailableSharedSlot() {
		AllocationContext context = AllocationContext.newBuilder().setGroups(2).build();

		context.allocateSlotsFor(0, 1);
		List<SlotExecutionVertexAssignment> executionVertexAssignments = context.allocateSlotsFor(1, 2);
		Collection<ExecutionVertexID> assignIds = getAssignIds(executionVertexAssignments);

		// execution 0 from the first allocateSlotsFor call and execution 1 from the second allocateSlotsFor call
		// share a slot, therefore only one physical slot allocation should happen
		assertThat(assignIds, containsInAnyOrder(context.getReqIds(1, 2).toArray()));
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));
	}

	@Test
	public void testDuplicateAllocationDoesNotRecreateLogicalSlotFuture() throws ExecutionException, InterruptedException {
		AllocationContext context = AllocationContext.newBuilder().setGroups(1).build();

		SlotExecutionVertexAssignment assignment1 = context.allocateSlotsFor(0, 1).get(0);
		SlotExecutionVertexAssignment assignment2 = context.allocateSlotsFor(0, 1).get(0);

		assertThat(assignment1.getLogicalSlotFuture().get() == assignment2.getLogicalSlotFuture().get(), is(true));
	}

	@Test
	public void testFailedPhysicalSlotRequestFailsLogicalSlotFuturesAndRemovesSharedSlot() {
		AllocationContext context = AllocationContext
			.newBuilder()
			.setGroups(1)
			.completePhysicalSlotFutureManually()
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = context.allocateSlotsFor(0, 1).get(0).getLogicalSlotFuture();
		SlotRequestId slotRequestId = context.getSlotProvider().getRequests().values().stream().findFirst().get().getSlotRequestId();

		assertThat(logicalSlotFuture.isDone(), is(false));
		context.getSlotProvider().failPhysicalSlotFutureFor(slotRequestId, new Throwable());
		assertThat(logicalSlotFuture.isCompletedExceptionally(), is(true));

		// next allocation allocates new shared slot
		context.allocateSlotsFor(0, 1);
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
			.setGroups(1)
			.setSlotWillBeOccupiedIndefinitely(slotWillBeOccupiedIndefinitely)
			.build();
		context.allocateSlotsFor(0, 1);

		PhysicalSlotRequest slotRequest = context.getSlotProvider().getRequests().values().stream().findFirst().get();
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
					fail("THe logical future must finish with the cancellation exception");
				} catch (InterruptedException | ExecutionException e) {
					assertThat(e.getCause(), instanceOf(CancellationException.class));
				}
			});
	}

	private static void testLogicalSlotRequestCancellation(
			boolean completePhysicalSlotFutureManually,
			BiConsumer<AllocationContext, SlotExecutionVertexAssignment> cancelAction) {
		//if (completePhysicalSlotRequest) {
		AllocationContext context = AllocationContext
			.newBuilder()
			.setGroups(2)
			.completePhysicalSlotFutureManually(completePhysicalSlotFutureManually)
			.build();

		List<SlotExecutionVertexAssignment> assignments = context.allocateSlotsFor(0, 2);
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel only one sharing logical slots
		cancelAction.accept(context, assignments.get(0));
		assignments = context.allocateSlotsFor(0, 2);
		// there should be no more physical slot allocations, as the first logical slot reuses the previous shared slot
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(1));

		// cancel all sharing logical slots
		for (SlotExecutionVertexAssignment assignment : assignments) {
			cancelAction.accept(context, assignment);
		}
		SlotRequestId slotRequestId = context.getSlotProvider().getRequests().keySet().stream().findFirst().get();
		assertThat(context.getSlotProvider().getCancelations().containsKey(slotRequestId), is(true));

		context.allocateSlotsFor(0, 2);
		// there should be one more physical slot allocation, as the first allocation should be removed after releasing all logical slots
		assertThat(context.getSlotProvider().getRequests().keySet(), hasSize(2));
	}

	@Test
	public void testPhysicalSlotReleaseLogicalSlots() throws ExecutionException, InterruptedException {
		AllocationContext context = AllocationContext.newBuilder().setGroups(2).build();
		List<SlotExecutionVertexAssignment> assignments = context.allocateSlotsFor(0, 2);
		List<TestingPayload> payloads = assignments
			.stream()
			.map(assignment -> {
				TestingPayload payload = new TestingPayload();
				assignment.getLogicalSlotFuture().thenAccept(logicalSlot -> logicalSlot.tryAssignPayload(payload));
				return payload;
			})
			.collect(Collectors.toList());
		TestingPhysicalSlot physicalSlot = context.getSlotProvider().getResponses().values().stream().findFirst().get().get();

		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(false));
		assertThat(physicalSlot.getPayload(), notNullValue());
		physicalSlot.getPayload().release(new Throwable());
		assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()), is(true));
	}

	private static List<ExecutionVertexID> getAssignIds(Collection<SlotExecutionVertexAssignment> assignments) {
		return assignments.stream().map(SlotExecutionVertexAssignment::getExecutionVertexId).collect(Collectors.toList());
	}

	private static class AllocationContext {
		private final List<ExecutionVertexSchedulingRequirements> requirements;
		private final TestingPhysicalSlotProvider slotProvider;
		private final TestingSlotSharingStrategy slotSharingStrategy;
		private final SlotSharingExecutionSlotAllocator allocator;
		private final TestingSharedSlotProfileRetrieverFactory slotProfileRetrieverFactory;

		AllocationContext(
				List<ExecutionVertexSchedulingRequirements> requirements,
				TestingPhysicalSlotProvider slotProvider,
				TestingSlotSharingStrategy slotSharingStrategy,
				SlotSharingExecutionSlotAllocator allocator,
				TestingSharedSlotProfileRetrieverFactory slotProfileRetrieverFactory) {
			this.requirements = requirements;
			this.slotProvider = slotProvider;
			this.slotSharingStrategy = slotSharingStrategy;
			this.allocator = allocator;
			this.slotProfileRetrieverFactory = slotProfileRetrieverFactory;
		}

		public SlotSharingExecutionSlotAllocator getAllocator() {
			return allocator;
		}

		private List<SlotExecutionVertexAssignment> allocateSlotsFor(int start, int end) {
			return allocator.allocateSlotsFor(requirements.subList(start, end));
		}

		private TestingSlotSharingStrategy getSlotSharingStrategy() {
			return slotSharingStrategy;
		}

		private List<ExecutionVertexID> getReqIds(int start, int end) {
			return getReqIds(requirements.subList(start, end));
		}

		private TestingPhysicalSlotProvider getSlotProvider() {
			return slotProvider;
		}

		private TestingSharedSlotProfileRetrieverFactory getSlotProfileRetrieverFactory() {
			return slotProfileRetrieverFactory;
		}

		private static List<ExecutionVertexID> getReqIds(Collection<ExecutionVertexSchedulingRequirements> requirements) {
			return requirements.stream().map(ExecutionVertexSchedulingRequirements::getExecutionVertexId).collect(Collectors.toList());
		}

		static Builder newBuilder() {
			return new Builder();
		}

		private static class Builder {
			private int[] groups = { 2, 1 }; // 2 executions in the first group, 1 in the second etc
			private List<ResourceProfile> resourceProfiles;
			private boolean completePhysicalSlotFutureManually;
			private boolean completeSlotProfileFutureManually;
			private boolean slotWillBeOccupiedIndefinitely;

			private Builder setGroups(int... groups) {
				int reqNumber = IntStream.of(groups).sum();
				List<ResourceProfile> resourceProfiles = Collections.nCopies(reqNumber, ResourceProfile.UNKNOWN);
				return setGroupsWithResourceProfiles(resourceProfiles, groups);
			}

			private Builder setGroupsWithResourceProfiles(List<ResourceProfile> resourceProfiles, int... groups) {
				Preconditions.checkArgument(resourceProfiles.size() == IntStream.of(groups).sum());
				this.resourceProfiles = resourceProfiles;
				this.groups = groups;
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
				List<ExecutionVertexSchedulingRequirements> requirements = createSchedulingRequirements();
				TestingPhysicalSlotProvider slotProvider = new TestingPhysicalSlotProvider(completePhysicalSlotFutureManually);
				TestingSharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
					new TestingSharedSlotProfileRetrieverFactory(completeSlotProfileFutureManually);
				TestingSlotSharingStrategy slotSharingStrategy = TestingSlotSharingStrategy.createWithGroups(getReqIds(requirements), groups);
				SlotSharingExecutionSlotAllocator allocator = new SlotSharingExecutionSlotAllocator(
					slotProvider,
					slotWillBeOccupiedIndefinitely,
					slotSharingStrategy,
					sharedSlotProfileRetrieverFactory);
				return new AllocationContext(
					requirements,
					slotProvider,
					slotSharingStrategy,
					allocator,
					sharedSlotProfileRetrieverFactory);
			}

			private List<ExecutionVertexSchedulingRequirements> createSchedulingRequirements() {
				return resourceProfiles
					.stream()
					.map(resourceProfile ->
						new ExecutionVertexSchedulingRequirements
							.Builder()
							.withExecutionVertexId(new ExecutionVertexID(new JobVertexID(), 0))
							.withTaskResourceProfile(resourceProfile)
							.withPhysicalSlotResourceProfile(resourceProfile) // not used
							.build())
					.collect(Collectors.toList());
			}
		}
	}

	private static class TestingPhysicalSlotProvider implements PhysicalSlotProvider {
		private final Map<SlotRequestId, PhysicalSlotRequest> requests;
		private final Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> responses;
		private final Map<SlotRequestId, Throwable> cancelations;
		private final boolean completePhysicalSlotFutureManually;

		private TestingPhysicalSlotProvider(boolean completePhysicalSlotFutureManually) {
			this.completePhysicalSlotFutureManually = completePhysicalSlotFutureManually;
			this.requests = new HashMap<>();
			this.responses = new HashMap<>();
			this.cancelations = new HashMap<>();
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
			cancelations.put(slotRequestId, cause);
		}

		private Map<SlotRequestId, PhysicalSlotRequest> getRequests() {
			return Collections.unmodifiableMap(requests);
		}

		private Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> getResponses() {
			return Collections.unmodifiableMap(responses);
		}

		private Map<SlotRequestId, Throwable> getCancelations() {
			return Collections.unmodifiableMap(cancelations);
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
			Set<ExecutionSlotSharingGroup> groups = new IdentityHashSet<>();
			groups.addAll(executionSlotSharingGroups.values());
			return groups;
		}

		private static TestingSlotSharingStrategy createWithGroups(
				List<ExecutionVertexID> executionVertexIds,
				int... groupSizes) {
			Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups = new HashMap<>();
			int startIndex = 0;
			int nextIndex = 0;
			for (int groupSize : groupSizes) {
				nextIndex = startIndex + groupSize;
				createGroup(executionSlotSharingGroups, executionVertexIds, startIndex, nextIndex);
				startIndex = nextIndex;
			}
			if (nextIndex < executionVertexIds.size()) {
				createGroup(executionSlotSharingGroups, executionVertexIds, nextIndex, executionVertexIds.size());
			}
			return new TestingSlotSharingStrategy(executionSlotSharingGroups);
		}

		private static void createGroup(
				Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups,
				List<ExecutionVertexID> executionVertexIds,
				int startIndex,
				int nextIndex) {
			ExecutionSlotSharingGroup executionSlotSharingGroup = new ExecutionSlotSharingGroup();
			executionSlotSharingGroup.addVertex(new ExecutionVertexID(new JobVertexID(), 0));
			executionVertexIds.subList(startIndex, nextIndex).forEach(executionVertexId -> {
				executionSlotSharingGroup.addVertex(executionVertexId);
				executionSlotSharingGroups.put(executionVertexId, executionSlotSharingGroup);
			});
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

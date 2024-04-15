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
import org.apache.flink.runtime.clusterframework.types.SlotProfileTestingUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingPayload;
import org.apache.flink.runtime.jobmaster.slotpool.DummyPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulk;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
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

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suite for {@link SlotSharingExecutionSlotAllocator}. */
class SlotSharingExecutionSlotAllocatorTest {
    private static final Time ALLOCATION_TIMEOUT = Time.milliseconds(100L);
    private static final ResourceProfile RESOURCE_PROFILE = ResourceProfile.fromResources(3, 5);

    private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV2 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV3 = createRandomExecutionVertexId();
    private static final ExecutionVertexID EV4 = createRandomExecutionVertexId();

    @Test
    void testSlotProfileRequestAskedBulkAndGroup() {
        AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                context.getSlotSharingStrategy().getExecutionSlotSharingGroup(EV1);

        context.allocateSlotsFor(EV1, EV2);

        List<Set<ExecutionVertexID>> askedBulks =
                context.getSlotProfileRetrieverFactory().getAskedBulks();
        assertThat(askedBulks).hasSize(1);
        assertThat(askedBulks.get(0)).containsExactlyInAnyOrder(EV1, EV2);
        assertThat(context.getSlotProfileRetrieverFactory().getAskedGroups())
                .containsExactly(executionSlotSharingGroup);
    }

    @Test
    void testSlotRequestProfile() {
        AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2, EV3).build();
        ResourceProfile physicalsSlotResourceProfile = RESOURCE_PROFILE.multiply(3);

        context.allocateSlotsFor(EV1, EV2);

        Optional<PhysicalSlotRequest> slotRequest =
                context.getSlotProvider().getRequests().values().stream().findFirst();
        assertThat(slotRequest).isPresent();
        assertThat(slotRequest.get().getSlotProfile().getPhysicalSlotResourceProfile())
                .isEqualTo(physicalsSlotResourceProfile);
    }

    @Test
    void testAllocatePhysicalSlotForNewSharedSlot() {
        AllocationContext context =
                AllocationContext.newBuilder().addGroup(EV1, EV2).addGroup(EV3, EV4).build();

        Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignments =
                context.allocateSlotsFor(EV1, EV2, EV3, EV4);
        Collection<ExecutionVertexID> assignIds = getAssignIds(executionSlotAssignments.values());

        assertThat(assignIds).containsExactlyInAnyOrder(EV1, EV2, EV3, EV4);
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);
    }

    @Test
    void testAllocateLogicalSlotFromAvailableSharedSlot() {
        AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();

        context.allocateSlotsFor(EV1);
        Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignments =
                context.allocateSlotsFor(EV2);
        Collection<ExecutionVertexID> assignIds = getAssignIds(executionSlotAssignments.values());

        // execution 0 from the first allocateSlotsFor call and execution 1 from the second
        // allocateSlotsFor call
        // share a slot, therefore only one physical slot allocation should happen
        assertThat(assignIds).containsExactly(EV2);
        assertThat(context.getSlotProvider().getRequests()).hasSize(1);
    }

    @Test
    void testDuplicateAllocationDoesNotRecreateLogicalSlotFuture()
            throws ExecutionException, InterruptedException {
        AllocationContext context = AllocationContext.newBuilder().addGroup(EV1).build();

        ExecutionSlotAssignment assignment1 =
                getAssignmentByExecutionVertexId(context.allocateSlotsFor(EV1), EV1);
        ExecutionSlotAssignment assignment2 =
                getAssignmentByExecutionVertexId(context.allocateSlotsFor(EV1), EV1);

        assertThat(assignment1.getLogicalSlotFuture().get())
                .isSameAs(assignment2.getLogicalSlotFuture().get());
    }

    @Test
    void testFailedPhysicalSlotRequestFailsLogicalSlotFuturesAndRemovesSharedSlot() {
        AllocationContext context =
                AllocationContext.newBuilder()
                        .addGroup(EV1)
                        .withPhysicalSlotProvider(
                                TestingPhysicalSlotProvider
                                        .createWithoutImmediatePhysicalSlotCreation())
                        .build();
        CompletableFuture<LogicalSlot> logicalSlotFuture =
                getAssignmentByExecutionVertexId(context.allocateSlotsFor(EV1), EV1)
                        .getLogicalSlotFuture();
        SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();

        assertThat(logicalSlotFuture).isNotDone();
        context.getSlotProvider()
                .getResponses()
                .get(slotRequestId)
                .completeExceptionally(new Throwable());
        assertThat(logicalSlotFuture).isCompletedExceptionally();

        // next allocation allocates new shared slot
        context.allocateSlotsFor(EV1);
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);
    }

    @Test
    void testSlotWillBeOccupiedIndefinitelyFalse() throws ExecutionException, InterruptedException {
        testSlotWillBeOccupiedIndefinitely(false);
    }

    @Test
    void testSlotWillBeOccupiedIndefinitelyTrue() throws ExecutionException, InterruptedException {
        testSlotWillBeOccupiedIndefinitely(true);
    }

    private static void testSlotWillBeOccupiedIndefinitely(boolean slotWillBeOccupiedIndefinitely)
            throws ExecutionException, InterruptedException {
        AllocationContext context =
                AllocationContext.newBuilder()
                        .addGroup(EV1)
                        .setSlotWillBeOccupiedIndefinitely(slotWillBeOccupiedIndefinitely)
                        .build();
        context.allocateSlotsFor(EV1);

        PhysicalSlotRequest slotRequest = context.getSlotProvider().getFirstRequestOrFail();
        assertThat(slotRequest.willSlotBeOccupiedIndefinitely())
                .isEqualTo(slotWillBeOccupiedIndefinitely);

        TestingPhysicalSlot physicalSlot =
                context.getSlotProvider().getResponses().get(slotRequest.getSlotRequestId()).get();
        assertThat(physicalSlot.getPayload()).isNotNull();
        assertThat(physicalSlot.getPayload().willOccupySlotIndefinitely())
                .isEqualTo(slotWillBeOccupiedIndefinitely);
    }

    @Test
    void testReturningLogicalSlotsRemovesSharedSlot() throws Exception {
        // physical slot request is completed and completes logical requests
        testLogicalSlotRequestCancellationOrRelease(
                false,
                true,
                (context, assignment) -> assignment.getLogicalSlotFuture().get().releaseSlot(null));
    }

    @Test
    void testLogicalSlotCancelsPhysicalSlotRequestAndRemovesSharedSlot() throws Exception {
        // physical slot request is not completed and does not complete logical requests
        testLogicalSlotRequestCancellationOrRelease(
                true,
                true,
                (context, assignment) -> {
                    context.getAllocator().cancel(assignment.getExecutionAttemptId());
                    assertThatThrownBy(
                                    () -> {
                                        context.getAllocator()
                                                .cancel(assignment.getExecutionAttemptId());
                                        assignment.getLogicalSlotFuture().get();
                                    })
                            .as("The logical future must finish with the cancellation exception.")
                            .hasCauseInstanceOf(CancellationException.class);
                });
    }

    @Test
    void
            testCompletedLogicalSlotCancelationDoesNotCancelPhysicalSlotRequestAndDoesNotRemoveSharedSlot()
                    throws Exception {
        // physical slot request is completed and completes logical requests
        testLogicalSlotRequestCancellationOrRelease(
                false,
                false,
                (context, assignment) -> {
                    context.getAllocator().cancel(assignment.getExecutionAttemptId());
                    assignment.getLogicalSlotFuture().get();
                });
    }

    private static void testLogicalSlotRequestCancellationOrRelease(
            boolean completePhysicalSlotFutureManually,
            boolean cancelsPhysicalSlotRequestAndRemovesSharedSlot,
            BiConsumerWithException<AllocationContext, ExecutionSlotAssignment, Exception>
                    cancelOrReleaseAction)
            throws Exception {
        AllocationContext.Builder allocationContextBuilder =
                AllocationContext.newBuilder().addGroup(EV1, EV2, EV3);
        if (completePhysicalSlotFutureManually) {
            allocationContextBuilder.withPhysicalSlotProvider(
                    TestingPhysicalSlotProvider.createWithoutImmediatePhysicalSlotCreation());
        }
        AllocationContext context = allocationContextBuilder.build();

        Map<ExecutionAttemptID, ExecutionSlotAssignment> assignments =
                context.allocateSlotsFor(EV1, EV2);
        assertThat(context.getSlotProvider().getRequests()).hasSize(1);

        // cancel or release only one sharing logical slots
        cancelOrReleaseAction.accept(context, getAssignmentByExecutionVertexId(assignments, EV1));
        Map<ExecutionAttemptID, ExecutionSlotAssignment> assignmentsAfterOneCancellation =
                context.allocateSlotsFor(EV1, EV2);
        // there should be no more physical slot allocations, as the first logical slot reuses the
        // previous shared slot
        assertThat(context.getSlotProvider().getRequests()).hasSize(1);

        // cancel or release all sharing logical slots
        for (ExecutionSlotAssignment assignment : assignmentsAfterOneCancellation.values()) {
            cancelOrReleaseAction.accept(context, assignment);
        }
        SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
        assertThat(context.getSlotProvider().getCancellations().containsKey(slotRequestId))
                .isEqualTo(cancelsPhysicalSlotRequestAndRemovesSharedSlot);

        context.allocateSlotsFor(EV3);
        // there should be one more physical slot allocation if the first allocation should be
        // removed with all logical slots
        int expectedNumberOfRequests = cancelsPhysicalSlotRequestAndRemovesSharedSlot ? 2 : 1;
        assertThat(context.getSlotProvider().getRequests()).hasSize(expectedNumberOfRequests);
    }

    @Test
    void testPhysicalSlotReleaseLogicalSlots() throws ExecutionException, InterruptedException {
        AllocationContext context = AllocationContext.newBuilder().addGroup(EV1, EV2).build();
        Map<ExecutionAttemptID, ExecutionSlotAssignment> assignments =
                context.allocateSlotsFor(EV1, EV2);
        List<TestingPayload> payloads =
                assignments.values().stream()
                        .map(
                                assignment -> {
                                    TestingPayload payload = new TestingPayload();
                                    assignment
                                            .getLogicalSlotFuture()
                                            .thenAccept(
                                                    logicalSlot ->
                                                            logicalSlot.tryAssignPayload(payload));
                                    return payload;
                                })
                        .collect(Collectors.toList());
        SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
        TestingPhysicalSlot physicalSlot = context.getSlotProvider().getFirstResponseOrFail().get();

        assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()))
                .isFalse();
        assertThat(physicalSlot.getPayload()).isNotNull();
        physicalSlot.getPayload().release(new Throwable());
        assertThat(payloads.stream().allMatch(payload -> payload.getTerminalStateFuture().isDone()))
                .isTrue();

        assertThat(context.getSlotProvider().getCancellations()).containsKey(slotRequestId);

        context.allocateSlotsFor(EV1, EV2);
        // there should be one more physical slot allocation, as the first allocation should be
        // removed after releasing all logical slots
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);
    }

    @Test
    void testSchedulePendingRequestBulkTimeoutCheck() {
        TestingPhysicalSlotRequestBulkChecker bulkChecker =
                new TestingPhysicalSlotRequestBulkChecker();
        AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

        context.allocateSlotsFor(EV1, EV3);
        PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

        assertThat(bulk.getPendingRequests()).hasSize(2);
        assertThat(bulk.getPendingRequests())
                .containsExactlyInAnyOrder(RESOURCE_PROFILE.multiply(2), RESOURCE_PROFILE);
        assertThat(bulk.getAllocationIdsOfFulfilledRequests()).isEmpty();
        assertThat(bulkChecker.getTimeout()).isEqualTo(ALLOCATION_TIMEOUT);
    }

    @Test
    void testRequestFulfilledInBulk() {
        TestingPhysicalSlotRequestBulkChecker bulkChecker =
                new TestingPhysicalSlotRequestBulkChecker();
        AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

        context.allocateSlotsFor(EV1, EV3);
        AllocationID allocationId = new AllocationID();
        ResourceProfile pendingSlotResourceProfile =
                fulfilOneOfTwoSlotRequestsAndGetPendingProfile(context, allocationId);
        PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

        assertThat(bulk.getPendingRequests()).hasSize(1);
        assertThat(bulk.getPendingRequests()).containsExactly(pendingSlotResourceProfile);
        assertThat(bulk.getAllocationIdsOfFulfilledRequests()).hasSize(1);
        assertThat(bulk.getAllocationIdsOfFulfilledRequests()).containsExactly(allocationId);
    }

    @Test
    void testRequestBulkCancel() {
        TestingPhysicalSlotRequestBulkChecker bulkChecker =
                new TestingPhysicalSlotRequestBulkChecker();
        AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

        // allocate 2 physical slots for 2 groups
        Map<ExecutionAttemptID, ExecutionSlotAssignment> assignments1 =
                context.allocateSlotsFor(EV1, EV3);
        fulfilOneOfTwoSlotRequestsAndGetPendingProfile(context, new AllocationID());
        PhysicalSlotRequestBulk bulk1 = bulkChecker.getBulk();
        Map<ExecutionAttemptID, ExecutionSlotAssignment> assignments2 =
                context.allocateSlotsFor(EV2);

        // cancelling of (EV1, EV3) releases assignments1 and only one physical slot for EV3
        // the second physical slot is held by sharing EV2 from the next bulk
        bulk1.cancel(new Throwable());

        // return completed logical slot to clear shared slot and release physical slot
        assertThat(assignments1).hasSize(2);
        CompletableFuture<LogicalSlot> ev1slot =
                getAssignmentByExecutionVertexId(assignments1, EV1).getLogicalSlotFuture();
        boolean ev1failed = ev1slot.isCompletedExceptionally();
        CompletableFuture<LogicalSlot> ev3slot =
                getAssignmentByExecutionVertexId(assignments1, EV3).getLogicalSlotFuture();
        boolean ev3failed = ev3slot.isCompletedExceptionally();
        LogicalSlot slot = ev1failed ? ev3slot.join() : ev1slot.join();
        releaseLogicalSlot(slot);

        // EV3 needs again a physical slot, therefore there are 3 requests overall
        context.allocateSlotsFor(EV1, EV3);

        assertThat(context.getSlotProvider().getRequests()).hasSize(3);
        // either EV1 or EV3 logical slot future is fulfilled before cancellation
        assertThat(ev1failed).isNotEqualTo(ev3failed);
        assertThat(assignments2).hasSize(1);
        assertThat(getAssignmentByExecutionVertexId(assignments2, EV2).getLogicalSlotFuture())
                .isNotCompletedExceptionally();
    }

    private static void releaseLogicalSlot(LogicalSlot slot) {
        slot.tryAssignPayload(new DummyPayload(CompletableFuture.completedFuture(null)));
        slot.releaseSlot(new Throwable());
    }

    @Test
    void testBulkClearIfPhysicalSlotRequestFails() {
        TestingPhysicalSlotRequestBulkChecker bulkChecker =
                new TestingPhysicalSlotRequestBulkChecker();
        AllocationContext context = createBulkCheckerContextWithEv12GroupAndEv3Group(bulkChecker);

        context.allocateSlotsFor(EV1, EV3);
        SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
        context.getSlotProvider()
                .getResultForRequestId(slotRequestId)
                .completeExceptionally(new Throwable());
        PhysicalSlotRequestBulk bulk = bulkChecker.getBulk();

        assertThat(bulk.getPendingRequests()).isEmpty();
    }

    @Test
    void failLogicalSlotsIfPhysicalSlotIsFailed() {
        final TestingPhysicalSlotRequestBulkChecker bulkChecker =
                new TestingPhysicalSlotRequestBulkChecker();
        AllocationContext context =
                AllocationContext.newBuilder()
                        .addGroup(EV1, EV2)
                        .withBulkChecker(bulkChecker)
                        .withPhysicalSlotProvider(
                                TestingPhysicalSlotProvider.createWithFailingPhysicalSlotCreation(
                                        new FlinkException("test failure")))
                        .build();

        final Map<ExecutionAttemptID, ExecutionSlotAssignment> allocatedSlots =
                context.allocateSlotsFor(EV1, EV2);

        for (ExecutionSlotAssignment allocatedSlot : allocatedSlots.values()) {
            assertThat(allocatedSlot.getLogicalSlotFuture()).isCompletedExceptionally();
        }

        assertThat(bulkChecker.getBulk().getPendingRequests()).isEmpty();

        final Set<SlotRequestId> requests = context.getSlotProvider().getRequests().keySet();
        assertThat(context.getSlotProvider().getCancellations().keySet()).isEqualTo(requests);
    }

    @Test
    void testSlotRequestProfileFromExecutionSlotSharingGroup() {
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        final AllocationContext context =
                AllocationContext.newBuilder()
                        .addGroupAndResource(resourceProfile1, EV1, EV3)
                        .addGroupAndResource(resourceProfile2, EV2, EV4)
                        .build();

        context.allocateSlotsFor(EV1, EV2);
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);

        assertThat(
                        context.getSlotProvider().getRequests().values().stream()
                                .map(PhysicalSlotRequest::getSlotProfile)
                                .map(SlotProfile::getPhysicalSlotResourceProfile)
                                .collect(Collectors.toList()))
                .containsExactlyInAnyOrder(resourceProfile1, resourceProfile2);
    }

    @Test
    void testSlotProviderBatchSlotRequestTimeoutCheckIsDisabled() {
        final AllocationContext context = AllocationContext.newBuilder().build();
        assertThat(context.getSlotProvider().isBatchSlotRequestTimeoutCheckEnabled()).isFalse();
    }

    private static List<ExecutionVertexID> getAssignIds(
            Collection<ExecutionSlotAssignment> assignments) {
        return assignments.stream()
                .map(ExecutionSlotAssignment::getExecutionAttemptId)
                .map(ExecutionAttemptID::getExecutionVertexId)
                .collect(Collectors.toList());
    }

    private static AllocationContext createBulkCheckerContextWithEv12GroupAndEv3Group(
            PhysicalSlotRequestBulkChecker bulkChecker) {
        return AllocationContext.newBuilder()
                .addGroup(EV1, EV2)
                .addGroup(EV3)
                .withBulkChecker(bulkChecker)
                .withPhysicalSlotProvider(
                        TestingPhysicalSlotProvider.createWithoutImmediatePhysicalSlotCreation())
                .build();
    }

    private static ResourceProfile fulfilOneOfTwoSlotRequestsAndGetPendingProfile(
            AllocationContext context, AllocationID allocationId) {
        Map<SlotRequestId, PhysicalSlotRequest> requests = context.getSlotProvider().getRequests();
        List<SlotRequestId> slotRequestIds = new ArrayList<>(requests.keySet());
        assertThat(slotRequestIds).hasSize(2);
        SlotRequestId slotRequestId1 = slotRequestIds.get(0);
        SlotRequestId slotRequestId2 = slotRequestIds.get(1);
        context.getSlotProvider()
                .getResultForRequestId(slotRequestId1)
                .complete(TestingPhysicalSlot.builder().withAllocationID(allocationId).build());
        return requests.get(slotRequestId2).getSlotProfile().getPhysicalSlotResourceProfile();
    }

    private static ExecutionSlotAssignment getAssignmentByExecutionVertexId(
            Map<ExecutionAttemptID, ExecutionSlotAssignment> assignments,
            ExecutionVertexID executionVertexId) {
        return assignments.entrySet().stream()
                .filter(entry -> entry.getKey().getExecutionVertexId().equals(executionVertexId))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList())
                .get(0);
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

        private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
                ExecutionVertexID... ids) {
            return allocator.allocateSlotsFor(
                    Arrays.stream(ids)
                            .map(
                                    executionVertexId ->
                                            createExecutionAttemptId(executionVertexId, 0))
                            .collect(Collectors.toList()));
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
            private final Map<ExecutionVertexID[], ResourceProfile> groups = new HashMap<>();
            private boolean slotWillBeOccupiedIndefinitely = false;
            private PhysicalSlotRequestBulkChecker bulkChecker =
                    new TestingPhysicalSlotRequestBulkChecker();

            private TestingPhysicalSlotProvider physicalSlotProvider =
                    TestingPhysicalSlotProvider.createWithInfiniteSlotCreation();

            private Builder addGroup(ExecutionVertexID... group) {
                groups.put(group, ResourceProfile.UNKNOWN);
                return this;
            }

            private Builder addGroupAndResource(
                    ResourceProfile resourceProfile, ExecutionVertexID... group) {
                groups.put(group, resourceProfile);
                return this;
            }

            private Builder setSlotWillBeOccupiedIndefinitely(
                    boolean slotWillBeOccupiedIndefinitely) {
                this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
                return this;
            }

            private Builder withBulkChecker(PhysicalSlotRequestBulkChecker bulkChecker) {
                this.bulkChecker = bulkChecker;
                return this;
            }

            private Builder withPhysicalSlotProvider(
                    TestingPhysicalSlotProvider physicalSlotProvider) {
                this.physicalSlotProvider = physicalSlotProvider;
                return this;
            }

            private AllocationContext build() {
                TestingSharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
                        new TestingSharedSlotProfileRetrieverFactory();
                TestingSlotSharingStrategy slotSharingStrategy =
                        TestingSlotSharingStrategy.createWithGroupsAndResources(groups);
                SlotSharingExecutionSlotAllocator allocator =
                        new SlotSharingExecutionSlotAllocator(
                                physicalSlotProvider,
                                slotWillBeOccupiedIndefinitely,
                                slotSharingStrategy,
                                sharedSlotProfileRetrieverFactory,
                                bulkChecker,
                                ALLOCATION_TIMEOUT,
                                executionVertexID -> RESOURCE_PROFILE);
                return new AllocationContext(
                        physicalSlotProvider,
                        slotSharingStrategy,
                        allocator,
                        sharedSlotProfileRetrieverFactory);
            }
        }
    }

    private static class TestingSlotSharingStrategy implements SlotSharingStrategy {
        private final Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups;

        private TestingSlotSharingStrategy(
                Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups) {
            this.executionSlotSharingGroups = executionSlotSharingGroups;
        }

        @Override
        public ExecutionSlotSharingGroup getExecutionSlotSharingGroup(
                ExecutionVertexID executionVertexId) {
            return executionSlotSharingGroups.get(executionVertexId);
        }

        @Override
        public Set<ExecutionSlotSharingGroup> getExecutionSlotSharingGroups() {
            return new HashSet<>(executionSlotSharingGroups.values());
        }

        private static TestingSlotSharingStrategy createWithGroupsAndResources(
                Map<ExecutionVertexID[], ResourceProfile> groupAndResources) {
            Map<ExecutionVertexID, ExecutionSlotSharingGroup> executionSlotSharingGroups =
                    new HashMap<>();
            for (Map.Entry<ExecutionVertexID[], ResourceProfile> groupAndResource :
                    groupAndResources.entrySet()) {
                ExecutionSlotSharingGroup executionSlotSharingGroup =
                        new ExecutionSlotSharingGroup();
                executionSlotSharingGroup.setResourceProfile(groupAndResource.getValue());
                for (ExecutionVertexID executionVertexId : groupAndResource.getKey()) {
                    executionSlotSharingGroup.addVertex(executionVertexId);
                    executionSlotSharingGroups.put(executionVertexId, executionSlotSharingGroup);
                }
            }
            return new TestingSlotSharingStrategy(executionSlotSharingGroups);
        }
    }

    private static class TestingSharedSlotProfileRetrieverFactory
            implements SharedSlotProfileRetrieverFactory {
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
                return SlotProfileTestingUtils.noLocality(resourceProfile);
            };
        }

        private List<Set<ExecutionVertexID>> getAskedBulks() {
            return Collections.unmodifiableList(askedBulks);
        }

        private List<ExecutionSlotSharingGroup> getAskedGroups() {
            return Collections.unmodifiableList(askedGroups);
        }
    }
}

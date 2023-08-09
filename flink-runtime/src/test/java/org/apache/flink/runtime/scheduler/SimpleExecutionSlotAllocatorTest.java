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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test suits for {@link SimpleExecutionSlotAllocator}. */
class SimpleExecutionSlotAllocatorTest {

    private static final ResourceProfile RESOURCE_PROFILE = ResourceProfile.fromResources(3, 5);
    private static final ExecutionAttemptID EXECUTION_ATTEMPT_ID = createExecutionAttemptId();

    @Test
    void testSlotAllocation() {
        final AllocationContext context = new AllocationContext();
        final CompletableFuture<LogicalSlot> slotFuture =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        assertThat(slotFuture).isCompleted();
        assertThat(context.getSlotProvider().getRequests()).hasSize(1);

        final PhysicalSlotRequest slotRequest =
                context.getSlotProvider().getRequests().values().iterator().next();
        assertThat(slotRequest.getSlotProfile().getPhysicalSlotResourceProfile())
                .isEqualTo(RESOURCE_PROFILE);
    }

    @Test
    void testDuplicateAllocationDoesNotRecreateLogicalSlotFuture() throws Exception {
        final AllocationContext context = new AllocationContext();

        final CompletableFuture<LogicalSlot> slotFuture1 =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        final CompletableFuture<LogicalSlot> slotFuture2 =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);

        assertThat(slotFuture1.get()).isSameAs(slotFuture2.get());
    }

    @Test
    void testFailedPhysicalSlotRequestFailsLogicalSlotFuture() {
        final AllocationContext context =
                new AllocationContext(
                        TestingPhysicalSlotProvider.createWithoutImmediatePhysicalSlotCreation(),
                        false);
        final CompletableFuture<LogicalSlot> slotFuture =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        final SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();

        assertThat(slotFuture).isNotDone();
        context.getSlotProvider()
                .getResponses()
                .get(slotRequestId)
                .completeExceptionally(new Throwable());
        assertThat(slotFuture).isCompletedExceptionally();

        // next allocation allocates new slot
        context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);
    }

    @Test
    void testSlotWillBeOccupiedIndefinitelyFalse() throws Exception {
        testSlotWillBeOccupiedIndefinitely(false);
    }

    @Test
    void testSlotWillBeOccupiedIndefinitelyTrue() throws Exception {
        testSlotWillBeOccupiedIndefinitely(true);
    }

    private static void testSlotWillBeOccupiedIndefinitely(boolean slotWillBeOccupiedIndefinitely)
            throws Exception {
        final AllocationContext context =
                new AllocationContext(
                        TestingPhysicalSlotProvider.createWithInfiniteSlotCreation(),
                        slotWillBeOccupiedIndefinitely);
        context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);

        final PhysicalSlotRequest slotRequest = context.getSlotProvider().getFirstRequestOrFail();
        assertThat(slotRequest.willSlotBeOccupiedIndefinitely())
                .isEqualTo(slotWillBeOccupiedIndefinitely);

        final TestingPhysicalSlot physicalSlot =
                context.getSlotProvider().getResponses().get(slotRequest.getSlotRequestId()).get();
        assertThat(physicalSlot.getPayload()).isNotNull();
        assertThat(physicalSlot.getPayload().willOccupySlotIndefinitely())
                .isEqualTo(slotWillBeOccupiedIndefinitely);
    }

    @Test
    void testLogicalSlotReleasingCancelsPhysicalSlotRequest() throws Exception {
        testLogicalSlotRequestCancellationOrRelease(
                true, true, (context, slotFuture) -> slotFuture.get().releaseSlot(null));
    }

    @Test
    void testLogicalSlotCancellationCancelsPhysicalSlotRequest() throws Exception {
        testLogicalSlotRequestCancellationOrRelease(
                false,
                true,
                (context, slotFuture) -> {
                    assertThatThrownBy(
                                    () -> {
                                        context.getAllocator().cancel(EXECUTION_ATTEMPT_ID);
                                        slotFuture.get();
                                    })
                            .as("The logical future must finish with a cancellation exception.")
                            .isInstanceOf(CancellationException.class);
                });
    }

    @Test
    void testCompletedLogicalSlotCancellationDoesNotCancelPhysicalSlotRequest() throws Exception {
        testLogicalSlotRequestCancellationOrRelease(
                true,
                false,
                (context, slotFuture) -> {
                    context.getAllocator().cancel(EXECUTION_ATTEMPT_ID);
                    slotFuture.get();
                });
    }

    private static void testLogicalSlotRequestCancellationOrRelease(
            final boolean autoCompletePhysicalSlotFuture,
            final boolean expectPhysicalSlotRequestCanceled,
            final BiConsumerWithException<
                            AllocationContext, CompletableFuture<LogicalSlot>, Exception>
                    cancelOrReleaseAction)
            throws Exception {

        final TestingPhysicalSlotProvider physicalSlotProvider;
        if (!autoCompletePhysicalSlotFuture) {
            physicalSlotProvider =
                    TestingPhysicalSlotProvider.createWithoutImmediatePhysicalSlotCreation();
        } else {
            physicalSlotProvider = TestingPhysicalSlotProvider.createWithInfiniteSlotCreation();
        }
        final AllocationContext context = new AllocationContext(physicalSlotProvider, false);

        final CompletableFuture<LogicalSlot> slotFuture1 =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);

        cancelOrReleaseAction.accept(context, slotFuture1);

        final SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
        assertThat(context.getSlotProvider().getCancellations().containsKey(slotRequestId))
                .isEqualTo(expectPhysicalSlotRequestCanceled);

        context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        final int expectedNumberOfRequests = expectPhysicalSlotRequestCanceled ? 2 : 1;
        assertThat(context.getSlotProvider().getRequests()).hasSize(expectedNumberOfRequests);
    }

    @Test
    void testPhysicalSlotReleasesLogicalSlots() throws Exception {
        final AllocationContext context = new AllocationContext();
        final CompletableFuture<LogicalSlot> slotFuture =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        final TestingPayload payload = new TestingPayload();
        slotFuture.thenAccept(logicalSlot -> logicalSlot.tryAssignPayload(payload));
        final SlotRequestId slotRequestId =
                context.getSlotProvider().getFirstRequestOrFail().getSlotRequestId();
        final TestingPhysicalSlot physicalSlot =
                context.getSlotProvider().getFirstResponseOrFail().get();

        assertThat(payload.getTerminalStateFuture()).isNotDone();
        assertThat(physicalSlot.getPayload()).isNotNull();

        physicalSlot.getPayload().release(new Throwable());
        assertThat(payload.getTerminalStateFuture()).isDone();
        assertThat(context.getSlotProvider().getCancellations()).containsKey(slotRequestId);

        context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        // there should be one more physical slot allocation, as the first allocation should be
        // removed after releasing all logical slots
        assertThat(context.getSlotProvider().getRequests()).hasSize(2);
    }

    @Test
    void testFailLogicalSlotIfPhysicalSlotIsFails() {
        final AllocationContext context =
                new AllocationContext(
                        TestingPhysicalSlotProvider.createWithFailingPhysicalSlotCreation(
                                new FlinkException("test failure")),
                        false);
        final CompletableFuture<LogicalSlot> slotFuture =
                context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);

        assertThat(slotFuture).isCompletedExceptionally();
        assertThat(context.getSlotProvider().getCancellations().keySet())
                .isEqualTo(context.getSlotProvider().getRequests().keySet());
    }

    @Test
    void testSlotProviderBatchSlotRequestTimeoutCheckIsEnabled() {
        final AllocationContext context = new AllocationContext();
        assertThat(context.getSlotProvider().isBatchSlotRequestTimeoutCheckEnabled()).isTrue();
    }

    @Test
    void testPreferredLocationsOfSlotProfile() {
        final AllocationContext context = new AllocationContext();
        List<TaskManagerLocation> taskManagerLocations =
                Collections.singletonList(
                        new TaskManagerLocation(
                                ResourceID.generate(), InetAddress.getLoopbackAddress(), 41));
        context.getLocations()
                .put(EXECUTION_ATTEMPT_ID.getExecutionVertexId(), taskManagerLocations);
        context.allocateSlotsFor(EXECUTION_ATTEMPT_ID);
        assertThat(context.getSlotProvider().getRequests()).hasSize(1);
        final PhysicalSlotRequest slotRequest =
                context.getSlotProvider().getRequests().values().iterator().next();

        assertThat(slotRequest.getSlotProfile().getPreferredLocations()).hasSize(1);
        assertThat(slotRequest.getSlotProfile().getPreferredLocations())
                .isEqualTo(taskManagerLocations);
    }

    private static class AllocationContext {
        private final TestingPhysicalSlotProvider slotProvider;
        private final boolean slotWillBeOccupiedIndefinitely;
        private final Map<ExecutionVertexID, Collection<TaskManagerLocation>> locations;
        private final SimpleExecutionSlotAllocator allocator;

        public AllocationContext() {
            this(TestingPhysicalSlotProvider.createWithInfiniteSlotCreation(), false);
        }

        public AllocationContext(
                TestingPhysicalSlotProvider slotProvider, boolean slotWillBeOccupiedIndefinitely) {
            this.slotProvider = slotProvider;
            this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
            this.locations = new HashMap<>();
            this.allocator =
                    new SimpleExecutionSlotAllocator(
                            slotProvider,
                            executionAttemptId -> RESOURCE_PROFILE,
                            (executionVertexId, producersToIgnore) ->
                                    locations.getOrDefault(
                                            executionVertexId, Collections.emptyList()),
                            slotWillBeOccupiedIndefinitely);
        }

        private CompletableFuture<LogicalSlot> allocateSlotsFor(
                ExecutionAttemptID executionAttemptId) {
            return allocator
                    .allocateSlotsFor(Collections.singletonList(executionAttemptId))
                    .get(executionAttemptId)
                    .getLogicalSlotFuture();
        }

        public TestingPhysicalSlotProvider getSlotProvider() {
            return slotProvider;
        }

        public boolean isSlotWillBeOccupiedIndefinitely() {
            return slotWillBeOccupiedIndefinitely;
        }

        public Map<ExecutionVertexID, Collection<TaskManagerLocation>> getLocations() {
            return locations;
        }

        public SimpleExecutionSlotAllocator getAllocator() {
            return allocator;
        }
    }
}

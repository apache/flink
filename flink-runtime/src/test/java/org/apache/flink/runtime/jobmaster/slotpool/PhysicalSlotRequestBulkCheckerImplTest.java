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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.SharingPhysicalSlotRequestBulk;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.createPhysicalSlot;
import static org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotTestUtils.occupyPhysicalSlot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PhysicalSlotRequestBulkCheckerImpl}. */
class PhysicalSlotRequestBulkCheckerImplTest {

    private static final Duration TIMEOUT = Duration.ofMillis(50L);

    private static ScheduledExecutorService singleThreadScheduledExecutorService;

    private static ComponentMainThreadExecutor mainThreadExecutor;

    private final ManualClock clock = new ManualClock();

    private PhysicalSlotRequestBulkCheckerImpl bulkChecker;

    private Set<PhysicalSlot> slots;

    private Supplier<Set<SlotInfo>> slotsRetriever;

    @BeforeAll
    private static void setupClass() {
        singleThreadScheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        singleThreadScheduledExecutorService);
    }

    @AfterAll
    private static void teardownClass() {
        if (singleThreadScheduledExecutorService != null) {
            singleThreadScheduledExecutorService.shutdownNow();
        }
    }

    @BeforeEach
    private void setup() {
        slots = new HashSet<>();
        slotsRetriever = () -> new HashSet<>(slots);
        bulkChecker = new PhysicalSlotRequestBulkCheckerImpl(slotsRetriever, clock);
        bulkChecker.start(mainThreadExecutor);
    }

    @Test
    void testPendingBulkIsNotCancelled() throws InterruptedException, ExecutionException {
        final CompletableFuture<ExecutionVertexID> cancellationFuture = new CompletableFuture<>();
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, new ExecutionVertexID(new JobVertexID(), 0));
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        checkNotCancelledAfter(cancellationFuture, 2 * TIMEOUT.toMillis());
    }

    @Test
    void testFulfilledBulkIsNotCancelled() throws InterruptedException, ExecutionException {
        final CompletableFuture<ExecutionVertexID> cancellationFuture = new CompletableFuture<>();
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, new ExecutionVertexID(new JobVertexID(), 0));
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        checkNotCancelledAfter(cancellationFuture, 2 * TIMEOUT.toMillis());
    }

    private static void checkNotCancelledAfter(CompletableFuture<?> cancellationFuture, long milli)
            throws ExecutionException, InterruptedException {
        mainThreadExecutor.schedule(() -> {}, milli, TimeUnit.MILLISECONDS).get();
        assertThatThrownBy(
                        () -> {
                            assertThatFuture(cancellationFuture).isNotDone();
                            cancellationFuture.get(milli, TimeUnit.MILLISECONDS);
                        })
                .withFailMessage("The future must not have been cancelled")
                .isInstanceOf(TimeoutException.class);
        assertThatFuture(cancellationFuture).isNotDone();
    }

    @Test
    void testUnfulfillableBulkIsCancelled() {
        final CompletableFuture<ExecutionVertexID> cancellationFuture = new CompletableFuture<>();
        final ExecutionVertexID executionVertexID = new ExecutionVertexID(new JobVertexID(), 0);
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, executionVertexID);
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        clock.advanceTime(TIMEOUT.toMillis() + 1L, TimeUnit.MILLISECONDS);
        assertThat(cancellationFuture.join()).isEqualTo(executionVertexID);
    }

    @Test
    void testBulkFulfilledOnCheck() {
        final ExecutionSlotSharingGroup executionSlotSharingGroup =
                new ExecutionSlotSharingGroup(new SlotSharingGroup());
        final SharingPhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(executionSlotSharingGroup);

        bulk.markFulfilled(executionSlotSharingGroup, new AllocationID());

        final PhysicalSlotRequestBulkWithTimestamp bulkWithTimestamp =
                new PhysicalSlotRequestBulkWithTimestamp(bulk);
        assertThat(checkBulkTimeout(bulkWithTimestamp))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.FULFILLED);
    }

    @Test
    void testBulkTimeoutOnCheck() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        clock.advanceTime(TIMEOUT.toMillis() + 1L, TimeUnit.MILLISECONDS);
        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.TIMEOUT);
    }

    @Test
    void testBulkPendingOnCheckIfFulfillable() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        final PhysicalSlot slot = addOneSlot();
        occupyPhysicalSlot(slot, false);

        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.PENDING);
    }

    @Test
    void testBulkPendingOnCheckIfUnfulfillableButNotTimedOut() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.PENDING);
    }

    @Test
    void testBulkFulfillable() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        addOneSlot();

        assertThat(isFulfillable(bulk)).isTrue();
    }

    @Test
    void testBulkUnfulfillableWithInsufficientSlots() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()),
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        addOneSlot();

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkUnfulfillableWithSlotAlreadyAssignedToBulk() {
        final ExecutionSlotSharingGroup executionSlotSharingGroup =
                new ExecutionSlotSharingGroup(new SlotSharingGroup());
        final SharingPhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(
                        executionSlotSharingGroup,
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        final PhysicalSlot slot = addOneSlot();

        bulk.markFulfilled(executionSlotSharingGroup, slot.getAllocationId());

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkUnfulfillableWithSlotOccupiedIndefinitely() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()),
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        final PhysicalSlot slot1 = addOneSlot();
        addOneSlot();

        occupyPhysicalSlot(slot1, true);

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkFulfillableWithSlotOccupiedTemporarily() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()),
                        new ExecutionSlotSharingGroup(new SlotSharingGroup()));

        final PhysicalSlot slot1 = addOneSlot();
        addOneSlot();

        occupyPhysicalSlot(slot1, false);

        assertThat(isFulfillable(bulk)).isTrue();
    }

    private PhysicalSlotRequestBulkWithTimestamp createPhysicalSlotRequestBulkWithTimestamp(
            ExecutionSlotSharingGroup... executionSlotSharingGroups) {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                new PhysicalSlotRequestBulkWithTimestamp(
                        createPhysicalSlotRequestBulk(executionSlotSharingGroups));
        bulk.markUnfulfillable(clock.relativeTimeMillis());
        return bulk;
    }

    private static SharingPhysicalSlotRequestBulk createPhysicalSlotRequestBulk(
            ExecutionSlotSharingGroup... executionSlotSharingGroups) {
        final TestingPhysicalSlotRequestBulkBuilder builder =
                TestingPhysicalSlotRequestBulkBuilder.newBuilder();
        for (ExecutionSlotSharingGroup executionSlotSharingGroup : executionSlotSharingGroups) {
            builder.addPendingRequest(executionSlotSharingGroup, ResourceProfile.UNKNOWN);
        }
        return builder.buildSharingPhysicalSlotRequestBulk();
    }

    private PhysicalSlotRequestBulk createPhysicalSlotRequestBulkWithCancellationFuture(
            CompletableFuture<ExecutionVertexID> cancellationFuture,
            ExecutionVertexID executionVertexID) {
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                new ExecutionSlotSharingGroup(new SlotSharingGroup());
        executionSlotSharingGroup.addVertex(executionVertexID);
        return TestingPhysicalSlotRequestBulkBuilder.newBuilder()
                .addPendingRequest(executionSlotSharingGroup, ResourceProfile.UNKNOWN)
                .setCanceller((id, t) -> cancellationFuture.complete(id))
                .buildSharingPhysicalSlotRequestBulk();
    }

    private PhysicalSlot addOneSlot() {
        final PhysicalSlot slot = createPhysicalSlot();
        CompletableFuture.runAsync(() -> slots.add(slot), mainThreadExecutor).join();
        return slot;
    }

    private PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult checkBulkTimeout(
            final PhysicalSlotRequestBulkWithTimestamp bulk) {
        return bulkChecker.checkPhysicalSlotRequestBulkTimeout(bulk, TIMEOUT);
    }

    private boolean isFulfillable(final PhysicalSlotRequestBulk bulk) {
        return PhysicalSlotRequestBulkCheckerImpl.isSlotRequestBulkFulfillable(
                bulk, slotsRetriever);
    }
}

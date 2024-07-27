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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    private static final Time TIMEOUT = Time.milliseconds(50L);

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
        final CompletableFuture<SlotRequestId> cancellationFuture = new CompletableFuture<>();
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, new SlotRequestId());
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        checkNotCancelledAfter(cancellationFuture, 2 * TIMEOUT.toMilliseconds());
    }

    @Test
    void testFulfilledBulkIsNotCancelled() throws InterruptedException, ExecutionException {
        final CompletableFuture<SlotRequestId> cancellationFuture = new CompletableFuture<>();
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, new SlotRequestId());
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        checkNotCancelledAfter(cancellationFuture, 2 * TIMEOUT.toMilliseconds());
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
        final CompletableFuture<SlotRequestId> cancellationFuture = new CompletableFuture<>();
        final SlotRequestId slotRequestId = new SlotRequestId();
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulkWithCancellationFuture(
                        cancellationFuture, slotRequestId);
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, TIMEOUT);
        clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
        assertThat(cancellationFuture.join()).isEqualTo(slotRequestId);
    }

    @Test
    void testBulkFulfilledOnCheck() {
        final SlotRequestId slotRequestId = new SlotRequestId();
        final PhysicalSlotRequestBulkImpl bulk = createPhysicalSlotRequestBulk(slotRequestId);

        bulk.markRequestFulfilled(slotRequestId, new AllocationID());

        final PhysicalSlotRequestBulkWithTimestamp bulkWithTimestamp =
                new PhysicalSlotRequestBulkWithTimestamp(bulk);
        assertThat(checkBulkTimeout(bulkWithTimestamp))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.FULFILLED);
    }

    @Test
    void testBulkTimeoutOnCheck() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(new SlotRequestId());

        clock.advanceTime(TIMEOUT.toMilliseconds() + 1L, TimeUnit.MILLISECONDS);
        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.TIMEOUT);
    }

    @Test
    void testBulkPendingOnCheckIfFulfillable() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(new SlotRequestId());

        final PhysicalSlot slot = addOneSlot();
        occupyPhysicalSlot(slot, false);

        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.PENDING);
    }

    @Test
    void testBulkPendingOnCheckIfUnfulfillableButNotTimedOut() {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                createPhysicalSlotRequestBulkWithTimestamp(new SlotRequestId());

        assertThat(checkBulkTimeout(bulk))
                .isEqualTo(PhysicalSlotRequestBulkCheckerImpl.TimeoutCheckResult.PENDING);
    }

    @Test
    void testBulkFulfillable() {
        final PhysicalSlotRequestBulk bulk = createPhysicalSlotRequestBulk(new SlotRequestId());

        addOneSlot();

        assertThat(isFulfillable(bulk)).isTrue();
    }

    @Test
    void testBulkUnfulfillableWithInsufficientSlots() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(new SlotRequestId(), new SlotRequestId());

        addOneSlot();

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkUnfulfillableWithSlotAlreadyAssignedToBulk() {
        final SlotRequestId slotRequestId = new SlotRequestId();
        final PhysicalSlotRequestBulkImpl bulk =
                createPhysicalSlotRequestBulk(slotRequestId, new SlotRequestId());

        final PhysicalSlot slot = addOneSlot();

        bulk.markRequestFulfilled(slotRequestId, slot.getAllocationId());

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkUnfulfillableWithSlotOccupiedIndefinitely() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(new SlotRequestId(), new SlotRequestId());

        final PhysicalSlot slot1 = addOneSlot();
        addOneSlot();

        occupyPhysicalSlot(slot1, true);

        assertThat(isFulfillable(bulk)).isFalse();
    }

    @Test
    void testBulkFulfillableWithSlotOccupiedTemporarily() {
        final PhysicalSlotRequestBulk bulk =
                createPhysicalSlotRequestBulk(new SlotRequestId(), new SlotRequestId());

        final PhysicalSlot slot1 = addOneSlot();
        addOneSlot();

        occupyPhysicalSlot(slot1, false);

        assertThat(isFulfillable(bulk)).isTrue();
    }

    private PhysicalSlotRequestBulkWithTimestamp createPhysicalSlotRequestBulkWithTimestamp(
            SlotRequestId... slotRequestIds) {
        final PhysicalSlotRequestBulkWithTimestamp bulk =
                new PhysicalSlotRequestBulkWithTimestamp(
                        createPhysicalSlotRequestBulk(slotRequestIds));
        bulk.markUnfulfillable(clock.relativeTimeMillis());
        return bulk;
    }

    private static PhysicalSlotRequestBulkImpl createPhysicalSlotRequestBulk(
            SlotRequestId... slotRequestIds) {
        final TestingPhysicalSlotRequestBulkBuilder builder =
                TestingPhysicalSlotRequestBulkBuilder.newBuilder();
        for (SlotRequestId slotRequestId : slotRequestIds) {
            builder.addPendingRequest(slotRequestId, ResourceProfile.UNKNOWN);
        }
        return builder.buildPhysicalSlotRequestBulkImpl();
    }

    private PhysicalSlotRequestBulk createPhysicalSlotRequestBulkWithCancellationFuture(
            CompletableFuture<SlotRequestId> cancellationFuture, SlotRequestId slotRequestId) {
        return TestingPhysicalSlotRequestBulkBuilder.newBuilder()
                .addPendingRequest(slotRequestId, ResourceProfile.UNKNOWN)
                .setCanceller((id, t) -> cancellationFuture.complete(id))
                .buildPhysicalSlotRequestBulkImpl();
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

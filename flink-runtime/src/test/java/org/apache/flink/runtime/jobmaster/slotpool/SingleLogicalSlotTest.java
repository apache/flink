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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link SingleLogicalSlot} class. */
class SingleLogicalSlotTest {

    @Test
    void testPayloadAssignment() {
        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot();
        final DummyPayload dummyPayload1 = new DummyPayload();
        final DummyPayload dummyPayload2 = new DummyPayload();

        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload1)).isTrue();
        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload2)).isFalse();

        assertThat(singleLogicalSlot.getPayload()).isSameAs(dummyPayload1);
    }

    private SingleLogicalSlot createSingleLogicalSlot() {
        return createSingleLogicalSlot(new DummySlotOwner());
    }

    private SingleLogicalSlot createSingleLogicalSlot(SlotOwner slotOwner) {
        return new SingleLogicalSlot(
                new SlotRequestId(), createSlotContext(), Locality.LOCAL, slotOwner);
    }

    private static SlotContext createSlotContext() {
        return new SimpleSlotContext(
                new AllocationID(),
                new LocalTaskManagerLocation(),
                0,
                new SimpleAckingTaskManagerGateway(),
                ResourceProfile.ANY);
    }

    @Test
    void testAlive() throws Exception {
        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot();
        final DummyPayload dummyPayload = new DummyPayload();

        assertThat(singleLogicalSlot.isAlive()).isTrue();
        ;

        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload)).isTrue();
        assertThat(singleLogicalSlot.isAlive()).isTrue();

        final CompletableFuture<?> releaseFuture =
                singleLogicalSlot.releaseSlot(new FlinkException("Test exception"));

        assertThat(singleLogicalSlot.isAlive()).isFalse();

        releaseFuture.get();

        assertThat(singleLogicalSlot.isAlive()).isFalse();
    }

    @Test
    void testPayloadAssignmentAfterRelease() {
        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot();
        final DummyPayload dummyPayload = new DummyPayload();

        singleLogicalSlot.releaseSlot(new FlinkException("Test exception"));

        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload)).isFalse();
    }

    /**
     * Tests that the {@link PhysicalSlot.Payload#release(Throwable)} does not wait for the payload
     * to reach a terminal state.
     */
    @Test
    void testAllocatedSlotRelease() {
        final CompletableFuture<LogicalSlot> returnSlotFuture = new CompletableFuture<>();
        final WaitingSlotOwner waitingSlotOwner =
                new WaitingSlotOwner(returnSlotFuture, new CompletableFuture<>());
        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot(waitingSlotOwner);

        final CompletableFuture<?> terminalStateFuture = new CompletableFuture<>();
        final CompletableFuture<?> failFuture = new CompletableFuture<>();
        final ManualTestingPayload dummyPayload =
                new ManualTestingPayload(failFuture, terminalStateFuture);

        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload)).isTrue();

        singleLogicalSlot.release(new FlinkException("Test exception"));

        assertThatFuture(failFuture).isDone();
        // we don't require the logical slot to return to the owner because
        // the release call should only come from the owner
        assertThatFuture(returnSlotFuture).isNotDone();
    }

    /** Tests that the slot release is only signaled after the owner has taken it back. */
    @Test
    void testSlotRelease() {
        final CompletableFuture<LogicalSlot> returnedSlotFuture = new CompletableFuture<>();
        final CompletableFuture<Boolean> returnSlotResponseFuture = new CompletableFuture<>();
        final WaitingSlotOwner waitingSlotOwner =
                new WaitingSlotOwner(returnedSlotFuture, returnSlotResponseFuture);
        final CompletableFuture<?> terminalStateFuture = new CompletableFuture<>();
        final CompletableFuture<?> failFuture = new CompletableFuture<>();
        final ManualTestingPayload dummyPayload =
                new ManualTestingPayload(failFuture, terminalStateFuture);

        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot(waitingSlotOwner);

        assertThat(singleLogicalSlot.tryAssignPayload(dummyPayload)).isTrue();

        final CompletableFuture<?> releaseFuture =
                singleLogicalSlot.releaseSlot(new FlinkException("Test exception"));

        assertThatFuture(releaseFuture).isNotDone();
        assertThatFuture(returnedSlotFuture).isNotDone();
        assertThatFuture(failFuture).isDone();

        terminalStateFuture.complete(null);

        assertThatFuture(returnedSlotFuture).isDone();

        returnSlotResponseFuture.complete(true);

        assertThatFuture(releaseFuture).isDone();
    }

    /**
     * Tests that concurrent release operations only trigger the failing of the payload and the
     * return of the slot once.
     */
    @Test
    void testConcurrentReleaseOperations() throws Exception {
        final CountingSlotOwner countingSlotOwner = new CountingSlotOwner();
        final CountingFailPayload countingFailPayload = new CountingFailPayload();
        final SingleLogicalSlot singleLogicalSlot = createSingleLogicalSlot(countingSlotOwner);

        singleLogicalSlot.tryAssignPayload(countingFailPayload);

        final ExecutorService executorService = Executors.newFixedThreadPool(4);

        try {
            final int numberConcurrentOperations = 10;
            final Collection<CompletableFuture<?>> releaseOperationFutures =
                    new ArrayList<>(numberConcurrentOperations);

            for (int i = 0; i < numberConcurrentOperations; i++) {
                final CompletableFuture<Void> releaseOperationFuture =
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        singleLogicalSlot
                                                .releaseSlot(new FlinkException("Test exception"))
                                                .get();
                                    } catch (InterruptedException | ExecutionException e) {
                                        ExceptionUtils.checkInterrupted(e);
                                        throw new CompletionException(e);
                                    }
                                });

                releaseOperationFutures.add(releaseOperationFuture);
            }

            final FutureUtils.ConjunctFuture<Void> releaseOperationsFuture =
                    FutureUtils.waitForAll(releaseOperationFutures);

            releaseOperationsFuture.get();

            assertThat(countingSlotOwner.getReleaseCount()).isOne();
            assertThat(countingFailPayload.getFailCount()).isOne();
        } finally {
            executorService.shutdownNow();
        }
    }

    private static final class CountingFailPayload implements LogicalSlot.Payload {

        private final AtomicInteger failCounter = new AtomicInteger(0);

        int getFailCount() {
            return failCounter.get();
        }

        @Override
        public void fail(Throwable cause) {
            failCounter.incrementAndGet();
        }

        @Override
        public CompletableFuture<?> getTerminalStateFuture() {
            return CompletableFuture.completedFuture(null);
        }
    }

    private static final class CountingSlotOwner implements SlotOwner {

        private final AtomicInteger counter;

        private CountingSlotOwner() {
            this.counter = new AtomicInteger(0);
        }

        public int getReleaseCount() {
            return counter.get();
        }

        @Override
        public void returnLogicalSlot(LogicalSlot logicalSlot) {
            counter.incrementAndGet();
        }
    }

    private static final class ManualTestingPayload implements LogicalSlot.Payload {

        private final CompletableFuture<?> failFuture;

        private final CompletableFuture<?> terminalStateFuture;

        private ManualTestingPayload(
                CompletableFuture<?> failFuture, CompletableFuture<?> terminalStateFuture) {
            this.failFuture = failFuture;
            this.terminalStateFuture = terminalStateFuture;
        }

        @Override
        public void fail(Throwable cause) {
            failFuture.completeExceptionally(cause);
        }

        @Override
        public CompletableFuture<?> getTerminalStateFuture() {
            return terminalStateFuture;
        }
    }

    private static final class WaitingSlotOwner implements SlotOwner {

        private final CompletableFuture<LogicalSlot> returnAllocatedSlotFuture;

        private final CompletableFuture<Boolean> returnAllocatedSlotResponse;

        private WaitingSlotOwner(
                CompletableFuture<LogicalSlot> returnAllocatedSlotFuture,
                CompletableFuture<Boolean> returnAllocatedSlotResponse) {
            this.returnAllocatedSlotFuture = Preconditions.checkNotNull(returnAllocatedSlotFuture);
            this.returnAllocatedSlotResponse =
                    Preconditions.checkNotNull(returnAllocatedSlotResponse);
        }

        @Override
        public void returnLogicalSlot(LogicalSlot logicalSlot) {
            returnAllocatedSlotFuture.complete(logicalSlot);
        }
    }
}

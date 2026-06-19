/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.TestingPhysicalSlotPayload;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link SharedSlot}. */
class SharedSlotTest {

    @Test
    void testConstructorAssignsPayload() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();

        new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        assertThat(physicalSlot.getPayload()).isNotNull();
    }

    @Test
    void testConstructorFailsIfSlotAlreadyHasAssignedPayload() {
        assertThatThrownBy(
                        () -> {
                            final TestingPhysicalSlot physicalSlot =
                                    TestingPhysicalSlot.builder().build();
                            physicalSlot.tryAssignPayload(new TestingPhysicalSlotPayload());

                            new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testAllocateLogicalSlot() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        assertThat(logicalSlot.getAllocationId()).isEqualTo(physicalSlot.getAllocationId());
        assertThat(logicalSlot.getLocality()).isEqualTo(Locality.UNKNOWN);
        assertThat(logicalSlot.getPayload()).isNull();
        assertThat(logicalSlot.getTaskManagerLocation())
                .isEqualTo(physicalSlot.getTaskManagerLocation());
        assertThat(logicalSlot.getTaskManagerGateway())
                .isEqualTo(physicalSlot.getTaskManagerGateway());
    }

    @Test
    void testAllocateLogicalSlotIssuesUniqueSlotRequestIds() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        final LogicalSlot logicalSlot1 = sharedSlot.allocateLogicalSlot();
        final LogicalSlot logicalSlot2 = sharedSlot.allocateLogicalSlot();

        assertThat(logicalSlot1.getSlotRequestId()).isNotEqualTo(logicalSlot2.getSlotRequestId());
    }

    @Test
    void testReturnLogicalSlotRejectsAliveSlots() {
        assertThatThrownBy(
                        () -> {
                            final TestingPhysicalSlot physicalSlot =
                                    TestingPhysicalSlot.builder().build();
                            final SharedSlot sharedSlot =
                                    new SharedSlot(
                                            new SlotRequestId(), physicalSlot, false, () -> {});
                            final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

                            sharedSlot.returnLogicalSlot(logicalSlot);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReturnLogicalSlotRejectsUnknownSlot() {
        assertThatThrownBy(
                        () -> {
                            final TestingPhysicalSlot physicalSlot =
                                    TestingPhysicalSlot.builder().build();
                            final SharedSlot sharedSlot =
                                    new SharedSlot(
                                            new SlotRequestId(), physicalSlot, false, () -> {});
                            final LogicalSlot logicalSlot =
                                    new TestingLogicalSlotBuilder().createTestingLogicalSlot();
                            logicalSlot.releaseSlot(new Exception("test"));

                            sharedSlot.returnLogicalSlot(logicalSlot);
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testReturnLogicalSlotTriggersExternalReleaseOnLastSlot() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final AtomicBoolean externalReleaseInitiated = new AtomicBoolean(false);
        final SharedSlot sharedSlot =
                new SharedSlot(
                        new SlotRequestId(),
                        physicalSlot,
                        false,
                        () -> externalReleaseInitiated.set(true));
        final LogicalSlot logicalSlot1 = sharedSlot.allocateLogicalSlot();
        final LogicalSlot logicalSlot2 = sharedSlot.allocateLogicalSlot();

        // this implicitly returns the slot
        logicalSlot1.releaseSlot(new Exception("test"));
        assertThat(externalReleaseInitiated).isFalse();

        logicalSlot2.releaseSlot(new Exception("test"));
        assertThat(externalReleaseInitiated).isTrue();
    }

    @Test
    void testReleaseDoesNotTriggersExternalRelease() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final AtomicBoolean externalReleaseInitiated = new AtomicBoolean(false);
        final SharedSlot sharedSlot =
                new SharedSlot(
                        new SlotRequestId(),
                        physicalSlot,
                        false,
                        () -> externalReleaseInitiated.set(true));

        sharedSlot.release(new Exception("test"));

        assertThat(externalReleaseInitiated).isFalse();
    }

    @Test
    void testReleaseAlsoReleasesLogicalSlots() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        sharedSlot.release(new Exception("test"));

        assertThat(logicalSlot.isAlive()).isFalse();
    }

    @Test
    void testReleaseForbidsSubsequentLogicalSlotAllocations() {
        assertThatThrownBy(
                        () -> {
                            final TestingPhysicalSlot physicalSlot =
                                    TestingPhysicalSlot.builder().build();
                            final SharedSlot sharedSlot =
                                    new SharedSlot(
                                            new SlotRequestId(), physicalSlot, false, () -> {});

                            sharedSlot.release(new Exception("test"));

                            sharedSlot.allocateLogicalSlot();
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCanReturnLogicalSlotDuringRelease() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
        final LogicalSlot logicalSlot1 = sharedSlot.allocateLogicalSlot();
        final LogicalSlot logicalSlot2 = sharedSlot.allocateLogicalSlot();

        // both slots try to release the other one, simulating that the failure of one execution due
        // to the release also fails others
        logicalSlot1.tryAssignPayload(
                new TestLogicalSlotPayload(
                        cause -> {
                            if (logicalSlot2.isAlive()) {
                                logicalSlot2.releaseSlot(cause);
                            }
                        }));
        logicalSlot2.tryAssignPayload(
                new TestLogicalSlotPayload(
                        cause -> {
                            if (logicalSlot1.isAlive()) {
                                logicalSlot1.releaseSlot(cause);
                            }
                        }));

        sharedSlot.release(new Exception("test"));

        // if all logical slots were released, and the sharedSlot no longer allows the allocation of
        // logical slots, then the slot release was completed
        assertThat(logicalSlot1.isAlive()).isFalse();
        assertThat(logicalSlot2.isAlive()).isFalse();
        assertThatThrownBy(sharedSlot::allocateLogicalSlot)
                .withFailMessage(
                        "Allocation of logical slot should have failed because the slot was released.")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testCannotAllocateLogicalSlotDuringRelease() {
        assertThatThrownBy(
                        () -> {
                            final TestingPhysicalSlot physicalSlot =
                                    TestingPhysicalSlot.builder().build();
                            final SharedSlot sharedSlot =
                                    new SharedSlot(
                                            new SlotRequestId(), physicalSlot, false, () -> {});

                            final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

                            logicalSlot.tryAssignPayload(
                                    new TestLogicalSlotPayload(
                                            ignored -> sharedSlot.allocateLogicalSlot()));

                            sharedSlot.release(new Exception("test"));
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    private static class TestLogicalSlotPayload implements LogicalSlot.Payload {

        private final Consumer<Throwable> failConsumer;

        public TestLogicalSlotPayload() {
            this.failConsumer = ignored -> {};
        }

        public TestLogicalSlotPayload(Consumer<Throwable> failConsumer) {
            this.failConsumer = failConsumer;
        }

        @Override
        public void fail(Throwable cause) {
            failConsumer.accept(cause);
        }

        @Override
        public CompletableFuture<?> getTerminalStateFuture() {
            return new CompletableFuture<>();
        }
    }
}

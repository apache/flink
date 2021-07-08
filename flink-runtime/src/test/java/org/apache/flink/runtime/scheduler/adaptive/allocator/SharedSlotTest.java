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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link SharedSlot}. */
public class SharedSlotTest extends TestLogger {

    @Test
    public void testConstructorAssignsPayload() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();

        new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        assertThat(physicalSlot.getPayload(), not(nullValue()));
    }

    @Test(expected = IllegalStateException.class)
    public void testConstructorFailsIfSlotAlreadyHasAssignedPayload() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        physicalSlot.tryAssignPayload(new TestPhysicalSlotPayload());

        new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
    }

    @Test
    public void testAllocateLogicalSlot() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        assertThat(logicalSlot.getAllocationId(), equalTo(physicalSlot.getAllocationId()));
        assertThat(logicalSlot.getLocality(), is(Locality.UNKNOWN));
        assertThat(logicalSlot.getPayload(), nullValue());
        assertThat(
                logicalSlot.getTaskManagerLocation(),
                equalTo(physicalSlot.getTaskManagerLocation()));
        assertThat(
                logicalSlot.getTaskManagerGateway(), equalTo(physicalSlot.getTaskManagerGateway()));
    }

    @Test
    public void testAllocateLogicalSlotIssuesUniqueSlotRequestIds() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        final LogicalSlot logicalSlot1 = sharedSlot.allocateLogicalSlot();
        final LogicalSlot logicalSlot2 = sharedSlot.allocateLogicalSlot();

        assertThat(logicalSlot1.getSlotRequestId(), not(equalTo(logicalSlot2.getSlotRequestId())));
    }

    @Test(expected = IllegalStateException.class)
    public void testReturnLogicalSlotRejectsAliveSlots() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        sharedSlot.returnLogicalSlot(logicalSlot);
    }

    @Test(expected = IllegalStateException.class)
    public void testReturnLogicalSlotRejectsUnknownSlot() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
        final LogicalSlot logicalSlot = new TestingLogicalSlotBuilder().createTestingLogicalSlot();
        logicalSlot.releaseSlot(new Exception("test"));

        sharedSlot.returnLogicalSlot(logicalSlot);
    }

    @Test
    public void testReturnLogicalSlotTriggersExternalReleaseOnLastSlot() {
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
        assertThat(externalReleaseInitiated.get(), is(false));

        logicalSlot2.releaseSlot(new Exception("test"));
        assertThat(externalReleaseInitiated.get(), is(true));
    }

    @Test
    public void testReleaseDoesNotTriggersExternalRelease() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final AtomicBoolean externalReleaseInitiated = new AtomicBoolean(false);
        final SharedSlot sharedSlot =
                new SharedSlot(
                        new SlotRequestId(),
                        physicalSlot,
                        false,
                        () -> externalReleaseInitiated.set(true));

        sharedSlot.release(new Exception("test"));

        assertThat(externalReleaseInitiated.get(), is(false));
    }

    @Test
    public void testReleaseAlsoReleasesLogicalSlots() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});
        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        sharedSlot.release(new Exception("test"));

        assertThat(logicalSlot.isAlive(), is(false));
    }

    @Test(expected = IllegalStateException.class)
    public void testReleaseForbidsSubsequentLogicalSlotAllocations() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        sharedSlot.release(new Exception("test"));

        sharedSlot.allocateLogicalSlot();
    }

    @Test
    public void testCanReturnLogicalSlotDuringRelease() {
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
        assertThat(logicalSlot1.isAlive(), is(false));
        assertThat(logicalSlot2.isAlive(), is(false));
        try {
            sharedSlot.allocateLogicalSlot();
            fail("Allocation of logical slot should have failed because the slot was released.");
        } catch (IllegalStateException expected) {
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testCannotAllocateLogicalSlotDuringRelease() {
        final TestingPhysicalSlot physicalSlot = TestingPhysicalSlot.builder().build();
        final SharedSlot sharedSlot =
                new SharedSlot(new SlotRequestId(), physicalSlot, false, () -> {});

        final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();

        logicalSlot.tryAssignPayload(
                new TestLogicalSlotPayload(ignored -> sharedSlot.allocateLogicalSlot()));

        sharedSlot.release(new Exception("test"));
    }

    private static class TestPhysicalSlotPayload implements PhysicalSlot.Payload {

        @Override
        public void release(Throwable cause) {}

        @Override
        public boolean willOccupySlotIndefinitely() {
            return false;
        }
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

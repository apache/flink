/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link DefaultAllocatedSlotPool}. */
public class DefaultAllocatedSlotPoolTest extends TestLogger {

    @Test
    public void testAddSlots() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final Collection<AllocatedSlot> slots = createAllocatedSlots();

        slotPool.addSlots(slots, 0);

        assertSlotPoolContainsSlots(slotPool, slots);
        assertSlotPoolContainsFreeSlots(slotPool, slots);
    }

    @Test
    public void testRemoveSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final Collection<AllocatedSlot> slots = createAllocatedSlots();

        slotPool.addSlots(slots, 0);

        final Iterator<AllocatedSlot> iterator = slots.iterator();
        final AllocatedSlot removedSlot = iterator.next();
        iterator.remove();

        slotPool.removeSlot(removedSlot.getAllocationId());

        assertSlotPoolContainsSlots(slotPool, slots);
    }

    @Test
    public void testRemoveSlots() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final ResourceID owner = ResourceID.generate();
        final Collection<AllocatedSlot> slots = createAllocatedSlotsWithOwner(owner);
        final AllocatedSlot otherSlot = createAllocatedSlot(ResourceID.generate());
        slots.add(otherSlot);

        slotPool.addSlots(slots, 0);

        slotPool.removeSlots(owner);

        assertSlotPoolContainsSlots(slotPool, Collections.singleton(otherSlot));
    }

    @Test
    public void testRemoveSlotsReturnValue() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final ResourceID owner = ResourceID.generate();
        final AllocatedSlot slot1 = createAllocatedSlot(owner);
        final AllocatedSlot slot2 = createAllocatedSlot(owner);

        slotPool.addSlots(Arrays.asList(slot1, slot2), 0);
        slotPool.reserveFreeSlot(slot1.getAllocationId());

        final AllocatedSlotPool.AllocatedSlotsAndReservationStatus
                allocatedSlotsAndReservationStatus = slotPool.removeSlots(owner);

        assertThat(allocatedSlotsAndReservationStatus.getAllocatedSlots(), hasItems(slot1, slot2));
        assertThat(allocatedSlotsAndReservationStatus.wasFree(slot1.getAllocationId()), is(false));
        assertThat(allocatedSlotsAndReservationStatus.wasFree(slot2.getAllocationId()), is(true));
    }

    @Test
    public void testContainsSlots() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final ResourceID owner = ResourceID.generate();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(owner);

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertTrue(slotPool.containsSlots(owner));
        assertFalse(slotPool.containsSlots(ResourceID.generate()));
    }

    @Test
    public void testContainsSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertTrue(slotPool.containsSlot(allocatedSlot.getAllocationId()));
        assertFalse(slotPool.containsSlot(new AllocationID()));
    }

    @Test
    public void testReserveFreeSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final Collection<AllocatedSlot> allSlots = createAllocatedSlots();
        final Collection<AllocatedSlot> freeSlots = new ArrayList<>(allSlots);
        final Iterator<AllocatedSlot> iterator = freeSlots.iterator();
        final AllocatedSlot allocatedSlot = iterator.next();
        iterator.remove();

        slotPool.addSlots(allSlots, 0);

        assertThat(
                slotPool.reserveFreeSlot(allocatedSlot.getAllocationId()),
                sameInstance(allocatedSlot));

        assertSlotPoolContainsFreeSlots(slotPool, freeSlots);
        assertSlotPoolContainsSlots(slotPool, allSlots);
    }

    @Test(expected = IllegalStateException.class)
    public void testReserveNonFreeSlotFails() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot slot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(slot), 0);

        slotPool.reserveFreeSlot(slot.getAllocationId());
        slotPool.reserveFreeSlot(slot.getAllocationId());
    }

    @Test
    public void testFreeingOfReservedSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final Collection<AllocatedSlot> slots = createAllocatedSlots();

        final int initialTime = 0;
        slotPool.addSlots(slots, initialTime);

        final AllocatedSlot slot = slots.iterator().next();

        slotPool.reserveFreeSlot(slot.getAllocationId());

        final int releaseTime = 1;
        assertTrue(slotPool.freeReservedSlot(slot.getAllocationId(), releaseTime).isPresent());
        assertSlotPoolContainsFreeSlots(slotPool, slots);

        for (AllocatedSlotPool.FreeSlotInfo freeSlotInfo : slotPool.getFreeSlotsInformation()) {
            final long time;
            if (freeSlotInfo.getAllocationId().equals(slot.getAllocationId())) {
                time = releaseTime;
            } else {
                time = initialTime;
            }

            assertThat(freeSlotInfo.getFreeSince(), is(time));
        }
    }

    @Test
    public void testFreeingOfFreeSlotIsIgnored() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot slot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(slot), 0);

        assertFalse(slotPool.freeReservedSlot(slot.getAllocationId(), 1).isPresent());

        final AllocatedSlotPool.FreeSlotInfo freeSlotInfo =
                Iterables.getOnlyElement(slotPool.getFreeSlotsInformation());

        assertThat(freeSlotInfo.getFreeSince(), is(0L));
    }

    @Test
    public void testSlotUtilizationCalculation() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final ResourceID owner = ResourceID.generate();
        final Collection<AllocatedSlot> slots = createAllocatedSlotsWithOwner(owner);

        slotPool.addSlots(slots, 0);

        for (AllocatedSlotPool.FreeSlotInfo freeSlotInfo : slotPool.getFreeSlotsInformation()) {
            assertThat(freeSlotInfo.asSlotInfo().getTaskExecutorUtilization(), closeTo(0, 0.1));
        }

        int numAllocatedSlots = 0;
        for (AllocatedSlot slot : slots) {
            assertThat(slotPool.reserveFreeSlot(slot.getAllocationId()), sameInstance(slot));
            numAllocatedSlots++;

            for (AllocatedSlotPool.FreeSlotInfo freeSlotInfo : slotPool.getFreeSlotsInformation()) {
                final double utilization = (double) numAllocatedSlots / slots.size();
                assertThat(
                        freeSlotInfo.asSlotInfo().getTaskExecutorUtilization(),
                        closeTo(utilization, 0.1));
            }
        }
    }

    @Test
    public void testRemoveSlotsOfUnknownOwnerIsIgnored() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        slotPool.removeSlots(ResourceID.generate());
    }

    @Test
    public void testContainsFreeSlotReturnsTrueIfSlotIsFree() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(ResourceID.generate());

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertTrue(slotPool.containsFreeSlot(allocatedSlot.getAllocationId()));
    }

    @Test
    public void testContainsFreeSlotReturnsFalseIfSlotDoesNotExist() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        assertFalse(slotPool.containsFreeSlot(new AllocationID()));
    }

    @Test
    public void testContainsFreeSlotReturnsFalseIfSlotIsReserved() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(ResourceID.generate());

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);
        slotPool.reserveFreeSlot(allocatedSlot.getAllocationId());

        assertFalse(slotPool.containsFreeSlot(allocatedSlot.getAllocationId()));
    }

    private void assertSlotPoolContainsSlots(
            DefaultAllocatedSlotPool slotPool, Collection<AllocatedSlot> slots) {
        assertThat(slotPool.getAllSlotsInformation(), hasSize(slots.size()));

        final Map<AllocationID, AllocatedSlot> slotsPerAllocationId =
                slots.stream()
                        .collect(
                                Collectors.toMap(
                                        AllocatedSlot::getAllocationId, Function.identity()));

        for (SlotInfo slotInfo : slotPool.getAllSlotsInformation()) {
            assertTrue(slotsPerAllocationId.containsKey(slotInfo.getAllocationId()));
            final AllocatedSlot allocatedSlot =
                    slotsPerAllocationId.get(slotInfo.getAllocationId());

            assertThat(slotInfo, matchesPhysicalSlot(allocatedSlot));
        }
    }

    private void assertSlotPoolContainsFreeSlots(
            DefaultAllocatedSlotPool slotPool, Collection<AllocatedSlot> allocatedSlots) {
        final Collection<AllocatedSlotPool.FreeSlotInfo> freeSlotsInformation =
                slotPool.getFreeSlotsInformation();

        assertThat(freeSlotsInformation, hasSize(allocatedSlots.size()));

        final Map<AllocationID, AllocatedSlot> allocatedSlotMap =
                allocatedSlots.stream()
                        .collect(
                                Collectors.toMap(
                                        AllocatedSlot::getAllocationId, Function.identity()));

        for (AllocatedSlotPool.FreeSlotInfo freeSlotInfo : freeSlotsInformation) {
            assertTrue(allocatedSlotMap.containsKey(freeSlotInfo.getAllocationId()));

            assertThat(
                    freeSlotInfo.asSlotInfo(),
                    matchesPhysicalSlot(allocatedSlotMap.get(freeSlotInfo.getAllocationId())));
        }
    }

    static Matcher<SlotInfo> matchesPhysicalSlot(PhysicalSlot allocatedSlot) {
        return new SlotInfoMatcher(allocatedSlot);
    }

    private Collection<AllocatedSlot> createAllocatedSlots() {
        return new ArrayList<>(
                Arrays.asList(
                        createAllocatedSlot(null),
                        createAllocatedSlot(null),
                        createAllocatedSlot(null)));
    }

    private Collection<AllocatedSlot> createAllocatedSlotsWithOwner(ResourceID owner) {
        return new ArrayList<>(
                Arrays.asList(
                        createAllocatedSlot(owner),
                        createAllocatedSlot(owner),
                        createAllocatedSlot(owner)));
    }

    @Nonnull
    private AllocatedSlot createAllocatedSlot(@Nullable ResourceID owner) {
        return new AllocatedSlot(
                new AllocationID(),
                owner == null
                        ? new LocalTaskManagerLocation()
                        : new TaskManagerLocation(owner, InetAddress.getLoopbackAddress(), 41),
                0,
                ResourceProfile.UNKNOWN,
                new RpcTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(),
                        JobMasterId.generate()));
    }

    static class SlotInfoMatcher extends TypeSafeMatcher<SlotInfo> {

        private final PhysicalSlot physicalSlot;

        SlotInfoMatcher(PhysicalSlot physicalSlot) {
            this.physicalSlot = physicalSlot;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("SlotInfo with values: ");
            description.appendValueList(
                    "{",
                    ",",
                    "}",
                    physicalSlot.getAllocationId(),
                    physicalSlot.getPhysicalSlotNumber(),
                    physicalSlot.getResourceProfile(),
                    physicalSlot.getTaskManagerLocation());
        }

        @Override
        protected boolean matchesSafely(SlotInfo item) {
            return item.getAllocationId().equals(physicalSlot.getAllocationId())
                    && item.getPhysicalSlotNumber() == physicalSlot.getPhysicalSlotNumber()
                    && item.getResourceProfile().equals(physicalSlot.getResourceProfile())
                    && item.getTaskManagerLocation().equals(physicalSlot.getTaskManagerLocation());
        }
    }
}

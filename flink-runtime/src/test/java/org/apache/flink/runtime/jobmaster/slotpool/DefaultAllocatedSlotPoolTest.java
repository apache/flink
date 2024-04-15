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

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.offset;

/** Tests for the {@link DefaultAllocatedSlotPool}. */
class DefaultAllocatedSlotPoolTest {

    @Test
    void testAddSlots() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final Collection<AllocatedSlot> slots = createAllocatedSlots();

        slotPool.addSlots(slots, 0);

        assertSlotPoolContainsSlots(slotPool, slots);
        assertSlotPoolContainsFreeSlots(slotPool, slots);
    }

    @Test
    void testRemoveSlot() {
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
    void testRemoveSlots() {
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
    void testRemoveSlotsReturnValue() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        final ResourceID owner = ResourceID.generate();
        final AllocatedSlot slot1 = createAllocatedSlot(owner);
        final AllocatedSlot slot2 = createAllocatedSlot(owner);

        slotPool.addSlots(Arrays.asList(slot1, slot2), 0);
        slotPool.reserveFreeSlot(slot1.getAllocationId());

        final AllocatedSlotPool.AllocatedSlotsAndReservationStatus
                allocatedSlotsAndReservationStatus = slotPool.removeSlots(owner);

        assertThat(allocatedSlotsAndReservationStatus.getAllocatedSlots())
                .containsExactlyInAnyOrder(slot1, slot2);
        assertThat(allocatedSlotsAndReservationStatus.wasFree(slot1.getAllocationId())).isFalse();
        assertThat(allocatedSlotsAndReservationStatus.wasFree(slot2.getAllocationId())).isTrue();
    }

    @Test
    void testContainsSlots() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final ResourceID owner = ResourceID.generate();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(owner);

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertThat(slotPool.containsSlots(owner)).isTrue();
        assertThat(slotPool.containsSlots(ResourceID.generate())).isFalse();
    }

    @Test
    void testContainsSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertThat(slotPool.containsSlot(allocatedSlot.getAllocationId())).isTrue();
        assertThat(slotPool.containsSlot(new AllocationID())).isFalse();
    }

    @Test
    void testReserveFreeSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final Collection<AllocatedSlot> allSlots = createAllocatedSlots();
        final Collection<AllocatedSlot> freeSlots = new ArrayList<>(allSlots);
        final Iterator<AllocatedSlot> iterator = freeSlots.iterator();
        final AllocatedSlot allocatedSlot = iterator.next();
        iterator.remove();

        slotPool.addSlots(allSlots, 0);

        assertThat(slotPool.reserveFreeSlot(allocatedSlot.getAllocationId()))
                .isEqualTo(allocatedSlot);

        assertSlotPoolContainsFreeSlots(slotPool, freeSlots);
        assertSlotPoolContainsSlots(slotPool, allSlots);
    }

    @Test
    void testReserveNonFreeSlotFails() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot slot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(slot), 0);

        slotPool.reserveFreeSlot(slot.getAllocationId());
        assertThatThrownBy(() -> slotPool.reserveFreeSlot(slot.getAllocationId()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testFreeingOfReservedSlot() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final Collection<AllocatedSlot> slots = createAllocatedSlots();

        final int initialTime = 0;
        slotPool.addSlots(slots, initialTime);

        final AllocatedSlot slot = slots.iterator().next();

        slotPool.reserveFreeSlot(slot.getAllocationId());

        final int releaseTime = 1;
        assertThat(slotPool.freeReservedSlot(slot.getAllocationId(), releaseTime)).isPresent();
        assertSlotPoolContainsFreeSlots(slotPool, slots);

        for (AllocatedSlotPool.FreeSlotInfo freeSlotInfo :
                slotPool.getFreeSlotInfoTracker().getFreeSlotsWithIdleSinceInformation()) {
            final long time;
            if (freeSlotInfo.getAllocationId().equals(slot.getAllocationId())) {
                time = releaseTime;
            } else {
                time = initialTime;
            }

            assertThat(freeSlotInfo.getFreeSince()).isEqualTo(time);
        }
    }

    @Test
    void testFreeingOfFreeSlotIsIgnored() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot slot = createAllocatedSlot(null);

        slotPool.addSlots(Collections.singleton(slot), 0);

        assertThat(slotPool.freeReservedSlot(slot.getAllocationId(), 1)).isNotPresent();

        final AllocatedSlotPool.FreeSlotInfo freeSlotInfo =
                Iterables.getOnlyElement(
                        slotPool.getFreeSlotInfoTracker().getFreeSlotsWithIdleSinceInformation());

        assertThat(freeSlotInfo.getFreeSince()).isEqualTo(0L);
    }

    @Test
    void testSlotUtilizationCalculation() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final ResourceID owner = ResourceID.generate();
        final Collection<AllocatedSlot> slots = createAllocatedSlotsWithOwner(owner);

        slotPool.addSlots(slots, 0);

        FreeSlotInfoTracker freeSlotInfoTracker = slotPool.getFreeSlotInfoTracker();

        assertThat(freeSlotInfoTracker.getAvailableSlots())
                .allSatisfy(
                        allocationId ->
                                assertThat(
                                                freeSlotInfoTracker.getTaskExecutorUtilization(
                                                        freeSlotInfoTracker.getSlotInfo(
                                                                allocationId)))
                                        .isCloseTo(0, offset(0.1)));

        int numAllocatedSlots = 0;
        for (AllocatedSlot slot : slots) {
            assertThat(slotPool.reserveFreeSlot(slot.getAllocationId())).isEqualTo(slot);
            freeSlotInfoTracker.reserveSlot(slot.getAllocationId());
            numAllocatedSlots++;
            final double utilization = (double) numAllocatedSlots / slots.size();

            assertThat(freeSlotInfoTracker.getAvailableSlots())
                    .allSatisfy(
                            allocationId ->
                                    assertThat(
                                                    freeSlotInfoTracker.getTaskExecutorUtilization(
                                                            freeSlotInfoTracker.getSlotInfo(
                                                                    allocationId)))
                                            .isCloseTo(utilization, offset(0.1)));
        }
    }

    @Test
    void testRemoveSlotsOfUnknownOwnerIsIgnored() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        slotPool.removeSlots(ResourceID.generate());
    }

    @Test
    void testContainsFreeSlotReturnsTrueIfSlotIsFree() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(ResourceID.generate());

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);

        assertThat(slotPool.containsFreeSlot(allocatedSlot.getAllocationId())).isTrue();
    }

    @Test
    void testContainsFreeSlotReturnsFalseIfSlotDoesNotExist() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();

        assertThat(slotPool.containsFreeSlot(new AllocationID())).isFalse();
    }

    @Test
    void testContainsFreeSlotReturnsFalseIfSlotIsReserved() {
        final DefaultAllocatedSlotPool slotPool = new DefaultAllocatedSlotPool();
        final AllocatedSlot allocatedSlot = createAllocatedSlot(ResourceID.generate());

        slotPool.addSlots(Collections.singleton(allocatedSlot), 0);
        slotPool.reserveFreeSlot(allocatedSlot.getAllocationId());

        assertThat(slotPool.containsFreeSlot(allocatedSlot.getAllocationId())).isFalse();
    }

    private void assertSlotPoolContainsSlots(
            DefaultAllocatedSlotPool slotPool, Collection<AllocatedSlot> slots) {
        assertThat(slotPool.getAllSlotsInformation()).hasSize(slots.size());

        final Map<AllocationID, AllocatedSlot> slotsPerAllocationId =
                slots.stream()
                        .collect(
                                Collectors.toMap(
                                        AllocatedSlot::getAllocationId, Function.identity()));

        assertThat(slotPool.getAllSlotsInformation())
                .allSatisfy(
                        slotInfo ->
                                assertThat(slotsPerAllocationId.get(slotInfo.getAllocationId()))
                                        .isEqualTo(slotInfo));
    }

    private void assertSlotPoolContainsFreeSlots(
            DefaultAllocatedSlotPool slotPool, Collection<AllocatedSlot> allocatedSlots) {
        final Collection<AllocatedSlotPool.FreeSlotInfo> freeSlotsInformation =
                slotPool.getFreeSlotInfoTracker().getFreeSlotsWithIdleSinceInformation();

        assertThat(freeSlotsInformation).hasSize(allocatedSlots.size());

        final Map<AllocationID, AllocatedSlot> allocatedSlotMap =
                allocatedSlots.stream()
                        .collect(
                                Collectors.toMap(
                                        AllocatedSlot::getAllocationId, Function.identity()));

        assertThat(freeSlotsInformation)
                .allSatisfy(
                        freeSlotInfo -> {
                            AllocatedSlot allocatedSlot =
                                    allocatedSlotMap.get(freeSlotInfo.getAllocationId());
                            assertThat(allocatedSlot).isNotNull();
                            SlotInfo slotInfo = freeSlotInfo.asSlotInfo();
                            assertThat(allocatedSlot.getAllocationId())
                                    .isEqualTo(slotInfo.getAllocationId());
                            assertThat(allocatedSlot.getPhysicalSlotNumber())
                                    .isEqualTo(slotInfo.getPhysicalSlotNumber());
                            assertThat(allocatedSlot.getResourceProfile())
                                    .isEqualTo(slotInfo.getResourceProfile());
                            assertThat(allocatedSlot.getTaskManagerLocation())
                                    .isEqualTo(slotInfo.getTaskManagerLocation());
                        });
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
}

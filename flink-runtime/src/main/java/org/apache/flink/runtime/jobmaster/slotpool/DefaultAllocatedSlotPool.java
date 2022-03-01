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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Default {@link AllocatedSlotPool} implementation. */
public class DefaultAllocatedSlotPool implements AllocatedSlotPool {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAllocatedSlotPool.class);

    private final Map<AllocationID, AllocatedSlot> registeredSlots;

    /** Map containing all free slots and since when they are free. */
    private final Map<AllocationID, Long> freeSlotsSince;

    /** Index containing a mapping between TaskExecutors and their slots. */
    private final Map<ResourceID, Set<AllocationID>> slotsPerTaskExecutor;

    public DefaultAllocatedSlotPool() {
        this.registeredSlots = new HashMap<>();
        this.slotsPerTaskExecutor = new HashMap<>();
        this.freeSlotsSince = new HashMap<>();
    }

    @Override
    public void addSlots(Collection<AllocatedSlot> slots, long currentTime) {
        for (AllocatedSlot slot : slots) {
            addSlot(slot, currentTime);
        }
    }

    private void addSlot(AllocatedSlot slot, long currentTime) {
        Preconditions.checkState(
                !registeredSlots.containsKey(slot.getAllocationId()),
                "The slot pool already contains a slot with id %s",
                slot.getAllocationId());
        addSlotInternal(slot, currentTime);

        slotsPerTaskExecutor
                .computeIfAbsent(slot.getTaskManagerId(), resourceID -> new HashSet<>())
                .add(slot.getAllocationId());
    }

    private void addSlotInternal(AllocatedSlot slot, long currentTime) {
        registeredSlots.put(slot.getAllocationId(), slot);
        freeSlotsSince.put(slot.getAllocationId(), currentTime);
    }

    @Override
    public Optional<AllocatedSlot> removeSlot(AllocationID allocationId) {
        final AllocatedSlot removedSlot = removeSlotInternal(allocationId);

        if (removedSlot != null) {
            final ResourceID owner = removedSlot.getTaskManagerId();

            slotsPerTaskExecutor.computeIfPresent(
                    owner,
                    (resourceID, allocationIds) -> {
                        allocationIds.remove(allocationId);

                        if (allocationIds.isEmpty()) {
                            return null;
                        }

                        return allocationIds;
                    });

            return Optional.of(removedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Nullable
    private AllocatedSlot removeSlotInternal(AllocationID allocationId) {
        final AllocatedSlot removedSlot = registeredSlots.remove(allocationId);
        freeSlotsSince.remove(allocationId);
        return removedSlot;
    }

    @Override
    public AllocatedSlotsAndReservationStatus removeSlots(ResourceID owner) {
        final Set<AllocationID> slotsOfTaskExecutor = slotsPerTaskExecutor.remove(owner);

        if (slotsOfTaskExecutor != null) {
            final Collection<AllocatedSlot> removedSlots = new ArrayList<>();
            final Map<AllocationID, ReservationStatus> removedSlotsReservationStatus =
                    new HashMap<>();

            for (AllocationID allocationId : slotsOfTaskExecutor) {
                final ReservationStatus reservationStatus =
                        containsFreeSlot(allocationId)
                                ? ReservationStatus.FREE
                                : ReservationStatus.RESERVED;

                final AllocatedSlot removedSlot =
                        Preconditions.checkNotNull(removeSlotInternal(allocationId));
                removedSlots.add(removedSlot);
                removedSlotsReservationStatus.put(removedSlot.getAllocationId(), reservationStatus);
            }

            return new DefaultAllocatedSlotsAndReservationStatus(
                    removedSlots, removedSlotsReservationStatus);
        } else {
            return new DefaultAllocatedSlotsAndReservationStatus(
                    Collections.emptyList(), Collections.emptyMap());
        }
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotsPerTaskExecutor.containsKey(owner);
    }

    @Override
    public boolean containsSlot(AllocationID allocationId) {
        return registeredSlots.containsKey(allocationId);
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return freeSlotsSince.containsKey(allocationId);
    }

    @Override
    public AllocatedSlot reserveFreeSlot(AllocationID allocationId) {
        LOG.debug("Reserve free slot with allocation id {}.", allocationId);
        Preconditions.checkState(
                freeSlotsSince.remove(allocationId) != null,
                "The slot with id %s was not free.",
                allocationId);
        return registeredSlots.get(allocationId);
    }

    @Override
    public Optional<AllocatedSlot> freeReservedSlot(AllocationID allocationId, long currentTime) {
        final AllocatedSlot allocatedSlot = registeredSlots.get(allocationId);

        if (allocatedSlot != null && !freeSlotsSince.containsKey(allocationId)) {
            freeSlotsSince.put(allocationId, currentTime);
            return Optional.of(allocatedSlot);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Collection<FreeSlotInfo> getFreeSlotsInformation() {
        final Map<ResourceID, Integer> freeSlotsPerTaskExecutor = new HashMap<>();

        for (AllocationID allocationId : freeSlotsSince.keySet()) {
            final ResourceID owner =
                    Preconditions.checkNotNull(registeredSlots.get(allocationId))
                            .getTaskManagerId();
            final int newCount = freeSlotsPerTaskExecutor.getOrDefault(owner, 0) + 1;
            freeSlotsPerTaskExecutor.put(owner, newCount);
        }

        final Collection<FreeSlotInfo> freeSlotInfos = new ArrayList<>();

        for (Map.Entry<AllocationID, Long> freeSlot : freeSlotsSince.entrySet()) {
            final AllocatedSlot allocatedSlot =
                    Preconditions.checkNotNull(registeredSlots.get(freeSlot.getKey()));

            final ResourceID owner = allocatedSlot.getTaskManagerId();
            final int numberOfSlotsOnOwner = slotsPerTaskExecutor.get(owner).size();
            final int numberOfFreeSlotsOnOwner = freeSlotsPerTaskExecutor.get(owner);
            final double taskExecutorUtilization =
                    (double) (numberOfSlotsOnOwner - numberOfFreeSlotsOnOwner)
                            / numberOfSlotsOnOwner;

            final SlotInfoWithUtilization slotInfoWithUtilization =
                    SlotInfoWithUtilization.from(allocatedSlot, taskExecutorUtilization);

            freeSlotInfos.add(
                    DefaultFreeSlotInfo.create(slotInfoWithUtilization, freeSlot.getValue()));
        }

        return freeSlotInfos;
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return registeredSlots.values();
    }

    private static final class DefaultFreeSlotInfo implements AllocatedSlotPool.FreeSlotInfo {

        private final SlotInfoWithUtilization slotInfoWithUtilization;

        private final long freeSince;

        private DefaultFreeSlotInfo(
                SlotInfoWithUtilization slotInfoWithUtilization, long freeSince) {
            this.slotInfoWithUtilization = slotInfoWithUtilization;
            this.freeSince = freeSince;
        }

        @Override
        public SlotInfoWithUtilization asSlotInfo() {
            return slotInfoWithUtilization;
        }

        @Override
        public long getFreeSince() {
            return freeSince;
        }

        private static DefaultFreeSlotInfo create(
                SlotInfoWithUtilization slotInfoWithUtilization, long idleSince) {
            return new DefaultFreeSlotInfo(
                    Preconditions.checkNotNull(slotInfoWithUtilization), idleSince);
        }
    }

    private static final class DefaultAllocatedSlotsAndReservationStatus
            implements AllocatedSlotsAndReservationStatus {

        private final Collection<AllocatedSlot> slots;
        private final Map<AllocationID, ReservationStatus> reservationStatus;

        private DefaultAllocatedSlotsAndReservationStatus(
                Collection<AllocatedSlot> slots,
                Map<AllocationID, ReservationStatus> reservationStatus) {
            this.slots = slots;
            this.reservationStatus = reservationStatus;
        }

        @Override
        public boolean wasFree(AllocationID allocatedSlot) {
            return reservationStatus.get(allocatedSlot) == ReservationStatus.FREE;
        }

        @Override
        public Collection<AllocatedSlot> getAllocatedSlots() {
            return slots;
        }
    }

    private enum ReservationStatus {
        FREE,
        RESERVED
    }
}

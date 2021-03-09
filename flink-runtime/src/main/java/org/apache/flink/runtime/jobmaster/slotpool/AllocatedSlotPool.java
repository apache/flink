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

import java.util.Collection;
import java.util.Optional;

/** The slot pool is responsible for maintaining a set of {@link AllocatedSlot AllocatedSlots}. */
public interface AllocatedSlotPool {

    /**
     * Adds the given collection of slots to the slot pool.
     *
     * @param slots slots to add to the slot pool
     * @param currentTime currentTime when the slots have been added to the slot pool
     * @throws IllegalStateException if the slot pool already contains a to be added slot
     */
    void addSlots(Collection<AllocatedSlot> slots, long currentTime);

    /**
     * Removes the slot with the given allocationId from the slot pool.
     *
     * @param allocationId allocationId identifying the slot to remove from the slot pool
     * @return the removed slot if there was a slot with the given allocationId; otherwise {@link
     *     Optional#empty()}
     */
    Optional<AllocatedSlot> removeSlot(AllocationID allocationId);

    /**
     * Removes all slots belonging to the owning TaskExecutor identified by owner.
     *
     * @param owner owner identifies the TaskExecutor whose slots shall be removed
     * @return the collection of removed slots
     */
    Collection<AllocatedSlot> removeSlots(ResourceID owner);

    /**
     * Checks whether the slot pool contains at least one slot belonging to the specified owner.
     *
     * @param owner owner for which to check whether the slot pool contains slots
     * @return {@code true} if the slot pool contains a slot from the given owner; otherwise {@code
     *     false}
     */
    boolean containsSlots(ResourceID owner);

    /**
     * Checks whether the slot pool contains a slot with the given allocationId.
     *
     * @param allocationId allocationId identifying the slot for which to check whether it is
     *     contained
     * @return {@code true} if the slot pool contains the slot with the given allocationId;
     *     otherwise {@code false}
     */
    boolean containsSlot(AllocationID allocationId);

    /**
     * Reserves the free slot specified by the given allocationId.
     *
     * @param allocationId allocationId identifying the free slot to reserve
     * @return the {@link AllocatedSlot} which has been reserved
     * @throws IllegalStateException if there is no free slot with the given allocationId
     */
    AllocatedSlot reserveFreeSlot(AllocationID allocationId);

    /**
     * Frees the reserved slot, adding it back into the set of free slots.
     *
     * @param allocationId identifying the reserved slot to freed
     * @param currentTime currentTime when the slot has been freed
     * @return the freed {@link AllocatedSlot} if there was an allocated with the given
     *     allocationId; otherwise {@link Optional#empty()}.
     */
    Optional<AllocatedSlot> freeReservedSlot(AllocationID allocationId, long currentTime);

    /**
     * Returns information about all currently free slots.
     *
     * @return collection of free slot information
     */
    Collection<FreeSlotInfo> getFreeSlotsInformation();

    /**
     * Returns information about all slots in this pool.
     *
     * @return collection of all slot information
     */
    Collection<? extends SlotInfo> getAllSlotsInformation();

    /** Information about a free slot. */
    interface FreeSlotInfo {
        SlotInfoWithUtilization asSlotInfo();

        /**
         * Returns since when this slot is free.
         *
         * @return the time since when the slot is free
         */
        long getFreeSince();

        default AllocationID getAllocationId() {
            return asSlotInfo().getAllocationId();
        }
    }
}

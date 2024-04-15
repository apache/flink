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
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.Collection;
import java.util.Set;

/** Track all free slots, support bookkeeping slot for {@link SlotSelectionStrategy}. */
public interface FreeSlotInfoTracker {

    /**
     * Get allocation id of all available slots.
     *
     * @return allocation id of available slots
     */
    Set<AllocationID> getAvailableSlots();

    /**
     * Get slot info by allocation id, this slot must exist.
     *
     * @param allocationId to get SlotInfo
     * @return slot info for the allocation id
     */
    SlotInfo getSlotInfo(AllocationID allocationId);

    /**
     * Returns a list of {@link AllocatedSlotPool.FreeSlotInfo} objects about all slots with slot
     * idle since that are currently available in the slot pool.
     *
     * @return a list of {@link AllocatedSlotPool.FreeSlotInfo} objects about all slots with slot
     *     idle since that are currently available in the slot pool.
     */
    Collection<AllocatedSlotPool.FreeSlotInfo> getFreeSlotsWithIdleSinceInformation();

    /**
     * Returns a list of {@link SlotInfo} objects about all slots that are currently available in
     * the slot pool.
     *
     * @return a list of {@link SlotInfo} objects about all slots that are currently available in
     *     the slot pool.
     */
    Collection<SlotInfo> getFreeSlotsInformation();

    /**
     * Get task executor utilization of this slot.
     *
     * @param slotInfo to get task executor utilization
     * @return task executor utilization of this slot
     */
    double getTaskExecutorUtilization(SlotInfo slotInfo);

    /**
     * Reserve free slot when it is used.
     *
     * @param allocationId to reserve
     */
    void reserveSlot(AllocationID allocationId);

    /**
     * Create a new free slot tracker without blocked slots.
     *
     * @param blockedSlots slots that should not be used
     * @return the new free slot tracker
     */
    FreeSlotInfoTracker createNewFreeSlotInfoTrackerWithoutBlockedSlots(
            Set<AllocationID> blockedSlots);
}

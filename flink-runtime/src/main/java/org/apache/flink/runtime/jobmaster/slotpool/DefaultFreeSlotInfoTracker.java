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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Default implements of {@link FreeSlotInfoTracker}. */
public class DefaultFreeSlotInfoTracker implements FreeSlotInfoTracker {
    private final Set<AllocationID> freeSlots;
    private final Function<AllocationID, SlotInfo> slotInfoLookup;
    private final Function<AllocationID, AllocatedSlotPool.FreeSlotInfo> freeSlotInfoLookup;
    private final Function<ResourceID, Double> taskExecutorUtilizationLookup;

    public DefaultFreeSlotInfoTracker(
            Set<AllocationID> freeSlots,
            Function<AllocationID, SlotInfo> slotInfoLookup,
            Function<AllocationID, AllocatedSlotPool.FreeSlotInfo> freeSlotInfoLookup,
            Function<ResourceID, Double> taskExecutorUtilizationLookup) {
        this.freeSlots = new HashSet<>(freeSlots);
        this.slotInfoLookup = slotInfoLookup;
        this.freeSlotInfoLookup = freeSlotInfoLookup;
        this.taskExecutorUtilizationLookup = taskExecutorUtilizationLookup;
    }

    @Override
    public Set<AllocationID> getAvailableSlots() {
        return Collections.unmodifiableSet(freeSlots);
    }

    @Override
    public SlotInfo getSlotInfo(AllocationID allocationId) {
        return Preconditions.checkNotNull(slotInfoLookup.apply(allocationId));
    }

    @Override
    public Collection<AllocatedSlotPool.FreeSlotInfo> getFreeSlotsWithIdleSinceInformation() {
        return freeSlots.stream().map(freeSlotInfoLookup).collect(Collectors.toList());
    }

    @Override
    public Collection<SlotInfo> getFreeSlotsInformation() {
        return freeSlots.stream().map(slotInfoLookup).collect(Collectors.toList());
    }

    @Override
    public double getTaskExecutorUtilization(SlotInfo slotInfo) {
        ResourceID resourceId = slotInfo.getTaskManagerLocation().getResourceID();
        return taskExecutorUtilizationLookup.apply(resourceId);
    }

    @Override
    public void reserveSlot(AllocationID allocationId) {
        Preconditions.checkState(
                freeSlots.remove(allocationId),
                "Slot %s does not exist in free slots",
                allocationId);
    }

    @Override
    public DefaultFreeSlotInfoTracker createNewFreeSlotInfoTrackerWithoutBlockedSlots(
            Set<AllocationID> blockedSlots) {

        Set<AllocationID> freeSlotInfoTrackerWithoutBlockedSlots =
                freeSlots.stream()
                        .filter(slot -> !blockedSlots.contains(slot))
                        .collect(Collectors.toSet());

        return new DefaultFreeSlotInfoTracker(
                freeSlotInfoTrackerWithoutBlockedSlots,
                slotInfoLookup,
                freeSlotInfoLookup,
                taskExecutorUtilizationLookup);
    }
}

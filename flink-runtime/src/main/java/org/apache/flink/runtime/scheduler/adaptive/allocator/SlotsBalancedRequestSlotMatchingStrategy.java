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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** The slots balanced request slot matching strategy implementation. */
public enum SlotsBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots) {

        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
                getSlotsPerTaskExecutor(freeSlots);
        final Map<ResourceID, SlotsUtilization> taskExecutorSlotsUtilizations =
                getSlotsUtilizationView(slotsPerTaskExecutor);
        final Map<SlotsUtilization, Set<PhysicalSlot>> utilizationSlotsMap =
                getUtilizationSlotsMap(freeSlots, taskExecutorSlotsUtilizations);

        SlotTaskExecutorWeight<SlotsUtilization> best;
        for (ExecutionSlotSharingGroup requestGroup : requestGroups) {
            best = getTheBestSlotUtilization(utilizationSlotsMap);
            slotAssignments.add(new SlotAssignment(best.physicalSlot, requestGroup));

            // Update the references
            final SlotsUtilization newSlotsUtilization = best.taskExecutorWeight.incReserved(1);
            taskExecutorSlotsUtilizations.put(best.getResourceID(), newSlotsUtilization);
            updateSlotsPerTaskExecutor(slotsPerTaskExecutor, best);
            Set<PhysicalSlot> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
            updateUtilizationSlotsMap(utilizationSlotsMap, best, slotInfos, newSlotsUtilization);
        }
        return slotAssignments;
    }

    private Map<ResourceID, SlotsUtilization> getSlotsUtilizationView(
            Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor) {
        return slotsPerTaskExecutor.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> new SlotsUtilization(entry.getValue().size(), 0)));
    }

    private static void updateUtilizationSlotsMap(
            Map<SlotsUtilization, Set<PhysicalSlot>> utilizationSlotsMap,
            SlotTaskExecutorWeight<SlotsUtilization> best,
            Set<PhysicalSlot> slotsToAdjust,
            SlotsUtilization newSlotsUtilization) {
        Set<PhysicalSlot> physicalSlots = utilizationSlotsMap.get(best.taskExecutorWeight);
        if (Objects.nonNull(physicalSlots)) {
            physicalSlots.remove(best.physicalSlot);
            if (Objects.nonNull(slotsToAdjust)) {
                physicalSlots.removeAll(slotsToAdjust);
            }
        }
        if (Objects.isNull(physicalSlots) || physicalSlots.isEmpty()) {
            utilizationSlotsMap.remove(best.taskExecutorWeight);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            utilizationSlotsMap
                    .computeIfAbsent(newSlotsUtilization, slotsUtilization -> new HashSet<>())
                    .addAll(slotsToAdjust);
        }
    }

    private static void updateSlotsPerTaskExecutor(
            Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor,
            SlotTaskExecutorWeight<SlotsUtilization> best) {
        Set<PhysicalSlot> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
        if (Objects.nonNull(slotInfos)) {
            slotInfos.remove(best.physicalSlot);
        }
        if (Objects.isNull(slotInfos) || slotInfos.isEmpty()) {
            slotsPerTaskExecutor.remove(best.getResourceID());
        }
    }

    private static Map<SlotsUtilization, Set<PhysicalSlot>> getUtilizationSlotsMap(
            Collection<PhysicalSlot> slots, Map<ResourceID, SlotsUtilization> slotsUtilizations) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                physicalSlot ->
                                        slotsUtilizations.get(
                                                physicalSlot
                                                        .getTaskManagerLocation()
                                                        .getResourceID()),
                                TreeMap::new,
                                Collectors.toSet()));
    }

    private static SlotTaskExecutorWeight<SlotsUtilization> getTheBestSlotUtilization(
            Map<SlotsUtilization, Set<PhysicalSlot>> slotsByUtilization) {
        final SlotsUtilization slotsUtilization =
                slotsByUtilization.keySet().stream()
                        .filter(su -> !su.isFullUtilization())
                        .min(SlotsUtilization::compareTo)
                        .orElseThrow(NO_SLOTS_EXCEPTION_GETTER);
        final PhysicalSlot targetSlot =
                slotsByUtilization.get(slotsUtilization).stream()
                        .findAny()
                        .orElseThrow(NO_SLOTS_EXCEPTION_GETTER);
        return new SlotTaskExecutorWeight<>(slotsUtilization, targetSlot);
    }

    static Map<ResourceID, Set<PhysicalSlot>> getSlotsPerTaskExecutor(
            Collection<PhysicalSlot> freeSlots) {
        return freeSlots.stream()
                .collect(
                        Collectors.groupingBy(
                                slot -> slot.getTaskManagerLocation().getResourceID(),
                                Collectors.toSet()));
    }

    /**
     * Helper class to represent the slot and the loading or slots utilization weight info of the
     * task executor where the slot is located at.
     */
    static class SlotTaskExecutorWeight<T> {
        final @Nonnull T taskExecutorWeight;
        final @Nonnull PhysicalSlot physicalSlot;

        SlotTaskExecutorWeight(@Nonnull T taskExecutorWeight, @Nonnull PhysicalSlot physicalSlot) {
            this.taskExecutorWeight = taskExecutorWeight;
            this.physicalSlot = physicalSlot;
        }

        ResourceID getResourceID() {
            return physicalSlot.getTaskManagerLocation().getResourceID();
        }
    }

    /** Help class to represent the slots utilization info. */
    static class SlotsUtilization implements Comparable<SlotsUtilization> {
        private final int total;
        private final int reserved;

        public SlotsUtilization(int total, int reserved) {
            Preconditions.checkArgument(total > 0);
            Preconditions.checkArgument(reserved >= 0);
            Preconditions.checkArgument(total >= reserved);
            this.total = total;
            this.reserved = reserved;
        }

        public SlotsUtilization incReserved(int inc) {
            Preconditions.checkArgument(inc > 0);
            Preconditions.checkArgument(reserved + inc <= total);
            return new SlotsUtilization(total, reserved + inc);
        }

        public double getUtilization() {
            return ((double) reserved) / total;
        }

        public boolean isFullUtilization() {
            return getUtilization() == 1d;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotsUtilization that = (SlotsUtilization) o;
            return total == that.total && reserved == that.reserved;
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, reserved);
        }

        @Override
        public int compareTo(@Nonnull SlotsUtilization o) {
            return Double.compare(getUtilization(), o.getUtilization());
        }
    }
}

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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

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

import static org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation.SlotsUtilization;

/** The slots balanced request slot matching strategy implementation. */
@Internal
public enum SlotsBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation) {

        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        final Map<ResourceID, SlotsUtilization> taskExecutorSlotsUtilizations =
                taskExecutorsLoadInformation.getTaskExecutorsSlotsUtilization();
        final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
                getSlotsPerTaskExecutor(freeSlots);
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
}

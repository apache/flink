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
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** The slots balanced request slot matching resolver implementation. */
public enum SlotsBalancedSlotMatchingResolver implements SlotMatchingResolver {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchSlotSharingGroupWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots) {

        final List<SlotAssignment> slotAssignments = new ArrayList<>(requestGroups.size());
        final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
                AllocatorUtil.getSlotsPerTaskExecutor(freeSlots);
        final Map<ResourceID, SlotsUtilization> taskExecutorSlotsUtilizations =
                getSlotsUtilizationView(slotsPerTaskExecutor);
        final TreeMap<Double, Set<PhysicalSlot>> utilizationSlotsMap =
                getUtilizationSlotsMap(freeSlots, taskExecutorSlotsUtilizations);

        SlotTaskExecutorWeight<SlotsUtilization> best;
        for (ExecutionSlotSharingGroup requestGroup : requestGroups) {
            best = getTheBestSlotUtilization(utilizationSlotsMap, taskExecutorSlotsUtilizations);
            ResourceID resourceID = best.getResourceID();
            slotAssignments.add(new SlotAssignment(best.physicalSlot, requestGroup));
            SlotsUtilization oldSlotsUtilization = taskExecutorSlotsUtilizations.get(resourceID);
            // Update the references
            final SlotsUtilization newSlotsUtilization = oldSlotsUtilization.incReserved(1);
            taskExecutorSlotsUtilizations.put(resourceID, newSlotsUtilization);
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
            Map<Double, Set<PhysicalSlot>> utilizationSlotsMap,
            SlotTaskExecutorWeight<SlotsUtilization> best,
            Set<PhysicalSlot> slotsToAdjust,
            SlotsUtilization newSlotsUtilization) {
        Double oldUtilization = best.taskExecutorWeight.getUtilization();
        Double newUtilization = newSlotsUtilization.getUtilization();

        Set<PhysicalSlot> physicalSlots = utilizationSlotsMap.get(oldUtilization);

        if (Objects.nonNull(physicalSlots)) {
            physicalSlots.remove(best.physicalSlot);
            if (Objects.nonNull(slotsToAdjust)) {
                physicalSlots.removeAll(slotsToAdjust);
            }
        }
        if (CollectionUtil.isNullOrEmpty(physicalSlots)) {
            utilizationSlotsMap.remove(oldUtilization);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            utilizationSlotsMap
                    .computeIfAbsent(newUtilization, slotsUtilization -> new HashSet<>())
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

    private static TreeMap<Double, Set<PhysicalSlot>> getUtilizationSlotsMap(
            Collection<PhysicalSlot> slots, Map<ResourceID, SlotsUtilization> slotsUtilizations) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                physicalSlot ->
                                        slotsUtilizations
                                                .get(
                                                        physicalSlot
                                                                .getTaskManagerLocation()
                                                                .getResourceID())
                                                .getUtilization(),
                                TreeMap::new,
                                Collectors.toSet()));
    }

    private static SlotTaskExecutorWeight<SlotsUtilization> getTheBestSlotUtilization(
            TreeMap<Double, Set<PhysicalSlot>> slotsByUtilization,
            Map<ResourceID, SlotsUtilization> taskExecutorSlotsUtilizations) {
        Map.Entry<Double, Set<PhysicalSlot>> firstEntry = slotsByUtilization.firstEntry();
        if (firstEntry == null
                || firstEntry.getKey() == null
                || CollectionUtil.isNullOrEmpty(firstEntry.getValue())) {
            throw NO_SLOTS_EXCEPTION_GETTER.get();
        }
        PhysicalSlot slot = firstEntry.getValue().iterator().next();
        ResourceID resourceID = slot.getTaskManagerLocation().getResourceID();
        SlotsUtilization slotsUtilization = taskExecutorSlotsUtilizations.get(resourceID);
        return new SlotTaskExecutorWeight<>(slotsUtilization, slot);
    }
}

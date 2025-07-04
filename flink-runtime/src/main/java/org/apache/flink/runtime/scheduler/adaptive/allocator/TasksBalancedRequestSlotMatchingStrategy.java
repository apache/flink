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
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import static org.apache.flink.runtime.scheduler.loading.WeightLoadable.sortByLoadingDescend;

/** The tasks balanced request slot matching strategy implementation. */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation) {
        final List<JobSchedulingPlan.SlotAssignment> slotAssignments =
                new ArrayList<>(requestGroups.size());
        final Map<ResourceID, LoadingWeight> taskExecutorLoadings =
                taskExecutorsLoadInformation.getTaskExecutorsLoadingWeight();
        final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
                RequestSlotMatchingStrategy.getSlotsPerTaskExecutor(freeSlots);
        final Map<LoadingWeight, Set<PhysicalSlot>> loadingSlotsMap =
                getLoadingSlotsMap(freeSlots, taskExecutorLoadings);

        SlotTaskExecutorWeight<LoadingWeight> best;
        for (ExecutionSlotSharingGroup requestGroup : sortByLoadingDescend(requestGroups)) {
            best = getTheBestSlotTaskExecutorLoading(loadingSlotsMap);
            slotAssignments.add(new SlotAssignment(best.physicalSlot, requestGroup));

            // Update the references
            final LoadingWeight newLoading =
                    best.taskExecutorWeight.merge(requestGroup.getLoading());
            taskExecutorLoadings.put(best.getResourceID(), newLoading);
            updateSlotsPerTaskExecutor(slotsPerTaskExecutor, best);
            Set<PhysicalSlot> physicalSlots = slotsPerTaskExecutor.get(best.getResourceID());
            updateLoadingSlotsMap(loadingSlotsMap, best, physicalSlots, newLoading);
        }
        return slotAssignments;
    }

    private static void updateLoadingSlotsMap(
            Map<LoadingWeight, Set<PhysicalSlot>> loadingSlotsMap,
            SlotTaskExecutorWeight<LoadingWeight> best,
            Set<PhysicalSlot> slotsToAdjust,
            LoadingWeight newLoading) {
        Set<PhysicalSlot> physicalSlots = loadingSlotsMap.get(best.taskExecutorWeight);
        if (Objects.nonNull(physicalSlots)) {
            physicalSlots.remove(best.physicalSlot);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            physicalSlots.removeAll(slotsToAdjust);
        }
        if (physicalSlots.isEmpty()) {
            loadingSlotsMap.remove(best.taskExecutorWeight);
        }
        if (Objects.nonNull(slotsToAdjust)) {
            Set<PhysicalSlot> slotsOfNewKey =
                    loadingSlotsMap.computeIfAbsent(
                            newLoading, slotsUtilization -> new HashSet<>());
            slotsOfNewKey.addAll(slotsToAdjust);
        }
    }

    private static void updateSlotsPerTaskExecutor(
            Map<ResourceID, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor,
            SlotTaskExecutorWeight<LoadingWeight> best) {
        Set<? extends SlotInfo> slotInfos = slotsPerTaskExecutor.get(best.getResourceID());
        if (Objects.nonNull(slotInfos)) {
            slotInfos.remove(best.physicalSlot);
        }
        if (Objects.isNull(slotInfos) || slotInfos.isEmpty()) {
            slotsPerTaskExecutor.remove(best.getResourceID());
        }
    }

    private static Map<LoadingWeight, Set<PhysicalSlot>> getLoadingSlotsMap(
            Collection<PhysicalSlot> slots, Map<ResourceID, LoadingWeight> taskExecutorLoadings) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                slotInfo ->
                                        taskExecutorLoadings.get(
                                                slotInfo.getTaskManagerLocation().getResourceID()),
                                TreeMap::new,
                                Collectors.toSet()));
    }

    private static SlotTaskExecutorWeight<LoadingWeight> getTheBestSlotTaskExecutorLoading(
            Map<LoadingWeight, Set<PhysicalSlot>> slotsByLoading) {
        final LoadingWeight loadingWeight =
                slotsByLoading.keySet().stream()
                        .min(LoadingWeight::compareTo)
                        .orElseThrow(NO_SLOTS_EXCEPTION_GETTER);
        final PhysicalSlot targetSlot =
                slotsByLoading.get(loadingWeight).stream()
                        .findAny()
                        .orElseThrow(NO_SLOTS_EXCEPTION_GETTER);
        return new SlotTaskExecutorWeight<>(loadingWeight, targetSlot);
    }
}

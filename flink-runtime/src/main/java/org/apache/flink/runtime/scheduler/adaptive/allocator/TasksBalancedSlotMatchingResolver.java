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
import org.apache.flink.runtime.scheduler.taskexecload.DefaultTaskExecutionLoad;
import org.apache.flink.runtime.scheduler.taskexecload.TaskExecutionLoad;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import static org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import static org.apache.flink.runtime.scheduler.taskexecload.HasTaskExecutionLoad.sortByTaskExecutionLoadDesc;

/** The tasks balanced request slot matching resolver implementation. */
public enum TasksBalancedSlotMatchingResolver implements SlotMatchingResolver {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchSlotSharingGroupWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots) {
        final List<JobSchedulingPlan.SlotAssignment> slotAssignments =
                new ArrayList<>(requestGroups.size());
        final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
                AllocatorUtil.getSlotsPerTaskExecutor(freeSlots);
        final TreeMap<TaskExecutionLoad, Set<PhysicalSlot>> loadingSlotsMap =
                getSlotsByTaskExecutionLoad(freeSlots);

        SlotTaskExecutorWeight<TaskExecutionLoad> best;
        for (ExecutionSlotSharingGroup requestGroup : sortByTaskExecutionLoadDesc(requestGroups)) {
            best = getLeastLoadedTaskSlot(loadingSlotsMap);
            slotAssignments.add(new SlotAssignment(best.physicalSlot, requestGroup));

            // Update the references
            final TaskExecutionLoad newLoading =
                    best.taskExecutorWeight.merge(requestGroup.getTaskExecutionLoad());
            updateSlotsPerTaskExecutor(slotsPerTaskExecutor, best);
            Set<PhysicalSlot> physicalSlots = slotsPerTaskExecutor.get(best.getResourceID());
            updateLoadingSlotsMap(loadingSlotsMap, best, physicalSlots, newLoading);
        }
        return slotAssignments;
    }

    private static void updateLoadingSlotsMap(
            Map<TaskExecutionLoad, Set<PhysicalSlot>> loadingSlotsMap,
            SlotTaskExecutorWeight<TaskExecutionLoad> best,
            Set<PhysicalSlot> slotsToAdjust,
            TaskExecutionLoad newLoading) {
        Set<PhysicalSlot> physicalSlots = loadingSlotsMap.get(best.taskExecutorWeight);
        if (!CollectionUtil.isNullOrEmpty(physicalSlots)) {
            physicalSlots.remove(best.physicalSlot);
        }
        if (!CollectionUtil.isNullOrEmpty(slotsToAdjust)
                && !CollectionUtil.isNullOrEmpty(physicalSlots)) {
            physicalSlots.removeAll(slotsToAdjust);
        }
        if (CollectionUtil.isNullOrEmpty(physicalSlots)) {
            loadingSlotsMap.remove(best.taskExecutorWeight);
        }
        if (!CollectionUtil.isNullOrEmpty(slotsToAdjust)) {
            Set<PhysicalSlot> slotsOfNewKey =
                    loadingSlotsMap.computeIfAbsent(
                            newLoading,
                            ignored ->
                                    CollectionUtil.newHashSetWithExpectedSize(
                                            slotsToAdjust.size()));
            slotsOfNewKey.addAll(slotsToAdjust);
        }
    }

    private static void updateSlotsPerTaskExecutor(
            Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor,
            SlotTaskExecutorWeight<TaskExecutionLoad> best) {
        Set<PhysicalSlot> slots = slotsPerTaskExecutor.get(best.getResourceID());
        if (Objects.nonNull(slots)) {
            slots.remove(best.physicalSlot);
        }
        if (CollectionUtil.isNullOrEmpty(slots)) {
            slotsPerTaskExecutor.remove(best.getResourceID());
        }
    }

    private static TreeMap<TaskExecutionLoad, Set<PhysicalSlot>> getSlotsByTaskExecutionLoad(
            Collection<PhysicalSlot> slots) {
        return new TreeMap<>() {
            {
                HashSet<PhysicalSlot> slotsValue =
                        CollectionUtil.newHashSetWithExpectedSize(slots.size());
                slotsValue.addAll(slots);
                put(DefaultTaskExecutionLoad.EMPTY, slotsValue);
            }
        };
    }

    private static SlotTaskExecutorWeight<TaskExecutionLoad> getLeastLoadedTaskSlot(
            TreeMap<TaskExecutionLoad, Set<PhysicalSlot>> slotsByTaskExecutionLoad) {
        final Map.Entry<TaskExecutionLoad, Set<PhysicalSlot>> firstEntry =
                slotsByTaskExecutionLoad.firstEntry();
        if (firstEntry == null
                || firstEntry.getKey() == null
                || CollectionUtil.isNullOrEmpty(firstEntry.getValue())) {
            throw NO_SLOTS_EXCEPTION_GETTER.get();
        }
        return new SlotTaskExecutorWeight<>(
                firstEntry.getKey(), firstEntry.getValue().iterator().next());
    }
}

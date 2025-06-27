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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.checkMinimumRequiredSlots;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.createExecutionSlotSharingGroups;

/**
 * Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. Specifically,
 * when the cluster is deployed in application mode and the {@link
 * org.apache.flink.configuration.JobManagerOptions#SCHEDULER_PREFER_MINIMAL_TASKMANAGERS_ENABLED}
 * is enabled, execution slot sharing groups are preferentially assigned to the minimal number of
 * task managers.
 */
public class DefaultSlotAssigner implements SlotAssigner {

    @VisibleForTesting static final String APPLICATION_MODE_EXECUTION_TARGET = "embedded";

    private final @Nullable String executionTarget;
    private final boolean minimalTaskManagerPreferred;

    DefaultSlotAssigner(@Nullable String executionTarget, boolean minimalTaskManagerPreferred) {
        this.executionTarget = executionTarget;
        this.minimalTaskManagerPreferred = minimalTaskManagerPreferred;
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkMinimumRequiredSlots(jobInformation, freeSlots);

        final List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        final Collection<? extends SlotInfo> pickedSlots =
                pickSlotsIfNeeded(allGroups.size(), freeSlots);

        Iterator<? extends SlotInfo> iterator = pickedSlots.iterator();
        Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : allGroups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    @VisibleForTesting
    Collection<? extends SlotInfo> pickSlotsIfNeeded(
            int requestExecutionSlotSharingGroups, Collection<? extends SlotInfo> freeSlots) {
        Collection<? extends SlotInfo> pickedSlots = freeSlots;
        if (APPLICATION_MODE_EXECUTION_TARGET.equalsIgnoreCase(executionTarget)
                && minimalTaskManagerPreferred
                // To avoid the sort-work loading.
                && freeSlots.size() > requestExecutionSlotSharingGroups) {
            final Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor =
                    getSlotsPerTaskExecutor(freeSlots);
            pickedSlots =
                    pickSlotsInMinimalTaskExecutors(
                            slotsPerTaskExecutor, requestExecutionSlotSharingGroups);
        }
        return pickedSlots;
    }

    /**
     * In order to minimize the using of task executors at the resource manager side in the
     * application-mode and release more task executors in a timely manner, it is a good choice to
     * prioritize selecting slots on task executors with the most available slots.
     *
     * @param slotsPerTaskExecutor The slots per task manager.
     * @return The ordered task manager that orders by the number of free slots descending.
     */
    private Iterator<TaskManagerLocation> getSortedTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsPerTaskExecutor) {
        final Comparator<TaskManagerLocation> taskExecutorComparator =
                (leftTml, rightTml) ->
                        Integer.compare(
                                slotsPerTaskExecutor.get(rightTml).size(),
                                slotsPerTaskExecutor.get(leftTml).size());
        return slotsPerTaskExecutor.keySet().stream().sorted(taskExecutorComparator).iterator();
    }

    /**
     * Pick the target slots to assign with the requested groups.
     *
     * @param slotsByTaskExecutor slots per task executor.
     * @param requestedGroups the number of the request execution slot sharing groups.
     * @return the target slots that are distributed on the minimal task executors.
     */
    private Collection<? extends SlotInfo> pickSlotsInMinimalTaskExecutors(
            Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> slotsByTaskExecutor,
            int requestedGroups) {
        final List<SlotInfo> pickedSlots = new ArrayList<>();
        final Iterator<TaskManagerLocation> sortedTaskExecutors =
                getSortedTaskExecutors(slotsByTaskExecutor);
        while (pickedSlots.size() < requestedGroups) {
            Set<? extends SlotInfo> slotInfos = slotsByTaskExecutor.get(sortedTaskExecutors.next());
            pickedSlots.addAll(slotInfos);
        }
        return pickedSlots;
    }

    private Map<TaskManagerLocation, ? extends Set<? extends SlotInfo>> getSlotsPerTaskExecutor(
            Collection<? extends SlotInfo> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                SlotInfo::getTaskManagerLocation,
                                Collectors.mapping(identity(), Collectors.toSet())));
    }
}

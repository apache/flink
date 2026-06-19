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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.checkMinimumRequiredSlots;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.getSlotsPerTaskExecutor;

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
    private final SlotSharingResolver slotSharingResolver;
    private final SlotMatchingResolver slotMatchingResolver;

    DefaultSlotAssigner(
            @Nullable String executionTarget,
            boolean minimalTaskManagerPreferred,
            SlotSharingResolver slotSharingResolver,
            SlotMatchingResolver slotMatchingResolver) {
        this.executionTarget = executionTarget;
        this.minimalTaskManagerPreferred = minimalTaskManagerPreferred;
        this.slotSharingResolver = slotSharingResolver;
        this.slotMatchingResolver = slotMatchingResolver;
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<PhysicalSlot> freeSlots,
            VertexParallelism vertexParallelism,
            JobAllocationsInformation previousAllocations) {
        checkMinimumRequiredSlots(jobInformation, freeSlots);

        final Collection<ExecutionSlotSharingGroup> allGroups =
                slotSharingResolver.getExecutionSlotSharingGroups(
                        jobInformation, vertexParallelism);

        final Collection<PhysicalSlot> pickedSlots = pickSlotsIfNeeded(allGroups.size(), freeSlots);

        return slotMatchingResolver.matchSlotSharingGroupWithSlots(allGroups, pickedSlots);
    }

    @VisibleForTesting
    Collection<PhysicalSlot> pickSlotsIfNeeded(
            int requestExecutionSlotSharingGroups, Collection<PhysicalSlot> freeSlots) {
        Collection<PhysicalSlot> pickedSlots = freeSlots;
        if (APPLICATION_MODE_EXECUTION_TARGET.equalsIgnoreCase(executionTarget)
                && minimalTaskManagerPreferred
                // To avoid the sort-work loading.
                && freeSlots.size() > requestExecutionSlotSharingGroups) {
            final Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor =
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
    private Iterator<ResourceID> getSortedTaskExecutors(
            Map<ResourceID, Set<PhysicalSlot>> slotsPerTaskExecutor) {
        final Comparator<ResourceID> taskExecutorComparator =
                (leftTm, rightTm) ->
                        Integer.compare(
                                slotsPerTaskExecutor.get(rightTm).size(),
                                slotsPerTaskExecutor.get(leftTm).size());
        return slotsPerTaskExecutor.keySet().stream().sorted(taskExecutorComparator).iterator();
    }

    /**
     * Pick the target slots to assign with the requested groups.
     *
     * @param slotsByTaskExecutor slots per task executor.
     * @param requestedGroups the number of the request execution slot sharing groups.
     * @return the target slots that are distributed on the minimal task executors.
     */
    private Collection<PhysicalSlot> pickSlotsInMinimalTaskExecutors(
            Map<ResourceID, Set<PhysicalSlot>> slotsByTaskExecutor, int requestedGroups) {
        final List<PhysicalSlot> pickedSlots = new ArrayList<>();
        final Iterator<ResourceID> sortedTaskExecutors =
                getSortedTaskExecutors(slotsByTaskExecutor);
        while (pickedSlots.size() < requestedGroups) {
            Set<PhysicalSlot> slotInfos = slotsByTaskExecutor.get(sortedTaskExecutors.next());
            pickedSlots.addAll(slotInfos);
        }
        return pickedSlots;
    }
}

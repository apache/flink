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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator {

    private final ReserveSlotFunction reserveSlotFunction;
    private final FreeSlotFunction freeSlotFunction;
    private final IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction;

    private SlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        this.reserveSlotFunction = reserveSlot;
        this.freeSlotFunction = freeSlotFunction;
        this.isSlotAvailableAndFreeFunction = isSlotAvailableAndFreeFunction;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        return new SlotSharingSlotAllocator(
                reserveSlot, freeSlotFunction, isSlotAvailableAndFreeFunction);
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        int numTotalRequiredSlots = 0;
        for (Integer requiredSlots : getMaxParallelismForSlotSharingGroups(vertices).values()) {
            numTotalRequiredSlots += requiredSlots;
        }
        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, numTotalRequiredSlots);
    }

    private static Map<SlotSharingGroupId, Integer> getMaxParallelismForSlotSharingGroups(
            Iterable<JobInformation.VertexInformation> vertices) {
        final Map<SlotSharingGroupId, Integer> maxParallelismForSlotSharingGroups = new HashMap<>();
        for (JobInformation.VertexInformation vertex : vertices) {
            maxParallelismForSlotSharingGroups.compute(
                    vertex.getSlotSharingGroup().getSlotSharingGroupId(),
                    (slotSharingGroupId, currentMaxParallelism) ->
                            currentMaxParallelism == null
                                    ? vertex.getParallelism()
                                    : Math.max(currentMaxParallelism, vertex.getParallelism()));
        }
        return maxParallelismForSlotSharingGroups;
    }

    @Override
    public Optional<VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {

        // => less slots than slot-sharing groups
        if (jobInformation.getSlotSharingGroups().size() > freeSlots.size()) {
            return Optional.empty();
        }

        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism =
                determineSlotsPerSharingGroup(jobInformation, freeSlots.size());

        final Map<JobVertexID, Integer> allVertexParallelism = new HashMap<>();

        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            final Map<JobVertexID, Integer> vertexParallelism =
                    determineVertexParallelism(
                            containedJobVertices,
                            slotSharingGroupParallelism.get(
                                    slotSharingGroup.getSlotSharingGroupId()));
            allVertexParallelism.putAll(vertexParallelism);
        }
        return Optional.of(new VertexParallelism(allVertexParallelism));
    }

    @Override
    public Optional<JobSchedulingPlan> determineParallelismAndCalculateAssignment(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots) {
        return determineParallelism(jobInformation, slots)
                .map(
                        parallelism ->
                                new JobSchedulingPlan(
                                        parallelism,
                                        assignSlots(jobInformation, slots, parallelism)));
    }

    private Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> freeSlots,
            VertexParallelism vertexParallelism) {
        List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        Iterator<? extends SlotInfo> iterator = freeSlots.iterator();
        Collection<SlotAssignment> assignments = new ArrayList<>();
        for (ExecutionSlotSharingGroup group : allGroups) {
            assignments.add(new SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }

    /**
     * Distributes free slots across the slot-sharing groups of the job. Slots are distributed as
     * evenly as possible. If a group requires less than an even share of slots the remainder is
     * distributed over the remaining groups.
     */
    private static Map<SlotSharingGroupId, Integer> determineSlotsPerSharingGroup(
            JobInformation jobInformation, int freeSlots) {
        int numUnassignedSlots = freeSlots;
        int numUnassignedSlotSharingGroups = jobInformation.getSlotSharingGroups().size();

        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism = new HashMap<>();

        for (Map.Entry<SlotSharingGroupId, Integer> slotSharingGroup :
                sortSlotSharingGroupsByDesiredParallelism(jobInformation)) {
            final int groupParallelism =
                    Math.min(
                            slotSharingGroup.getValue(),
                            numUnassignedSlots / numUnassignedSlotSharingGroups);

            slotSharingGroupParallelism.put(slotSharingGroup.getKey(), groupParallelism);

            numUnassignedSlots -= groupParallelism;
            numUnassignedSlotSharingGroups--;
        }

        return slotSharingGroupParallelism;
    }

    private static List<Map.Entry<SlotSharingGroupId, Integer>>
            sortSlotSharingGroupsByDesiredParallelism(JobInformation jobInformation) {

        return getMaxParallelismForSlotSharingGroups(jobInformation.getVertices()).entrySet()
                .stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());
    }

    private static Map<JobVertexID, Integer> determineVertexParallelism(
            Collection<JobInformation.VertexInformation> containedJobVertices, int availableSlots) {
        final Map<JobVertexID, Integer> vertexParallelism = new HashMap<>();
        for (JobInformation.VertexInformation jobVertex : containedJobVertices) {
            final int parallelism = Math.min(jobVertex.getParallelism(), availableSlots);

            vertexParallelism.put(jobVertex.getJobVertexID(), parallelism);
        }

        return vertexParallelism;
    }

    private static List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            VertexParallelism vertexParallelism, SlotSharingGroup slotSharingGroup) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();

        for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
            int parallelism = vertexParallelism.getParallelism(jobVertexId);
            for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                sharedSlotToVertexAssignment
                        .computeIfAbsent(subtaskIdx, ignored -> new HashSet<>())
                        .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
            }
        }

        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(JobSchedulingPlan jobSchedulingPlan) {
        final Collection<AllocationID> expectedSlots =
                calculateExpectedSlots(jobSchedulingPlan.getSlotAssignments());

        if (areAllExpectedSlotsAvailableAndFree(expectedSlots)) {
            final Map<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            for (SlotAssignment assignment : jobSchedulingPlan.getSlotAssignments()) {
                final SharedSlot sharedSlot = reserveSharedSlot(assignment.getSlotInfo());
                for (ExecutionVertexID executionVertexId :
                        assignment
                                .getTargetAs(ExecutionSlotSharingGroup.class)
                                .getContainedExecutionVertices()) {
                    assignedSlots.put(executionVertexId, sharedSlot.allocateLogicalSlot());
                }
            }

            return Optional.of(ReservedSlots.create(assignedSlots));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private Collection<AllocationID> calculateExpectedSlots(Iterable<SlotAssignment> assignments) {
        final Collection<AllocationID> requiredSlots = new ArrayList<>();

        for (SlotAssignment assignment : assignments) {
            requiredSlots.add(assignment.getSlotInfo().getAllocationId());
        }
        return requiredSlots;
    }

    private boolean areAllExpectedSlotsAvailableAndFree(
            Iterable<? extends AllocationID> requiredSlots) {
        for (AllocationID requiredSlot : requiredSlots) {
            if (!isSlotAvailableAndFreeFunction.isSlotAvailableAndFree(requiredSlot)) {
                return false;
            }
        }

        return true;
    }

    private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
        final PhysicalSlot physicalSlot =
                reserveSlotFunction.reserveSlot(
                        slotInfo.getAllocationId(), ResourceProfile.UNKNOWN);

        return new SharedSlot(
                new SlotRequestId(),
                physicalSlot,
                slotInfo.willBeOccupiedIndefinitely(),
                () ->
                        freeSlotFunction.freeSlot(
                                slotInfo.getAllocationId(), null, System.currentTimeMillis()));
    }

    static class ExecutionSlotSharingGroup {
        private final Set<ExecutionVertexID> containedExecutionVertices;

        public ExecutionSlotSharingGroup(Set<ExecutionVertexID> containedExecutionVertices) {
            this.containedExecutionVertices = containedExecutionVertices;
        }

        public Collection<ExecutionVertexID> getContainedExecutionVertices() {
            return containedExecutionVertices;
        }
    }
}

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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

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
    public Optional<VertexParallelismWithSlotSharing> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {

        // => less slots than slot-sharing groups
        if (jobInformation.getSlotSharingGroups().size() > freeSlots.size()) {
            return Optional.empty();
        }

        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism =
                determineSlotsPerSharingGroup(jobInformation, freeSlots.size());

        final Iterator<? extends SlotInfo> slotIterator = freeSlots.iterator();

        final Collection<ExecutionSlotSharingGroupAndSlot> assignments = new ArrayList<>();
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

            final Iterable<ExecutionSlotSharingGroup> sharedSlotToVertexAssignment =
                    createExecutionSlotSharingGroups(vertexParallelism);

            for (ExecutionSlotSharingGroup executionSlotSharingGroup :
                    sharedSlotToVertexAssignment) {
                final SlotInfo slotInfo = slotIterator.next();

                assignments.add(
                        new ExecutionSlotSharingGroupAndSlot(executionSlotSharingGroup, slotInfo));
            }
            allVertexParallelism.putAll(vertexParallelism);
        }

        return Optional.of(new VertexParallelismWithSlotSharing(allVertexParallelism, assignments));
    }

    /**
     * Distributes free slots across the slot-sharing groups of the job. Slots are distributed as
     * evenly as possible while taking the minimum parallelism of contained vertices into account.
     */
    private static Map<SlotSharingGroupId, Integer> determineSlotsPerSharingGroup(
            JobInformation jobInformation, int freeSlots) {
        int numUnassignedSlots = freeSlots;
        int numUnassignedSlotSharingGroups = jobInformation.getSlotSharingGroups().size();

        final Map<SlotSharingGroupId, Integer> slotSharingGroupParallelism = new HashMap<>();

        for (Tuple2<SlotSharingGroup, Integer> slotSharingGroup :
                sortSlotSharingGroupsByUpperParallelism(jobInformation)) {
            final int groupParallelism =
                    Math.min(
                            slotSharingGroup.f1,
                            numUnassignedSlots / numUnassignedSlotSharingGroups);

            slotSharingGroupParallelism.put(
                    slotSharingGroup.f0.getSlotSharingGroupId(), groupParallelism);

            numUnassignedSlots -= groupParallelism;
            numUnassignedSlotSharingGroups--;
        }

        return slotSharingGroupParallelism;
    }

    private static List<Tuple2<SlotSharingGroup, Integer>> sortSlotSharingGroupsByUpperParallelism(
            JobInformation jobInformation) {

        final List<Tuple2<SlotSharingGroup, Integer>> slotSharingGroupsSortedByParallelism =
                new ArrayList<>();

        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            int maxParallelismInGroup = 1;
            for (JobVertexID jobVertexId : slotSharingGroup.getJobVertexIds()) {
                maxParallelismInGroup =
                        Math.max(
                                maxParallelismInGroup,
                                jobInformation.getVertexInformation(jobVertexId).getParallelism());
            }
            slotSharingGroupsSortedByParallelism.add(
                    Tuple2.of(slotSharingGroup, maxParallelismInGroup));
        }
        slotSharingGroupsSortedByParallelism.sort(Comparator.comparingInt(x -> x.f1));
        return slotSharingGroupsSortedByParallelism;
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

    private static Iterable<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            Map<JobVertexID, Integer> containedJobVertices) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();

        for (Map.Entry<JobVertexID, Integer> jobVertex : containedJobVertices.entrySet()) {
            for (int i = 0; i < jobVertex.getValue(); i++) {
                sharedSlotToVertexAssignment
                        .computeIfAbsent(i, ignored -> new HashSet<>())
                        .add(new ExecutionVertexID(jobVertex.getKey(), i));
            }
        }

        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(VertexParallelism vertexParallelism) {
        Preconditions.checkArgument(
                vertexParallelism instanceof VertexParallelismWithSlotSharing,
                String.format(
                        "%s expects %s as argument.",
                        SlotSharingSlotAllocator.class.getSimpleName(),
                        VertexParallelismWithSlotSharing.class.getSimpleName()));

        final VertexParallelismWithSlotSharing vertexParallelismWithSlotSharing =
                (VertexParallelismWithSlotSharing) vertexParallelism;

        final Collection<AllocationID> expectedSlots =
                calculateExpectedSlots(vertexParallelismWithSlotSharing.getAssignments());

        if (areAllExpectedSlotsAvailableAndFree(expectedSlots)) {
            final Map<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            for (ExecutionSlotSharingGroupAndSlot executionSlotSharingGroup :
                    vertexParallelismWithSlotSharing.getAssignments()) {
                final SharedSlot sharedSlot =
                        reserveSharedSlot(executionSlotSharingGroup.getSlotInfo());

                for (ExecutionVertexID executionVertexId :
                        executionSlotSharingGroup
                                .getExecutionSlotSharingGroup()
                                .getContainedExecutionVertices()) {
                    final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();
                    assignedSlots.put(executionVertexId, logicalSlot);
                }
            }

            return Optional.of(ReservedSlots.create(assignedSlots));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private Collection<AllocationID> calculateExpectedSlots(
            Iterable<? extends ExecutionSlotSharingGroupAndSlot> assignments) {
        final Collection<AllocationID> requiredSlots = new ArrayList<>();

        for (ExecutionSlotSharingGroupAndSlot assignment : assignments) {
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

    static class ExecutionSlotSharingGroupAndSlot {
        private final ExecutionSlotSharingGroup executionSlotSharingGroup;
        private final SlotInfo slotInfo;

        public ExecutionSlotSharingGroupAndSlot(
                ExecutionSlotSharingGroup executionSlotSharingGroup, SlotInfo slotInfo) {
            this.executionSlotSharingGroup = executionSlotSharingGroup;
            this.slotInfo = slotInfo;
        }

        public ExecutionSlotSharingGroup getExecutionSlotSharingGroup() {
            return executionSlotSharingGroup;
        }

        public SlotInfo getSlotInfo() {
            return slotInfo;
        }
    }
}

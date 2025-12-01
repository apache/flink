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

import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.getMinimumRequiredSlots;
import static org.apache.flink.runtime.scheduler.adaptive.allocator.AllocatorUtil.getSlotSharingGroupMetaInfos;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator {

    private final ReserveSlotFunction reserveSlotFunction;
    private final FreeSlotFunction freeSlotFunction;
    private final IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction;
    private final boolean localRecoveryEnabled;
    private final @Nullable String executionTarget;
    private final boolean minimalTaskManagerPreferred;
    private final SlotSharingResolver slotSharingResolver;
    private final SlotMatchingResolver slotMatchingResolver;

    private SlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction,
            boolean localRecoveryEnabled,
            @Nullable String executionTarget,
            boolean minimalTaskManagerPreferred,
            TaskManagerOptions.TaskManagerLoadBalanceMode taskManagerLoadBalanceMode) {
        this.reserveSlotFunction = reserveSlot;
        this.freeSlotFunction = freeSlotFunction;
        this.isSlotAvailableAndFreeFunction = isSlotAvailableAndFreeFunction;
        this.localRecoveryEnabled = localRecoveryEnabled;
        this.executionTarget = executionTarget;
        this.minimalTaskManagerPreferred = minimalTaskManagerPreferred;
        this.slotSharingResolver = getSlotSharingResolver(taskManagerLoadBalanceMode);
        this.slotMatchingResolver = getSlotMatchingResolver(taskManagerLoadBalanceMode);
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction,
            boolean localRecoveryEnabled,
            @Nullable String executionTarget,
            boolean minimalTaskManagerPreferred,
            TaskManagerOptions.TaskManagerLoadBalanceMode taskManagerLoadBalanceMode) {
        return new SlotSharingSlotAllocator(
                reserveSlot,
                freeSlotFunction,
                isSlotAvailableAndFreeFunction,
                localRecoveryEnabled,
                executionTarget,
                minimalTaskManagerPreferred,
                taskManagerLoadBalanceMode);
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        int numTotalRequiredSlots = 0;
        for (SlotSharingGroupMetaInfo slotSharingGroupMetaInfo :
                SlotSharingGroupMetaInfo.from(vertices).values()) {
            numTotalRequiredSlots += slotSharingGroupMetaInfo.getMaxUpperBound();
        }
        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, numTotalRequiredSlots);
    }

    @Override
    public Optional<VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {

        final Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo =
                getSlotSharingGroupMetaInfos(jobInformation);

        final int minimumRequiredSlots = getMinimumRequiredSlots(slotSharingGroupMetaInfo);

        if (minimumRequiredSlots > freeSlots.size()) {
            return Optional.empty();
        }

        final Map<SlotSharingGroup, Integer> slotSharingGroupParallelism =
                determineSlotsPerSharingGroup(
                        jobInformation,
                        freeSlots.size(),
                        minimumRequiredSlots,
                        slotSharingGroupMetaInfo);

        final Map<JobVertexID, Integer> allVertexParallelism = new HashMap<>();

        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            final Map<JobVertexID, Integer> vertexParallelism =
                    determineVertexParallelism(
                            containedJobVertices,
                            slotSharingGroupParallelism.get(slotSharingGroup));
            allVertexParallelism.putAll(vertexParallelism);
        }
        return Optional.of(new VertexParallelism(allVertexParallelism));
    }

    @Override
    public Optional<JobSchedulingPlan> determineParallelismAndCalculateAssignment(
            JobInformation jobInformation,
            Collection<PhysicalSlot> slots,
            JobAllocationsInformation jobAllocationsInformation) {
        return determineParallelism(jobInformation, slots)
                .map(
                        parallelism -> {
                            SlotAssigner slotAssigner =
                                    localRecoveryEnabled && !jobAllocationsInformation.isEmpty()
                                            ? new StateLocalitySlotAssigner(slotSharingResolver)
                                            : new DefaultSlotAssigner(
                                                    executionTarget,
                                                    minimalTaskManagerPreferred,
                                                    slotSharingResolver,
                                                    slotMatchingResolver);
                            return new JobSchedulingPlan(
                                    parallelism,
                                    slotAssigner.assignSlots(
                                            jobInformation,
                                            slots,
                                            parallelism,
                                            jobAllocationsInformation));
                        });
    }

    private SlotSharingResolver getSlotSharingResolver(
            TaskManagerOptions.TaskManagerLoadBalanceMode taskManagerLoadBalanceMode) {
        switch (taskManagerLoadBalanceMode) {
            case NONE:
            case MIN_RESOURCES:
            case SLOTS:
                return DefaultSlotSharingResolver.INSTANCE;
            case TASKS:
                return TaskBalancedSlotSharingResolver.INSTANCE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode: %s when initializing slot sharing resolver.",
                                taskManagerLoadBalanceMode));
        }
    }

    private SlotMatchingResolver getSlotMatchingResolver(
            TaskManagerOptions.TaskManagerLoadBalanceMode taskManagerLoadBalanceMode) {
        switch (taskManagerLoadBalanceMode) {
            case NONE:
            case MIN_RESOURCES:
                return SimpleSlotMatchingResolver.INSTANCE;
            case SLOTS:
                return SlotsBalancedSlotMatchingResolver.INSTANCE;
            case TASKS:
                return TasksBalancedSlotMatchingResolver.INSTANCE;
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported task manager load balance mode: %s when initializing slot matching resolver",
                                taskManagerLoadBalanceMode));
        }
    }

    /**
     * Distributes free slots across the slot-sharing groups of the job. Slots are distributed as
     * evenly as possible. If a group requires less than an even share of slots the remainder is
     * distributed over the remaining groups.
     */
    private static Map<SlotSharingGroup, Integer> determineSlotsPerSharingGroup(
            JobInformation jobInformation,
            int freeSlots,
            int minRequiredSlots,
            Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo) {

        int numUnassignedSlots = freeSlots;
        int numUnassignedSlotSharingGroups = jobInformation.getSlotSharingGroups().size();
        int numMinSlotsRequiredByRemainingGroups = minRequiredSlots;

        final Map<SlotSharingGroup, Integer> slotSharingGroupParallelism = new HashMap<>();

        for (SlotSharingGroup slotSharingGroup :
                sortSlotSharingGroupsByHighestParallelismRange(slotSharingGroupMetaInfo)) {
            final int minParallelism =
                    slotSharingGroupMetaInfo.get(slotSharingGroup).getMaxLowerBound();

            // if we reached this point we know we have more slots than we need to fulfill the
            // minimum requirements for each slot sharing group.
            // this means that a certain number of slots are already implicitly reserved (to fulfill
            // the minimum requirement of other groups); so we only need to distribute the remaining
            // "optional" slots while only accounting for the requirements beyond the minimum

            // the number of slots this group can use beyond the minimum
            final int maxOptionalSlots =
                    slotSharingGroupMetaInfo.get(slotSharingGroup).getMaxUpperBound()
                            - minParallelism;
            // the number of slots that are not implicitly reserved for minimum requirements
            final int freeOptionalSlots = numUnassignedSlots - numMinSlotsRequiredByRemainingGroups;
            // the number of slots this group is allowed to use beyond the minimum requirements
            final int optionalSlotShare = freeOptionalSlots / numUnassignedSlotSharingGroups;

            final int groupParallelism =
                    minParallelism + Math.min(maxOptionalSlots, optionalSlotShare);

            slotSharingGroupParallelism.put(slotSharingGroup, groupParallelism);

            numMinSlotsRequiredByRemainingGroups -= minParallelism;
            numUnassignedSlots -= groupParallelism;
            numUnassignedSlotSharingGroups--;
        }

        return slotSharingGroupParallelism;
    }

    private static List<SlotSharingGroup> sortSlotSharingGroupsByHighestParallelismRange(
            Map<SlotSharingGroup, SlotSharingGroupMetaInfo> slotSharingGroupMetaInfo) {

        return slotSharingGroupMetaInfo.entrySet().stream()
                .sorted(
                        Comparator.comparingInt(
                                entry -> entry.getValue().getMaxLowerUpperBoundRange()))
                .map(Map.Entry::getKey)
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

    /** The execution slot sharing group for adaptive scheduler. */
    public static class ExecutionSlotSharingGroup implements WeightLoadable {
        private final String id;
        private final SlotSharingGroup slotSharingGroup;
        private final Set<ExecutionVertexID> containedExecutionVertices;

        public ExecutionSlotSharingGroup(
                SlotSharingGroup slotSharingGroup,
                Set<ExecutionVertexID> containedExecutionVertices) {
            this(UUID.randomUUID().toString(), slotSharingGroup, containedExecutionVertices);
        }

        public ExecutionSlotSharingGroup(
                String id,
                SlotSharingGroup slotSharingGroup,
                Set<ExecutionVertexID> containedExecutionVertices) {
            this.id = id;
            this.slotSharingGroup = Preconditions.checkNotNull(slotSharingGroup);
            this.containedExecutionVertices = containedExecutionVertices;
        }

        public SlotSharingGroup getSlotSharingGroup() {
            return slotSharingGroup;
        }

        public String getId() {
            return id;
        }

        public ResourceProfile getResourceProfile() {
            return slotSharingGroup.getResourceProfile();
        }

        public Collection<ExecutionVertexID> getContainedExecutionVertices() {
            return containedExecutionVertices;
        }

        @Nonnull
        @Override
        public LoadingWeight getLoading() {
            return new DefaultLoadingWeight(containedExecutionVertices.size());
        }
    }

    public static class SlotSharingGroupMetaInfo {

        private final int minLowerBound;
        private final int maxLowerBound;
        private final int maxUpperBound;

        private SlotSharingGroupMetaInfo(int minLowerBound, int maxLowerBound, int maxUpperBound) {
            this.minLowerBound = minLowerBound;
            this.maxLowerBound = maxLowerBound;
            this.maxUpperBound = maxUpperBound;
        }

        public int getMinLowerBound() {
            return minLowerBound;
        }

        public int getMaxLowerBound() {
            return maxLowerBound;
        }

        public int getMaxUpperBound() {
            return maxUpperBound;
        }

        public int getMaxLowerUpperBoundRange() {
            return maxUpperBound - maxLowerBound;
        }

        public static Map<SlotSharingGroup, SlotSharingGroupMetaInfo> from(
                Iterable<JobInformation.VertexInformation> vertices) {

            return getPerSlotSharingGroups(
                    vertices,
                    vertexInformation ->
                            new SlotSharingGroupMetaInfo(
                                    vertexInformation.getMinParallelism(),
                                    vertexInformation.getMinParallelism(),
                                    vertexInformation.getParallelism()),
                    (metaInfo1, metaInfo2) ->
                            new SlotSharingGroupMetaInfo(
                                    Math.min(
                                            metaInfo1.getMinLowerBound(),
                                            metaInfo2.getMinLowerBound()),
                                    Math.max(
                                            metaInfo1.getMaxLowerBound(),
                                            metaInfo2.getMaxLowerBound()),
                                    Math.max(
                                            metaInfo1.getMaxUpperBound(),
                                            metaInfo2.getMaxUpperBound())));
        }

        static <T> Map<SlotSharingGroup, T> getPerSlotSharingGroups(
                Iterable<JobInformation.VertexInformation> vertices,
                Function<JobInformation.VertexInformation, T> mapper,
                BiFunction<T, T, T> reducer) {
            final Map<SlotSharingGroup, T> extractedPerSlotSharingGroups = new HashMap<>();
            for (JobInformation.VertexInformation vertex : vertices) {
                extractedPerSlotSharingGroups.compute(
                        vertex.getSlotSharingGroup(),
                        (ignored, currentData) ->
                                currentData == null
                                        ? mapper.apply(vertex)
                                        : reducer.apply(currentData, mapper.apply(vertex)));
            }
            return extractedPerSlotSharingGroups;
        }
    }
}

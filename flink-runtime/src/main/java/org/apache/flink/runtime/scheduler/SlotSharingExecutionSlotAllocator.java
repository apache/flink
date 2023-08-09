/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}. It allocates
 * a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the
 * shared slot. Each subsequent sharing subtask allocates a logical slot from the existing shared
 * slot. The shared/physical slot can be released only if all the requested logical slots are
 * released or canceled.
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
    private static final Logger LOG =
            LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final SlotSharingStrategy slotSharingStrategy;

    private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

    private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

    SlotSharingExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy,
            SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
        this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
        this.bulkChecker = checkNotNull(bulkChecker);
        this.allocationTimeout = checkNotNull(allocationTimeout);
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.sharedSlots = new IdentityHashMap<>();

        this.slotProvider.disableBatchSlotRequestTimeoutCheck();
    }

    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {

        final Map<ExecutionVertexID, ExecutionAttemptID> vertexIdToExecutionId = new HashMap<>();
        executionAttemptIds.forEach(
                executionId ->
                        vertexIdToExecutionId.put(executionId.getExecutionVertexId(), executionId));

        checkState(
                vertexIdToExecutionId.size() == executionAttemptIds.size(),
                "SlotSharingExecutionSlotAllocator does not support one execution vertex to have multiple concurrent executions");

        final List<ExecutionVertexID> vertexIds =
                executionAttemptIds.stream()
                        .map(ExecutionAttemptID::getExecutionVertexId)
                        .collect(Collectors.toList());

        return allocateSlotsForVertices(vertexIds).stream()
                .collect(
                        Collectors.toMap(
                                vertexAssignment ->
                                        vertexIdToExecutionId.get(
                                                vertexAssignment.getExecutionVertexId()),
                                vertexAssignment ->
                                        new ExecutionSlotAssignment(
                                                vertexIdToExecutionId.get(
                                                        vertexAssignment.getExecutionVertexId()),
                                                vertexAssignment.getLogicalSlotFuture())));
    }

    /**
     * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
     *
     * <p>The allocation has the following steps:
     *
     * <ol>
     *   <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link
     *       SlotSharingStrategy}
     *   <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot
     *   <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot
     *       yet:
     *   <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then
     *   <li>Allocate a physical slot from the {@link PhysicalSlotProvider}
     *   <li>Create a shared slot based on the returned physical slot futures
     *   <li>Allocate logical slot futures for the executions from all corresponding shared slots.
     *   <li>If a physical slot request fails, associated logical slot requests are canceled within
     *       the shared slot
     *   <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and
     *       returns the results.
     * </ol>
     *
     * @param executionVertexIds Execution vertices to allocate slots for
     */
    private List<SlotExecutionVertexAssignment> allocateSlotsForVertices(
            List<ExecutionVertexID> executionVertexIds) {

        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                sharedSlotProfileRetrieverFactory.createFromBulk(new HashSet<>(executionVertexIds));
        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup =
                executionVertexIds.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotSharingStrategy::getExecutionSlotSharingGroup));

        Map<ExecutionSlotSharingGroup, SharedSlot> slots = new HashMap<>(executionsByGroup.size());
        Set<ExecutionSlotSharingGroup> groupsToAssign = new HashSet<>(executionsByGroup.keySet());

        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                tryAssignExistingSharedSlots(groupsToAssign);
        slots.putAll(assignedSlots);
        groupsToAssign.removeAll(assignedSlots.keySet());

        if (!groupsToAssign.isEmpty()) {
            Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots =
                    allocateSharedSlots(groupsToAssign, sharedSlotProfileRetriever);
            slots.putAll(allocatedSlots);
            groupsToAssign.removeAll(allocatedSlots.keySet());
            Preconditions.checkState(groupsToAssign.isEmpty());
        }

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments =
                allocateLogicalSlotsFromSharedSlots(slots, executionsByGroup);

        // we need to pass the slots map to the createBulk method instead of using the allocator's
        // 'sharedSlots'
        // because if any physical slots have already failed, their shared slots have been removed
        // from the allocator's 'sharedSlots' by failed logical slots.
        SharingPhysicalSlotRequestBulk bulk = createBulk(slots, executionsByGroup);
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, allocationTimeout);

        return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        cancelLogicalSlotRequest(executionAttemptId.getExecutionVertexId(), null);
    }

    private void cancelLogicalSlotRequest(ExecutionVertexID executionVertexId, Throwable cause) {
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
        checkNotNull(
                executionSlotSharingGroup,
                "There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
        SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
        if (slot != null) {
            slot.cancelLogicalSlotRequest(executionVertexId, cause);
        } else {
            LOG.debug(
                    "There is no SharedSlot for ExecutionSlotSharingGroup of ExecutionVertexID {}",
                    executionVertexId);
        }
    }

    private static Map<ExecutionVertexID, SlotExecutionVertexAssignment>
            allocateLogicalSlotsFromSharedSlots(
                    Map<ExecutionSlotSharingGroup, SharedSlot> slots,
                    Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup) {

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = new HashMap<>();

        for (Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> entry :
                executionsByGroup.entrySet()) {
            ExecutionSlotSharingGroup group = entry.getKey();
            List<ExecutionVertexID> executionIds = entry.getValue();

            for (ExecutionVertexID executionId : executionIds) {
                CompletableFuture<LogicalSlot> logicalSlotFuture =
                        slots.get(group).allocateLogicalSlot(executionId);
                SlotExecutionVertexAssignment assignment =
                        new SlotExecutionVertexAssignment(executionId, logicalSlotFuture);
                assignments.put(executionId, assignment);
            }
        }

        return assignments;
    }

    private Map<ExecutionSlotSharingGroup, SharedSlot> tryAssignExistingSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups) {
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots =
                new HashMap<>(executionSlotSharingGroups.size());
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SharedSlot sharedSlot = sharedSlots.get(group);
            if (sharedSlot != null) {
                assignedSlots.put(group, sharedSlot);
            }
        }
        return assignedSlots;
    }

    private Map<ExecutionSlotSharingGroup, SharedSlot> allocateSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups,
            SharedSlotProfileRetriever sharedSlotProfileRetriever) {

        List<PhysicalSlotRequest> slotRequests = new ArrayList<>();
        Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots = new HashMap<>();

        Map<SlotRequestId, ExecutionSlotSharingGroup> requestToGroup = new HashMap<>();
        Map<SlotRequestId, ResourceProfile> requestToPhysicalResources = new HashMap<>();

        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SlotRequestId physicalSlotRequestId = new SlotRequestId();
            ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);
            SlotProfile slotProfile =
                    sharedSlotProfileRetriever.getSlotProfile(group, physicalSlotResourceProfile);
            PhysicalSlotRequest request =
                    new PhysicalSlotRequest(
                            physicalSlotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
            slotRequests.add(request);
            requestToGroup.put(physicalSlotRequestId, group);
            requestToPhysicalResources.put(physicalSlotRequestId, physicalSlotResourceProfile);
        }

        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocateResult =
                slotProvider.allocatePhysicalSlots(slotRequests);

        allocateResult.forEach(
                (slotRequestId, resultCompletableFuture) -> {
                    ExecutionSlotSharingGroup group = requestToGroup.get(slotRequestId);
                    CompletableFuture<PhysicalSlot> physicalSlotFuture =
                            resultCompletableFuture.thenApply(
                                    PhysicalSlotRequest.Result::getPhysicalSlot);
                    SharedSlot slot =
                            new SharedSlot(
                                    slotRequestId,
                                    requestToPhysicalResources.get(slotRequestId),
                                    group,
                                    physicalSlotFuture,
                                    slotWillBeOccupiedIndefinitely,
                                    this::releaseSharedSlot);
                    allocatedSlots.put(group, slot);
                    Preconditions.checkState(!sharedSlots.containsKey(group));
                    sharedSlots.put(group, slot);
                });
        return allocatedSlots;
    }

    private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(
                slot.isEmpty(),
                "Trying to remove a shared slot with physical request id %s which has assigned logical slots",
                slot.getPhysicalSlotRequestId());
        slotProvider.cancelSlotRequest(
                slot.getPhysicalSlotRequestId(),
                new FlinkException(
                        "Slot is being returned from SlotSharingExecutionSlotAllocator."));
    }

    private ResourceProfile getPhysicalSlotResourceProfile(
            ExecutionSlotSharingGroup executionSlotSharingGroup) {
        if (!executionSlotSharingGroup.getResourceProfile().equals(ResourceProfile.UNKNOWN)) {
            return executionSlotSharingGroup.getResourceProfile();
        } else {
            return executionSlotSharingGroup.getExecutionVertexIds().stream()
                    .reduce(
                            ResourceProfile.ZERO,
                            (r, e) -> r.merge(resourceProfileRetriever.apply(e)),
                            ResourceProfile::merge);
        }
    }

    private SharingPhysicalSlotRequestBulk createBulk(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests =
                executions.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        group -> group,
                                        group ->
                                                slots.get(group).getPhysicalSlotResourceProfile()));
        SharingPhysicalSlotRequestBulk bulk =
                new SharingPhysicalSlotRequestBulk(
                        executions, pendingRequests, this::cancelLogicalSlotRequest);
        registerPhysicalSlotRequestBulkCallbacks(slots, executions.keySet(), bulk);
        return bulk;
    }

    private static void registerPhysicalSlotRequestBulkCallbacks(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Iterable<ExecutionSlotSharingGroup> executions,
            SharingPhysicalSlotRequestBulk bulk) {
        for (ExecutionSlotSharingGroup group : executions) {
            CompletableFuture<PhysicalSlot> slotContextFuture =
                    slots.get(group).getSlotContextFuture();
            slotContextFuture.thenAccept(
                    physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
            slotContextFuture.exceptionally(
                    t -> {
                        // clear the bulk to stop the fulfillability check
                        bulk.clearPendingRequests();
                        return null;
                    });
        }
    }

    /** The slot assignment for an {@link ExecutionVertex}. */
    private static class SlotExecutionVertexAssignment {

        private final ExecutionVertexID executionVertexId;

        private final CompletableFuture<LogicalSlot> logicalSlotFuture;

        SlotExecutionVertexAssignment(
                ExecutionVertexID executionVertexId,
                CompletableFuture<LogicalSlot> logicalSlotFuture) {
            this.executionVertexId = checkNotNull(executionVertexId);
            this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
        }

        ExecutionVertexID getExecutionVertexId() {
            return executionVertexId;
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return logicalSlotFuture;
        }
    }
}

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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for all {@link ExecutionSlotAllocator}. It is responsible to allocate slots for tasks
 * and keep the unfulfilled slot requests for further cancellation.
 */
abstract class AbstractExecutionSlotAllocator implements ExecutionSlotAllocator {

    private final Map<ExecutionVertexID, SlotExecutionVertexAssignment> pendingSlotAssignments;

    private final PreferredLocationsRetriever preferredLocationsRetriever;

    AbstractExecutionSlotAllocator(final PreferredLocationsRetriever preferredLocationsRetriever) {
        this.preferredLocationsRetriever = checkNotNull(preferredLocationsRetriever);
        this.pendingSlotAssignments = new HashMap<>();
    }

    @Override
    public void cancel(final ExecutionVertexID executionVertexId) {
        final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
                pendingSlotAssignments.get(executionVertexId);
        if (slotExecutionVertexAssignment != null) {
            slotExecutionVertexAssignment.getLogicalSlotFuture().cancel(false);
        }
    }

    void validateSchedulingRequirements(
            final Collection<ExecutionVertexSchedulingRequirements> schedulingRequirements) {
        schedulingRequirements.stream()
                .map(ExecutionVertexSchedulingRequirements::getExecutionVertexId)
                .forEach(
                        id ->
                                checkState(
                                        !pendingSlotAssignments.containsKey(id),
                                        "BUG: vertex %s tries to allocate a slot when its previous slot request is still pending",
                                        id));
    }

    SlotExecutionVertexAssignment createAndRegisterSlotExecutionVertexAssignment(
            final ExecutionVertexID executionVertexId,
            final CompletableFuture<LogicalSlot> logicalSlotFuture,
            final Consumer<Throwable> slotRequestFailureHandler) {

        final SlotExecutionVertexAssignment slotExecutionVertexAssignment =
                new SlotExecutionVertexAssignment(executionVertexId, logicalSlotFuture);

        // add to map first in case the slot future is already completed
        pendingSlotAssignments.put(executionVertexId, slotExecutionVertexAssignment);

        logicalSlotFuture.whenComplete(
                (ignored, throwable) -> {
                    pendingSlotAssignments.remove(executionVertexId);
                    if (throwable != null) {
                        slotRequestFailureHandler.accept(throwable);
                    }
                });

        return slotExecutionVertexAssignment;
    }

    CompletableFuture<SlotProfile> getSlotProfileFuture(
            final ExecutionVertexSchedulingRequirements schedulingRequirements,
            final ResourceProfile physicalSlotResourceProfile,
            final Set<ExecutionVertexID> producersToIgnore,
            final Set<AllocationID> allPreviousAllocationIds) {

        final CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture =
                preferredLocationsRetriever.getPreferredLocations(
                        schedulingRequirements.getExecutionVertexId(), producersToIgnore);

        return preferredLocationsFuture.thenApply(
                preferredLocations ->
                        SlotProfile.priorAllocation(
                                schedulingRequirements.getTaskResourceProfile(),
                                physicalSlotResourceProfile,
                                preferredLocations,
                                Collections.singletonList(
                                        schedulingRequirements.getPreviousAllocationId()),
                                allPreviousAllocationIds));
    }

    @VisibleForTesting
    static Set<AllocationID> computeAllPriorAllocationIds(
            final Collection<ExecutionVertexSchedulingRequirements>
                    executionVertexSchedulingRequirements) {

        return executionVertexSchedulingRequirements.stream()
                .map(ExecutionVertexSchedulingRequirements::getPreviousAllocationId)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    Map<ExecutionVertexID, SlotExecutionVertexAssignment> getPendingSlotAssignments() {
        return Collections.unmodifiableMap(pendingSlotAssignments);
    }
}

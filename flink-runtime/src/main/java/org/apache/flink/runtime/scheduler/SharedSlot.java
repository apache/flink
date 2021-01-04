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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shared slot implementation for the {@link SlotSharingExecutionSlotAllocator}.
 *
 * <p>The shared slots are owned and tracked by {@link SlotSharingExecutionSlotAllocator}. The
 * shared slot represents a collection of {@link SingleLogicalSlot} requests which share one
 * physical slot. The shared slot is created by the {@link SlotSharingExecutionSlotAllocator} from
 * the physical slot request. Afterwards, {@link SlotSharingExecutionSlotAllocator} requests logical
 * slots from the underlying physical slot for {@link Execution executions} which share it.
 *
 * <p>The shared slot becomes a {@link PhysicalSlot.Payload} of its underlying physical slot once
 * the physical slot is obtained. If the allcoated physical slot gets released then it calls back
 * the shared slot to release the logical slots which fail their execution payloads.
 *
 * <p>A logical slot request can be cancelled if it is not completed yet or returned by the
 * execution if it has been completed and given to the execution by {@link
 * SlotSharingExecutionSlotAllocator}. If the underlying physical slot fails, it fails all logical
 * slot requests. The failed, cancelled or returned logical slot requests are removed from the
 * shared slot. Once the shared slot has no registered logical slot requests, it calls back its
 * {@link SlotSharingExecutionSlotAllocator} to remove it from the allocator and cancel its
 * underlying physical slot request if the request is not fulfilled yet.
 */
class SharedSlot implements SlotOwner, PhysicalSlot.Payload {
    private static final Logger LOG = LoggerFactory.getLogger(SharedSlot.class);

    private final SlotRequestId physicalSlotRequestId;

    private final ResourceProfile physicalSlotResourceProfile;

    private final ExecutionSlotSharingGroup executionSlotSharingGroup;

    private final CompletableFuture<PhysicalSlot> slotContextFuture;

    private final DualKeyLinkedMap<
                    ExecutionVertexID, SlotRequestId, CompletableFuture<SingleLogicalSlot>>
            requestedLogicalSlots;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final Consumer<ExecutionSlotSharingGroup> externalReleaseCallback;

    private State state;

    SharedSlot(
            SlotRequestId physicalSlotRequestId,
            ResourceProfile physicalSlotResourceProfile,
            ExecutionSlotSharingGroup executionSlotSharingGroup,
            CompletableFuture<PhysicalSlot> slotContextFuture,
            boolean slotWillBeOccupiedIndefinitely,
            Consumer<ExecutionSlotSharingGroup> externalReleaseCallback) {
        this.physicalSlotRequestId = physicalSlotRequestId;
        this.physicalSlotResourceProfile = physicalSlotResourceProfile;
        this.executionSlotSharingGroup = executionSlotSharingGroup;
        this.slotContextFuture =
                slotContextFuture.thenApply(
                        physicalSlot -> {
                            Preconditions.checkState(
                                    physicalSlot.tryAssignPayload(this),
                                    "Unexpected physical slot payload assignment failure!");
                            return physicalSlot;
                        });
        this.requestedLogicalSlots =
                new DualKeyLinkedMap<>(executionSlotSharingGroup.getExecutionVertexIds().size());
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.externalReleaseCallback = externalReleaseCallback;
        this.state = State.ALLOCATED;
    }

    SlotRequestId getPhysicalSlotRequestId() {
        return physicalSlotRequestId;
    }

    ResourceProfile getPhysicalSlotResourceProfile() {
        return physicalSlotResourceProfile;
    }

    public ExecutionSlotSharingGroup getExecutionSlotSharingGroup() {
        return executionSlotSharingGroup;
    }

    CompletableFuture<PhysicalSlot> getSlotContextFuture() {
        return slotContextFuture;
    }

    /**
     * Registers an allocation request for a logical slot.
     *
     * <p>The logical slot request is complete once the underlying physical slot request is
     * complete.
     *
     * @param executionVertexId {@link ExecutionVertexID} of the execution for which to allocate the
     *     logical slot
     * @return the logical slot future
     */
    CompletableFuture<LogicalSlot> allocateLogicalSlot(ExecutionVertexID executionVertexId) {
        Preconditions.checkArgument(
                executionSlotSharingGroup.getExecutionVertexIds().contains(executionVertexId),
                "Trying to allocate a logical slot for execution %s which is not in the ExecutionSlotSharingGroup",
                executionVertexId);
        CompletableFuture<SingleLogicalSlot> logicalSlotFuture =
                requestedLogicalSlots.getValueByKeyA(executionVertexId);
        if (logicalSlotFuture != null) {
            LOG.debug("Request for {} already exists", getLogicalSlotString(executionVertexId));
        } else {
            logicalSlotFuture = allocateNonExistentLogicalSlot(executionVertexId);
        }
        return logicalSlotFuture.thenApply(Function.identity());
    }

    private CompletableFuture<SingleLogicalSlot> allocateNonExistentLogicalSlot(
            ExecutionVertexID executionVertexId) {
        CompletableFuture<SingleLogicalSlot> logicalSlotFuture;
        SlotRequestId logicalSlotRequestId = new SlotRequestId();
        String logMessageBase = getLogicalSlotString(logicalSlotRequestId, executionVertexId);
        LOG.debug("Request a {}", logMessageBase);

        logicalSlotFuture =
                slotContextFuture.thenApply(
                        physicalSlot -> {
                            LOG.debug("Allocated {}", logMessageBase);
                            return createLogicalSlot(physicalSlot, logicalSlotRequestId);
                        });
        requestedLogicalSlots.put(executionVertexId, logicalSlotRequestId, logicalSlotFuture);

        // If the physical slot request fails (slotContextFuture), it will also fail the
        // logicalSlotFuture.
        // Therefore, the next `exceptionally` callback will call removeLogicalSlotRequest and do
        // the cleanup
        // in requestedLogicalSlots and eventually in sharedSlots
        logicalSlotFuture.exceptionally(
                cause -> {
                    LOG.debug("Failed {}", logMessageBase, cause);
                    removeLogicalSlotRequest(logicalSlotRequestId);
                    return null;
                });
        return logicalSlotFuture;
    }

    private SingleLogicalSlot createLogicalSlot(
            PhysicalSlot physicalSlot, SlotRequestId logicalSlotRequestId) {
        return new SingleLogicalSlot(
                logicalSlotRequestId,
                physicalSlot,
                null,
                Locality.UNKNOWN,
                this,
                slotWillBeOccupiedIndefinitely);
    }

    /**
     * Cancels a logical slot request.
     *
     * <p>If the logical slot request is already complete, nothing happens because the logical slot
     * is already given to the execution and it the responsibility of the execution to call {@link
     * #returnLogicalSlot(LogicalSlot)}.
     *
     * <p>If the logical slot request is not complete yet, its future gets cancelled or failed.
     *
     * @param executionVertexID {@link ExecutionVertexID} of the execution for which to cancel the
     *     logical slot
     * @param cause the reason of cancellation or null if it is not available
     */
    void cancelLogicalSlotRequest(ExecutionVertexID executionVertexID, @Nullable Throwable cause) {
        Preconditions.checkState(
                state == State.ALLOCATED,
                "SharedSlot (physical request %s) has been released",
                physicalSlotRequestId);
        CompletableFuture<SingleLogicalSlot> logicalSlotFuture =
                requestedLogicalSlots.getValueByKeyA(executionVertexID);
        SlotRequestId logicalSlotRequestId = requestedLogicalSlots.getKeyBByKeyA(executionVertexID);
        if (logicalSlotFuture != null) {
            LOG.debug(
                    "Cancel {} from {}",
                    getLogicalSlotString(logicalSlotRequestId),
                    executionVertexID);
            // If the logicalSlotFuture was not completed and now it fails, the exceptionally
            // callback will also call removeLogicalSlotRequest
            if (cause == null) {
                logicalSlotFuture.cancel(false);
            } else {
                logicalSlotFuture.completeExceptionally(cause);
            }
        } else {
            LOG.debug(
                    "No SlotExecutionVertexAssignment for logical {} from physical {}}",
                    logicalSlotRequestId,
                    physicalSlotRequestId);
        }
    }

    @Override
    public void returnLogicalSlot(LogicalSlot logicalSlot) {
        removeLogicalSlotRequest(logicalSlot.getSlotRequestId());
    }

    private void removeLogicalSlotRequest(SlotRequestId logicalSlotRequestId) {
        LOG.debug("Remove {}", getLogicalSlotString(logicalSlotRequestId));
        Preconditions.checkState(
                requestedLogicalSlots.removeKeyB(logicalSlotRequestId) != null,
                "Trying to remove a logical slot request which has been either already removed or never created.");
        releaseExternally();
    }

    @Override
    public void release(Throwable cause) {
        Preconditions.checkState(
                slotContextFuture.isDone(),
                "Releasing of the shared slot is expected only from its successfully allocated physical slot ({})",
                physicalSlotRequestId);
        LOG.debug("Release shared slot ({})", physicalSlotRequestId);

        // copy the logical slot collection to avoid ConcurrentModificationException
        // if logical slot releases cause cancellation of other executions
        // which will try to call returnLogicalSlot and modify requestedLogicalSlots collection
        Map<ExecutionVertexID, CompletableFuture<SingleLogicalSlot>> logicalSlotFutures =
                requestedLogicalSlots.keySetA().stream()
                        .collect(
                                Collectors.toMap(
                                        executionVertexId -> executionVertexId,
                                        requestedLogicalSlots::getValueByKeyA));
        for (Map.Entry<ExecutionVertexID, CompletableFuture<SingleLogicalSlot>> entry :
                logicalSlotFutures.entrySet()) {
            LOG.debug("Release {}", getLogicalSlotString(entry.getKey()));
            CompletableFuture<SingleLogicalSlot> logicalSlotFuture = entry.getValue();
            Preconditions.checkNotNull(logicalSlotFuture);
            Preconditions.checkState(
                    logicalSlotFuture.isDone(),
                    "Logical slot future must already done when release call comes from the successfully allocated physical slot ({})",
                    physicalSlotRequestId);
            logicalSlotFuture.thenAccept(logicalSlot -> logicalSlot.release(cause));
        }
        requestedLogicalSlots.clear();
        releaseExternally();
    }

    private void releaseExternally() {
        if (state != State.RELEASED && requestedLogicalSlots.values().isEmpty()) {
            state = State.RELEASED;
            LOG.debug("Release shared slot externally ({})", physicalSlotRequestId);
            externalReleaseCallback.accept(executionSlotSharingGroup);
        }
    }

    @Override
    public boolean willOccupySlotIndefinitely() {
        return slotWillBeOccupiedIndefinitely;
    }

    private String getLogicalSlotString(SlotRequestId logicalSlotRequestId) {
        return getLogicalSlotString(
                logicalSlotRequestId, requestedLogicalSlots.getKeyAByKeyB(logicalSlotRequestId));
    }

    private String getLogicalSlotString(ExecutionVertexID executionVertexId) {
        return getLogicalSlotString(
                requestedLogicalSlots.getKeyBByKeyA(executionVertexId), executionVertexId);
    }

    private String getLogicalSlotString(
            SlotRequestId logicalSlotRequestId, ExecutionVertexID executionVertexId) {
        return String.format(
                "logical slot (%s) for execution vertex (id %s) from the physical slot (%s)",
                logicalSlotRequestId, executionVertexId, physicalSlotRequestId);
    }

    /** Returns whether the shared slot has no assigned logical slot requests. */
    boolean isEmpty() {
        return requestedLogicalSlots.size() == 0;
    }

    private enum State {
        ALLOCATED,
        RELEASED
    }
}

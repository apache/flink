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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot;
import org.apache.flink.runtime.util.DualKeyLinkedMap;
import org.apache.flink.util.FlinkException;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A simple implementation of {@link ExecutionSlotAllocator}. No support for slot sharing,
 * co-location, state/input locality, nor local recovery.
 */
public class SimpleExecutionSlotAllocator implements ExecutionSlotAllocator {
    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever;

    private final DualKeyLinkedMap<
                    ExecutionAttemptID, SlotRequestId, CompletableFuture<LogicalSlot>>
            requestedPhysicalSlots;

    SimpleExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            Function<ExecutionAttemptID, ResourceProfile> resourceProfileRetriever,
            boolean slotWillBeOccupiedIndefinitely) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.requestedPhysicalSlots = new DualKeyLinkedMap<>();
    }

    @Override
    public List<ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {
        return executionAttemptIds.stream()
                .map(id -> new ExecutionSlotAssignment(id, allocateSlotFor(id)))
                .collect(Collectors.toList());
    }

    private CompletableFuture<LogicalSlot> allocateSlotFor(ExecutionAttemptID executionAttemptId) {
        if (requestedPhysicalSlots.containsKeyA(executionAttemptId)) {
            return requestedPhysicalSlots.getValueByKeyA(executionAttemptId);
        }
        final SlotRequestId slotRequestId = new SlotRequestId();
        final ResourceProfile resourceProfile = resourceProfileRetriever.apply(executionAttemptId);
        final SlotProfile slotProfile =
                SlotProfile.priorAllocation(
                        resourceProfile,
                        resourceProfile,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptySet());
        final PhysicalSlotRequest request =
                new PhysicalSlotRequest(slotRequestId, slotProfile, slotWillBeOccupiedIndefinitely);
        final CompletableFuture<LogicalSlot> slotFuture =
                slotProvider
                        .allocatePhysicalSlot(request)
                        .thenApply(
                                physicalSlotRequest ->
                                        allocateLogicalSlotFromPhysicalSlot(
                                                slotRequestId,
                                                physicalSlotRequest.getPhysicalSlot(),
                                                slotWillBeOccupiedIndefinitely));
        slotFuture.exceptionally(
                throwable -> {
                    this.requestedPhysicalSlots.removeKeyA(executionAttemptId);
                    this.slotProvider.cancelSlotRequest(slotRequestId, throwable);
                    return null;
                });
        this.requestedPhysicalSlots.put(executionAttemptId, slotRequestId, slotFuture);
        return slotFuture;
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        final CompletableFuture<LogicalSlot> slotFuture =
                this.requestedPhysicalSlots.getValueByKeyA(executionAttemptId);
        if (slotFuture != null) {
            slotFuture.cancel(false);
        }
    }

    private void returnLogicalSlot(LogicalSlot slot) {
        releaseSlot(
                slot,
                new FlinkException("Slot is being returned from SimpleExecutionSlotAllocator."));
    }

    private void releaseSlot(LogicalSlot slot, Throwable cause) {
        requestedPhysicalSlots.removeKeyB(slot.getSlotRequestId());
        slotProvider.cancelSlotRequest(slot.getSlotRequestId(), cause);
    }

    private LogicalSlot allocateLogicalSlotFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleLogicalSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        Locality.UNKNOWN,
                        this::returnLogicalSlot,
                        slotWillBeOccupiedIndefinitely);

        final LogicalSlotHolder logicalSlotHolder = new LogicalSlotHolder(singleLogicalSlot);
        if (physicalSlot.tryAssignPayload(logicalSlotHolder)) {
            return singleLogicalSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    private class LogicalSlotHolder implements PhysicalSlot.Payload {
        private final SingleLogicalSlot logicalSlot;

        private LogicalSlotHolder(SingleLogicalSlot logicalSlot) {
            this.logicalSlot = checkNotNull(logicalSlot);
        }

        @Override
        public void release(Throwable cause) {
            logicalSlot.release(cause);
            releaseSlot(logicalSlot, new FlinkException("Physical slot releases its payload."));
        }

        @Override
        public boolean willOccupySlotIndefinitely() {
            return logicalSlot.willOccupySlotIndefinitely();
        }
    }

    /** Factory to instantiate a {@link SimpleExecutionSlotAllocator}. */
    public static class Factory implements ExecutionSlotAllocatorFactory {
        private final PhysicalSlotProvider slotProvider;

        private final boolean slotWillBeOccupiedIndefinitely;

        public Factory(PhysicalSlotProvider slotProvider, boolean slotWillBeOccupiedIndefinitely) {
            this.slotProvider = slotProvider;
            this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        }

        @Override
        public ExecutionSlotAllocator createInstance(ExecutionSlotAllocationContext context) {
            return new SimpleExecutionSlotAllocator(
                    slotProvider,
                    id -> context.getResourceProfile(id.getExecutionVertexId()),
                    slotWillBeOccupiedIndefinitely);
        }
    }
}

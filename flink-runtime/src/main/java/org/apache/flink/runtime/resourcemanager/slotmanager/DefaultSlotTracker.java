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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/** Default SlotTracker implementation. */
public class DefaultSlotTracker implements SlotTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSlotTracker.class);

    /** Map for all registered slots. */
    private final Map<SlotID, DeclarativeTaskManagerSlot> slots = new HashMap<>();

    /** Index of all currently free slots. */
    private final Map<SlotID, DeclarativeTaskManagerSlot> freeSlots = new LinkedHashMap<>();

    private final MultiSlotStatusUpdateListener slotStatusUpdateListeners =
            new MultiSlotStatusUpdateListener();

    private final SlotStatusStateReconciler slotStatusStateReconciler =
            new SlotStatusStateReconciler(
                    this::transitionSlotToFree,
                    this::transitionSlotToPending,
                    this::transitionSlotToAllocated);

    @Override
    public void registerSlotStatusUpdateListener(
            SlotStatusUpdateListener slotStatusUpdateListener) {
        this.slotStatusUpdateListeners.registerSlotStatusUpdateListener(slotStatusUpdateListener);
    }

    @Override
    public void addSlot(
            SlotID slotId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection,
            @Nullable JobID assignedJob) {
        Preconditions.checkNotNull(slotId);
        Preconditions.checkNotNull(resourceProfile);
        Preconditions.checkNotNull(taskManagerConnection);

        if (slots.containsKey(slotId)) {
            // remove the old slot first
            LOG.debug(
                    "A slot was added with an already tracked slot ID {}. Removing previous entry.",
                    slotId);
            removeSlot(slotId);
        }

        DeclarativeTaskManagerSlot slot =
                new DeclarativeTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);
        slots.put(slotId, slot);
        freeSlots.put(slotId, slot);
        slotStatusStateReconciler.executeStateTransition(slot, assignedJob);
    }

    @Override
    public void removeSlots(Iterable<SlotID> slotsToRemove) {
        Preconditions.checkNotNull(slotsToRemove);

        for (SlotID slotId : slotsToRemove) {
            removeSlot(slotId);
        }
    }

    private void removeSlot(SlotID slotId) {
        DeclarativeTaskManagerSlot slot = slots.remove(slotId);

        if (slot != null) {
            if (slot.getState() != SlotState.FREE) {
                transitionSlotToFree(slot);
            }
            freeSlots.remove(slotId);
        } else {
            LOG.debug("There was no slot registered with slot id {}.", slotId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // ResourceManager slot status API - optimistically trigger transitions, but they may not
    // represent true state on task executors
    // ---------------------------------------------------------------------------------------------

    @Override
    public void notifyFree(SlotID slotId) {
        Preconditions.checkNotNull(slotId);
        transitionSlotToFree(slots.get(slotId));
    }

    @Override
    public void notifyAllocationStart(SlotID slotId, JobID jobId) {
        Preconditions.checkNotNull(slotId);
        Preconditions.checkNotNull(jobId);
        transitionSlotToPending(slots.get(slotId), jobId);
    }

    @Override
    public void notifyAllocationComplete(SlotID slotId, JobID jobId) {
        Preconditions.checkNotNull(slotId);
        Preconditions.checkNotNull(jobId);
        transitionSlotToAllocated(slots.get(slotId), jobId);
    }

    // ---------------------------------------------------------------------------------------------
    // TaskExecutor slot status API - acts as source of truth
    // ---------------------------------------------------------------------------------------------

    @Override
    public boolean notifySlotStatus(Iterable<SlotStatus> slotStatuses) {
        Preconditions.checkNotNull(slotStatuses);
        boolean anyStatusChanged = false;
        for (SlotStatus slotStatus : slotStatuses) {
            anyStatusChanged |=
                    slotStatusStateReconciler.executeStateTransition(
                            slots.get(slotStatus.getSlotID()), slotStatus.getJobID());
        }
        return anyStatusChanged;
    }

    // ---------------------------------------------------------------------------------------------
    // Core state transitions
    // ---------------------------------------------------------------------------------------------

    private void transitionSlotToFree(DeclarativeTaskManagerSlot slot) {
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(slot.getState() != SlotState.FREE);

        // remember the slots current job and state for the notification, as this information will
        // be cleared from
        // the slot upon freeing
        final JobID jobId = slot.getJobId();
        final SlotState state = slot.getState();

        slot.freeSlot();
        freeSlots.put(slot.getSlotId(), slot);
        slotStatusUpdateListeners.notifySlotStatusChange(slot, state, SlotState.FREE, jobId);
    }

    private void transitionSlotToPending(DeclarativeTaskManagerSlot slot, JobID jobId) {
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(slot.getState() == SlotState.FREE);

        slot.startAllocation(jobId);
        freeSlots.remove(slot.getSlotId());
        slotStatusUpdateListeners.notifySlotStatusChange(
                slot, SlotState.FREE, SlotState.PENDING, jobId);
    }

    private void transitionSlotToAllocated(DeclarativeTaskManagerSlot slot, JobID jobId) {
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(
                jobId.equals(slot.getJobId()),
                "Job ID from slot status updated (%s) does not match currently assigned job ID (%s) for slot %s.",
                jobId,
                slot.getJobId(),
                slot.getSlotId());
        Preconditions.checkState(
                slot.getState() == SlotState.PENDING,
                "State of slot %s must be %s, but was %s.",
                slot.getSlotId(),
                SlotState.PENDING,
                slot.getState());

        slot.completeAllocation();
        slotStatusUpdateListeners.notifySlotStatusChange(
                slot, SlotState.PENDING, SlotState.ALLOCATED, jobId);
    }

    // ---------------------------------------------------------------------------------------------
    // Misc
    // ---------------------------------------------------------------------------------------------

    @Override
    public Collection<TaskManagerSlotInformation> getFreeSlots() {
        return Collections.unmodifiableCollection(freeSlots.values());
    }

    @VisibleForTesting
    boolean areMapsEmpty() {
        return slots.isEmpty() && freeSlots.isEmpty();
    }

    @VisibleForTesting
    @Nullable
    DeclarativeTaskManagerSlot getSlot(SlotID slotId) {
        return slots.get(slotId);
    }

    /**
     * Slot reports from task executor are the source of truth regarding the state of slots. The
     * reported state may not match what is currently being tracked, and if so can contain illegal
     * transitions (e.g., from free to allocated). The tracked and reported states are reconciled by
     * simulating state transitions that lead us from our currently tracked state to the actual
     * reported state.
     *
     * <p>One exception to the reported state being the source of truth are slots reported as being
     * free, but tracked as being pending. This mismatch is assumed to be due to a slot allocation
     * RPC not yet having been process by the task executor. This mismatch is hence ignored; it will
     * be resolved eventually with the allocation either being completed or timing out.
     */
    @VisibleForTesting
    static class SlotStatusStateReconciler {
        private final Consumer<DeclarativeTaskManagerSlot> toFreeSlot;
        private final BiConsumer<DeclarativeTaskManagerSlot, JobID> toPendingSlot;
        private final BiConsumer<DeclarativeTaskManagerSlot, JobID> toAllocatedSlot;

        @VisibleForTesting
        SlotStatusStateReconciler(
                Consumer<DeclarativeTaskManagerSlot> toFreeSlot,
                BiConsumer<DeclarativeTaskManagerSlot, JobID> toPendingSlot,
                BiConsumer<DeclarativeTaskManagerSlot, JobID> toAllocatedSlot) {
            this.toFreeSlot = toFreeSlot;
            this.toPendingSlot = toPendingSlot;
            this.toAllocatedSlot = toAllocatedSlot;
        }

        public boolean executeStateTransition(DeclarativeTaskManagerSlot slot, JobID jobId) {
            final SlotState reportedSlotState =
                    jobId == null ? SlotState.FREE : SlotState.ALLOCATED;
            final SlotState trackedSlotState = slot.getState();

            if (reportedSlotState == SlotState.FREE) {
                switch (trackedSlotState) {
                    case FREE:
                        // matching state
                        return false;
                    case PENDING:
                        // don't do anything because we expect the slot to be allocated soon
                        return false;
                    case ALLOCATED:
                        toFreeSlot.accept(slot);
                        return true;
                }
            } else {
                switch (trackedSlotState) {
                    case FREE:
                        toPendingSlot.accept(slot, jobId);
                        toAllocatedSlot.accept(slot, jobId);
                        return true;
                    case PENDING:
                        if (!jobId.equals(slot.getJobId())) {
                            toFreeSlot.accept(slot);
                            toPendingSlot.accept(slot, jobId);
                        }
                        toAllocatedSlot.accept(slot, jobId);
                        return true;
                    case ALLOCATED:
                        if (!jobId.equals(slot.getJobId())) {
                            toFreeSlot.accept(slot);
                            toPendingSlot.accept(slot, jobId);
                            toAllocatedSlot.accept(slot, jobId);
                            return true;
                        } else {
                            // matching state
                            return false;
                        }
                }
            }
            return false;
        }
    }

    private static class MultiSlotStatusUpdateListener implements SlotStatusUpdateListener {

        private final Collection<SlotStatusUpdateListener> listeners = new ArrayList<>();

        public void registerSlotStatusUpdateListener(
                SlotStatusUpdateListener slotStatusUpdateListener) {
            listeners.add(slotStatusUpdateListener);
        }

        @Override
        public void notifySlotStatusChange(
                TaskManagerSlotInformation slot,
                SlotState previous,
                SlotState current,
                JobID jobId) {
            LOG.trace(
                    "Slot {} transitioned from {} to {} for job {}.",
                    slot.getSlotId(),
                    previous,
                    current,
                    jobId);
            listeners.forEach(
                    listeners -> listeners.notifySlotStatusChange(slot, previous, current, jobId));
        }
    }
}

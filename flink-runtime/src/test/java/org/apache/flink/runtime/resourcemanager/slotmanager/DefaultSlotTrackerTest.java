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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DefaultSlotTracker}. */
class DefaultSlotTrackerTest {

    private static final TaskExecutorConnection TASK_EXECUTOR_CONNECTION =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    private static final JobID jobId = new JobID();

    @Test
    public void testFreeSlotsIsEmptyOnInitially() {
        SlotTracker tracker = new DefaultSlotTracker();

        assertThat(tracker.getFreeSlots()).isEmpty();
    }

    @Test
    public void testSlotAddition() {
        SlotTracker tracker = new DefaultSlotTracker();

        SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
        SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);

        tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        assertThat(tracker.getFreeSlots().stream().map(TaskManagerSlotInformation::getSlotId))
                .containsExactlyInAnyOrder(slotId1, slotId2);
    }

    @Test
    public void testSlotRemoval() {
        Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
        DefaultSlotTracker tracker = new DefaultSlotTracker();
        tracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(
                                new SlotStateTransition(
                                        slot.getSlotId(), previous, current, jobId)));

        SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
        SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);

        tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        tracker.addSlot(slotId3, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        tracker.notifyAllocationStart(slotId2, jobId);
        tracker.notifyAllocationStart(slotId3, jobId);
        tracker.notifyAllocationComplete(slotId3, jobId);

        // the transitions to this point are not relevant for this test
        stateTransitions.clear();
        // we now have 1 slot in each slot state (free, pending, allocated)
        // it should be possible to remove slots regardless of their state
        tracker.removeSlots(Arrays.asList(slotId1, slotId2, slotId3));

        assertThat(tracker.getFreeSlots()).isEmpty();
        assertThat(tracker.areMapsEmpty()).isTrue();

        assertThat(stateTransitions)
                .containsExactlyInAnyOrder(
                        new SlotStateTransition(slotId2, SlotState.PENDING, SlotState.FREE, jobId),
                        new SlotStateTransition(
                                slotId3, SlotState.ALLOCATED, SlotState.FREE, jobId));
    }

    @Test
    public void testAllocationCompletion() {
        Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
        SlotTracker tracker = new DefaultSlotTracker();
        tracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(
                                new SlotStateTransition(
                                        slot.getSlotId(), previous, current, jobId)));

        SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

        tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        tracker.notifyAllocationStart(slotId, jobId);
        assertThat(tracker.getFreeSlots()).isEmpty();
        assertThat(stateTransitions.remove())
                .isEqualTo(
                        new SlotStateTransition(slotId, SlotState.FREE, SlotState.PENDING, jobId));

        tracker.notifyAllocationComplete(slotId, jobId);
        assertThat(tracker.getFreeSlots()).isEmpty();
        assertThat(stateTransitions.remove())
                .isEqualTo(
                        new SlotStateTransition(
                                slotId, SlotState.PENDING, SlotState.ALLOCATED, jobId));

        tracker.notifyFree(slotId);

        assertThat(tracker.getFreeSlots().stream().map(TaskManagerSlotInformation::getSlotId))
                .contains(slotId);
        assertThat(stateTransitions.remove())
                .isEqualTo(
                        new SlotStateTransition(
                                slotId, SlotState.ALLOCATED, SlotState.FREE, jobId));
    }

    @Test
    public void testAllocationCompletionForDifferentJobThrowsIllegalStateException() {
        SlotTracker tracker = new DefaultSlotTracker();

        SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

        tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        tracker.notifyAllocationStart(slotId, new JobID());
        assertThatThrownBy(() -> tracker.notifyAllocationComplete(slotId, new JobID()))
                .withFailMessage("Allocations must not be completed for a different job ID.")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testAllocationCancellation() {
        Queue<SlotStateTransition> stateTransitions = new ArrayDeque<>();
        SlotTracker tracker = new DefaultSlotTracker();
        tracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) ->
                        stateTransitions.add(
                                new SlotStateTransition(
                                        slot.getSlotId(), previous, current, jobId)));

        SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

        tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        tracker.notifyAllocationStart(slotId, jobId);
        assertThat(tracker.getFreeSlots()).isEmpty();
        assertThat(stateTransitions.remove())
                .isEqualTo(
                        new SlotStateTransition(slotId, SlotState.FREE, SlotState.PENDING, jobId));

        tracker.notifyFree(slotId);
        assertThat(tracker.getFreeSlots().stream().map(TaskManagerSlotInformation::getSlotId))
                .contains(slotId);
        assertThat(stateTransitions.remove())
                .isEqualTo(
                        new SlotStateTransition(slotId, SlotState.PENDING, SlotState.FREE, jobId));
    }

    /**
     * Tests that notifications are fired before the internal state transition has been executed, to
     * ensure that components reacting to the status update are in a consistent state with the
     * tracker. Note that this test is not conclusive for transitions from PENDING to ALLOCATED, but
     * that's okay for now because this distinction isn't exposed anywhere in the API.
     */
    @Test
    public void testNotificationsFiredAfterStateTransition() {
        SlotID slotId = new SlotID(ResourceID.generate(), 0);

        DefaultSlotTracker tracker = new DefaultSlotTracker();
        tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);

        tracker.registerSlotStatusUpdateListener(
                (slot, previous, current, jobId) -> {
                    if (current == SlotState.FREE) {
                        assertThat(
                                        tracker.getFreeSlots().stream()
                                                .map(TaskManagerSlotInformation::getSlotId))
                                .contains(slotId);
                    } else {
                        assertThat(
                                        tracker.getFreeSlots().stream()
                                                .map(TaskManagerSlotInformation::getSlotId))
                                .doesNotContain(slotId);
                    }
                });

        tracker.notifyAllocationStart(slotId, jobId);
        tracker.notifyAllocationComplete(slotId, jobId);
        tracker.notifyFree(slotId);
    }

    @Test
    public void testSlotStatusProcessing() {
        SlotTracker tracker = new DefaultSlotTracker();
        SlotID slotId1 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);
        SlotID slotId2 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 1);
        SlotID slotId3 = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 2);
        tracker.addSlot(slotId1, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        tracker.addSlot(slotId2, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        tracker.addSlot(slotId3, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, jobId);

        assertThat(tracker.getFreeSlots().stream().map(TaskManagerSlotInformation::getSlotId))
                .containsExactlyInAnyOrder(slotId1, slotId2);

        // move slot2 to PENDING
        tracker.notifyAllocationStart(slotId2, jobId);

        final List<SlotStatus> slotReport =
                Arrays.asList(
                        new SlotStatus(
                                slotId1,
                                ResourceProfile.ANY,
                                jobId,
                                new AllocationID(),
                                LoadingWeight.EMPTY),
                        new SlotStatus(
                                slotId2,
                                ResourceProfile.ANY,
                                null,
                                new AllocationID(),
                                LoadingWeight.EMPTY),
                        new SlotStatus(
                                slotId3,
                                ResourceProfile.ANY,
                                null,
                                new AllocationID(),
                                LoadingWeight.EMPTY));

        assertThat(tracker.notifySlotStatus(slotReport)).isTrue();

        // slot1 should now be allocated; slot2 should continue to be in a pending state; slot3
        // should be freed
        assertThat(tracker.getFreeSlots().stream().map(TaskManagerSlotInformation::getSlotId))
                .contains(slotId3);

        // if slot2 is not in a pending state, this will fail with an exception
        tracker.notifyAllocationComplete(slotId2, jobId);

        final List<SlotStatus> idempotentSlotReport =
                Arrays.asList(
                        new SlotStatus(
                                slotId1,
                                ResourceProfile.ANY,
                                jobId,
                                new AllocationID(),
                                LoadingWeight.EMPTY),
                        new SlotStatus(
                                slotId2,
                                ResourceProfile.ANY,
                                jobId,
                                new AllocationID(),
                                LoadingWeight.EMPTY),
                        new SlotStatus(
                                slotId3,
                                ResourceProfile.ANY,
                                null,
                                new AllocationID(),
                                LoadingWeight.EMPTY));

        assertThat(tracker.notifySlotStatus(idempotentSlotReport)).isFalse();
    }

    @Test
    public void testGetTaskExecutorsWithAllocatedSlotsForJob() {
        final SlotTracker tracker = new DefaultSlotTracker();

        final JobID jobId = new JobID();
        final SlotID slotId = new SlotID(TASK_EXECUTOR_CONNECTION.getResourceID(), 0);

        assertThat(tracker.getTaskExecutorsWithAllocatedSlotsForJob(new JobID())).isEmpty();

        tracker.addSlot(slotId, ResourceProfile.ANY, TASK_EXECUTOR_CONNECTION, null);
        assertThat(tracker.getTaskExecutorsWithAllocatedSlotsForJob(new JobID())).isEmpty();

        tracker.notifyAllocationStart(slotId, jobId);
        assertThat(tracker.getTaskExecutorsWithAllocatedSlotsForJob(jobId))
                .contains(TASK_EXECUTOR_CONNECTION);

        tracker.notifyAllocationComplete(slotId, jobId);
        assertThat(tracker.getTaskExecutorsWithAllocatedSlotsForJob(jobId))
                .contains(TASK_EXECUTOR_CONNECTION);

        tracker.notifyFree(slotId);
        assertThat(tracker.getTaskExecutorsWithAllocatedSlotsForJob(new JobID())).isEmpty();
    }

    private static class SlotStateTransition {

        private final SlotID slotId;
        private final SlotState oldState;
        private final SlotState newState;
        @Nullable private final JobID jobId;

        private SlotStateTransition(
                SlotID slotId, SlotState oldState, SlotState newState, @Nullable JobID jobId) {
            this.slotId = slotId;
            this.jobId = jobId;
            this.oldState = oldState;
            this.newState = newState;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotStateTransition that = (SlotStateTransition) o;
            return Objects.equals(slotId, that.slotId)
                    && oldState == that.oldState
                    && newState == that.newState
                    && Objects.equals(jobId, that.jobId);
        }

        @Override
        public String toString() {
            return "SlotStateTransition{"
                    + "slotId="
                    + slotId
                    + ", oldState="
                    + oldState
                    + ", newState="
                    + newState
                    + ", jobId="
                    + jobId
                    + '}';
        }
    }
}

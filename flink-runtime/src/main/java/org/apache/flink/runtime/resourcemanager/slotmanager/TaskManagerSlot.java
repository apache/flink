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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A TaskManagerSlot represents a slot located in a TaskManager. It has a unique identification and
 * resource profile associated.
 */
public class TaskManagerSlot implements TaskManagerSlotInformation {

    /** The unique identification of this slot. */
    private final SlotID slotId;

    /** The resource profile of this slot. */
    private final ResourceProfile resourceProfile;

    /** Gateway to the TaskExecutor which owns the slot. */
    private final TaskExecutorConnection taskManagerConnection;

    /** Allocation id for which this slot has been allocated. */
    @Nullable private AllocationID allocationId;

    /** Job id for which this slot has been allocated. */
    @Nullable private JobID jobId;

    /** Assigned slot request if there is currently an ongoing request. */
    private PendingSlotRequest assignedSlotRequest;

    private SlotState state;

    public TaskManagerSlot(
            SlotID slotId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection) {
        this.slotId = checkNotNull(slotId);
        this.resourceProfile = checkNotNull(resourceProfile);
        this.taskManagerConnection = checkNotNull(taskManagerConnection);

        this.state = SlotState.FREE;
        this.allocationId = null;
        this.assignedSlotRequest = null;
    }

    @Override
    public SlotState getState() {
        return state;
    }

    @Override
    public SlotID getSlotId() {
        return slotId;
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Override
    public TaskExecutorConnection getTaskManagerConnection() {
        return taskManagerConnection;
    }

    @Nullable
    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    @Nullable
    @Override
    public JobID getJobId() {
        return jobId;
    }

    public PendingSlotRequest getAssignedSlotRequest() {
        return assignedSlotRequest;
    }

    @Override
    public InstanceID getInstanceId() {
        return taskManagerConnection.getInstanceID();
    }

    public void freeSlot() {
        Preconditions.checkState(
                state == SlotState.ALLOCATED, "Slot must be allocated before freeing it.");

        state = SlotState.FREE;
        allocationId = null;
        jobId = null;
    }

    public void clearPendingSlotRequest() {
        Preconditions.checkState(state == SlotState.PENDING, "No slot request to clear.");

        state = SlotState.FREE;
        assignedSlotRequest = null;
    }

    public void assignPendingSlotRequest(PendingSlotRequest pendingSlotRequest) {
        Preconditions.checkState(
                state == SlotState.FREE, "Slot must be free to be assigned a slot request.");

        state = SlotState.PENDING;
        assignedSlotRequest = Preconditions.checkNotNull(pendingSlotRequest);
    }

    public void completeAllocation(AllocationID allocationId, JobID jobId) {
        Preconditions.checkNotNull(allocationId, "Allocation id must not be null.");
        Preconditions.checkNotNull(jobId, "Job id must not be null.");
        Preconditions.checkState(
                state == SlotState.PENDING,
                "In order to complete an allocation, the slot has to be allocated.");
        Preconditions.checkState(
                Objects.equals(allocationId, assignedSlotRequest.getAllocationId()),
                "Mismatch between allocation id of the pending slot request.");

        state = SlotState.ALLOCATED;
        this.allocationId = allocationId;
        this.jobId = jobId;
        assignedSlotRequest = null;
    }

    public void updateAllocation(AllocationID allocationId, JobID jobId) {
        Preconditions.checkState(
                state == SlotState.FREE,
                "The slot has to be free in order to set an allocation id.");

        state = SlotState.ALLOCATED;
        this.allocationId = Preconditions.checkNotNull(allocationId);
        this.jobId = Preconditions.checkNotNull(jobId);
    }
}

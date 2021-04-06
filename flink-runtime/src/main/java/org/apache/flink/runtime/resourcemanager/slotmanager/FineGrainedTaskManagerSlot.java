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

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A FineGrainedTaskManagerSlot represents a slot located in a TaskManager. It maintains states of
 * the slot needed in {@link FineGrainedSlotManager}.
 *
 * <p>Note that it should not in the state of {@link SlotState#FREE}.
 */
public class FineGrainedTaskManagerSlot implements TaskManagerSlotInformation {
    /** The resource profile of this slot. */
    private final ResourceProfile resourceProfile;

    /** Gateway to the TaskExecutor which owns the slot. */
    private final TaskExecutorConnection taskManagerConnection;

    /** Allocation id for which this slot has been allocated. */
    private final AllocationID allocationId;

    /** Job id for which this slot has been allocated. */
    private final JobID jobId;

    /** Current state of this slot. Should be either PENDING or ALLOCATED. */
    private SlotState state;

    public FineGrainedTaskManagerSlot(
            AllocationID allocationId,
            JobID jobId,
            ResourceProfile resourceProfile,
            TaskExecutorConnection taskManagerConnection,
            SlotState slotState) {
        this.resourceProfile = checkNotNull(resourceProfile);
        this.taskManagerConnection = checkNotNull(taskManagerConnection);
        this.allocationId = checkNotNull(allocationId);
        this.jobId = checkNotNull(jobId);
        this.state = checkNotNull(slotState);
        checkArgument(
                !slotState.equals(SlotState.FREE),
                "The slot of fine-grained resource management should be dynamically created in allocation. Thus it should not in FREE state.");
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Override
    public SlotState getState() {
        return state;
    }

    @Override
    public JobID getJobId() {
        return jobId;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    @Override
    public SlotID getSlotId() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InstanceID getInstanceId() {
        return taskManagerConnection.getInstanceID();
    }

    @Override
    public TaskExecutorConnection getTaskManagerConnection() {
        return taskManagerConnection;
    }

    public void completeAllocation() {
        Preconditions.checkState(
                state == SlotState.PENDING,
                "In order to complete an allocation, the slot has to be allocated.");

        state = SlotState.ALLOCATED;
    }
}

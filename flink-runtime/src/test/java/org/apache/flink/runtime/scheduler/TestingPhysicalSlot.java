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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

/** An implementing for the {@link PhysicalSlot} interface for testing purposes. */
public class TestingPhysicalSlot implements PhysicalSlot {

    private final AllocationID allocationId;

    private final TaskManagerLocation taskManagerLocation;

    private final int physicalSlotNumber;

    private final TaskManagerGateway taskManagerGateway;

    private final ResourceProfile resourceProfile;

    @Nullable private Payload payload;

    TestingPhysicalSlot(ResourceProfile resourceProfile, AllocationID allocationId) {
        this(
                allocationId,
                new LocalTaskManagerLocation(),
                0,
                new SimpleAckingTaskManagerGateway(),
                resourceProfile);
    }

    TestingPhysicalSlot(
            AllocationID allocationId,
            TaskManagerLocation taskManagerLocation,
            int physicalSlotNumber,
            TaskManagerGateway taskManagerGateway,
            ResourceProfile resourceProfile) {
        this.allocationId = Preconditions.checkNotNull(allocationId);
        this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
        this.physicalSlotNumber = physicalSlotNumber;
        this.taskManagerGateway = Preconditions.checkNotNull(taskManagerGateway);
        this.resourceProfile = resourceProfile;
    }

    @Override
    public AllocationID getAllocationId() {
        return allocationId;
    }

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return taskManagerLocation;
    }

    @Override
    public int getPhysicalSlotNumber() {
        return physicalSlotNumber;
    }

    @Override
    public TaskManagerGateway getTaskManagerGateway() {
        return taskManagerGateway;
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    @Override
    public boolean willBeOccupiedIndefinitely() {
        return true;
    }

    @Override
    public boolean tryAssignPayload(Payload payload) {
        if (this.payload != null) {
            return false;
        }
        this.payload = payload;
        return true;
    }

    @Nullable
    public Payload getPayload() {
        return payload;
    }

    public void releasePayload(Throwable cause) {
        if (payload != null) {
            payload.release(cause);
            payload = null;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** {@code Builder} for creating {@code TestingPhysicalSlot} instances. */
    public static class Builder {

        private AllocationID allocationID = new AllocationID();
        private TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        private int physicalSlotNumber = 0;
        private TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
        private ResourceProfile resourceProfile = ResourceProfile.ANY;

        private Builder() {}

        public Builder withAllocationID(AllocationID allocationID) {
            this.allocationID = allocationID;
            return this;
        }

        public Builder withTaskManagerLocation(TaskManagerLocation taskManagerLocation) {
            this.taskManagerLocation = taskManagerLocation;
            return this;
        }

        public Builder withPhysicalSlotNumber(int physicalSlotNumber) {
            this.physicalSlotNumber = physicalSlotNumber;
            return this;
        }

        public Builder withTaskManagerGateway(TaskManagerGateway taskManagerGateway) {
            this.taskManagerGateway = taskManagerGateway;
            return this;
        }

        public Builder withResourceProfile(ResourceProfile resourceProfile) {
            this.resourceProfile = resourceProfile;
            return this;
        }

        public TestingPhysicalSlot build() {
            return new TestingPhysicalSlot(
                    allocationID,
                    taskManagerLocation,
                    physicalSlotNumber,
                    taskManagerGateway,
                    resourceProfile);
        }
    }
}

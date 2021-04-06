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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;

import javax.annotation.Nullable;

/** Testing implementation of {@link TaskManagerSlotInformation}. */
public final class TestingTaskManagerSlotInformation implements TaskManagerSlotInformation {

    private final SlotID slotId;
    private final InstanceID instanceId;
    @Nullable private final AllocationID allocationId;
    @Nullable private final JobID jobId;
    private final ResourceProfile resourceProfile;
    private final SlotState state;
    private final TaskExecutorConnection taskExecutorConnection =
            new TaskExecutorConnection(
                    ResourceID.generate(),
                    new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

    private TestingTaskManagerSlotInformation(
            SlotID slotId,
            @Nullable AllocationID allocationId,
            @Nullable JobID jobId,
            InstanceID instanceId,
            ResourceProfile resourceProfile,
            SlotState state) {
        this.slotId = slotId;
        this.allocationId = allocationId;
        this.jobId = jobId;
        this.instanceId = instanceId;
        this.resourceProfile = resourceProfile;
        this.state = state;
    }

    @Override
    public SlotID getSlotId() {
        return slotId;
    }

    @Override
    @Nullable
    public JobID getJobId() {
        return jobId;
    }

    @Override
    @Nullable
    public AllocationID getAllocationId() {
        return allocationId;
    }

    @Override
    public SlotState getState() {
        return state;
    }

    @Override
    public InstanceID getInstanceId() {
        return instanceId;
    }

    @Override
    public TaskExecutorConnection getTaskManagerConnection() {
        return taskExecutorConnection;
    }

    @Override
    public boolean isMatchingRequirement(ResourceProfile required) {
        return resourceProfile.isMatching(required);
    }

    @Override
    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    static class Builder {
        private SlotID slotId = new SlotID(ResourceID.generate(), 0);
        private AllocationID allocationId = new AllocationID();
        private JobID jobId = new JobID();
        private InstanceID instanceId = new InstanceID();
        private ResourceProfile resourceProfile = ResourceProfile.ANY;
        private SlotState state = SlotState.FREE;

        public Builder setState(SlotState state) {
            this.state = state;
            return this;
        }

        public Builder setInstanceId(InstanceID instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder setResourceProfile(ResourceProfile resourceProfile) {
            this.resourceProfile = resourceProfile;
            return this;
        }

        public Builder setSlotId(SlotID slotId) {
            this.slotId = slotId;
            return this;
        }

        public Builder setAllocationId(AllocationID allocationId) {
            this.allocationId = allocationId;
            return this;
        }

        public Builder setJobId(JobID jobId) {
            this.jobId = jobId;
            return this;
        }

        public TestingTaskManagerSlotInformation build() {
            return new TestingTaskManagerSlotInformation(
                    slotId, allocationId, jobId, instanceId, resourceProfile, state);
        }
    }
}

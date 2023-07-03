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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.Map;

/** Implementation of {@link TaskManagerInfo} for testing purpose. */
public class TestingTaskManagerInfo implements TaskManagerInfo {
    private final TaskExecutorConnection taskExecutorConnection;
    private final ResourceProfile totalResource;
    private final ResourceProfile availableResource;
    private final ResourceProfile defaultSlotResourceProfile;
    private final int defaultNumSlots;

    private long idleSince = Long.MAX_VALUE;

    public TestingTaskManagerInfo(
            ResourceProfile totalResource,
            ResourceProfile availableResource,
            ResourceProfile defaultSlotResourceProfile) {
        this.totalResource = Preconditions.checkNotNull(totalResource);
        this.availableResource = Preconditions.checkNotNull(availableResource);
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);

        this.defaultNumSlots =
                SlotManagerUtils.calculateDefaultNumSlots(
                        totalResource, defaultSlotResourceProfile);
        this.taskExecutorConnection =
                new TaskExecutorConnection(
                        ResourceID.generate(),
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());
    }

    public void setIdleSince(long idleSince) {
        this.idleSince = idleSince;
    }

    @Override
    public InstanceID getInstanceId() {
        return taskExecutorConnection.getInstanceID();
    }

    @Override
    public TaskExecutorConnection getTaskExecutorConnection() {
        return taskExecutorConnection;
    }

    @Override
    public Map<AllocationID, TaskManagerSlotInformation> getAllocatedSlots() {
        return Collections.emptyMap();
    }

    @Override
    public ResourceProfile getAvailableResource() {
        return availableResource;
    }

    @Override
    public ResourceProfile getTotalResource() {
        return totalResource;
    }

    @Override
    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    @Override
    public int getDefaultNumSlots() {
        return defaultNumSlots;
    }

    @Override
    public long getIdleSince() {
        return idleSince;
    }

    @Override
    public boolean isIdle() {
        return idleSince != Long.MAX_VALUE;
    }
}

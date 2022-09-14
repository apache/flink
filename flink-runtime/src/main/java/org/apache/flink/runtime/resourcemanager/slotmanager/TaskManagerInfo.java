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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;

import java.util.Map;

/** Information of a TaskManager needed in {@link SlotManager}. */
public interface TaskManagerInfo {

    /**
     * Get the instanceId of this task manager.
     *
     * @return the instanceId
     */
    InstanceID getInstanceId();

    /**
     * Get the taskExecutorConnection.
     *
     * @return the taskExecutorConnection
     */
    TaskExecutorConnection getTaskExecutorConnection();

    /**
     * Get allocated slots information.
     *
     * @return allocated slots information mapped by its allocationId
     */
    Map<AllocationID, TaskManagerSlotInformation> getAllocatedSlots();

    /**
     * Get the available resource.
     *
     * @return the available resource
     */
    ResourceProfile getAvailableResource();

    /**
     * Get the total resource.
     *
     * @return the total resource
     */
    ResourceProfile getTotalResource();

    /**
     * Get the default slot resource profile.
     *
     * @return the default slot resource profile
     */
    ResourceProfile getDefaultSlotResourceProfile();

    /**
     * Get the default number of slots.
     *
     * @return the default number of slots
     */
    int getDefaultNumSlots();

    /**
     * Get the timestamp when the last time becoming idle.
     *
     * @return the timestamp when the last time becoming idle
     */
    long getIdleSince();

    /**
     * Check whether this task manager is idle.
     *
     * @return whether this task manager is idle
     */
    boolean isIdle();
}

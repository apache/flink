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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;

import java.util.Collection;
import java.util.Optional;

/** Provide the information of TaskManager's resource and slot status. */
interface TaskManagerResourceInfoProvider {

    /**
     * Get the {@link TaskManagerInfo}s of all registered task managers.
     *
     * @return a collection of {@link TaskManagerInfo}s of all registered task managers.
     */
    Collection<? extends TaskManagerInfo> getRegisteredTaskManagers();

    /**
     * Get the {@link TaskManagerInfo} of a registered task manager with the given instanceId
     *
     * @param instanceId of the task manager
     * @return An Optional of {@link TaskManagerInfo}, if find, of the task manager
     */
    Optional<TaskManagerInfo> getRegisteredTaskManager(InstanceID instanceId);

    /**
     * Get all pending task managers.
     *
     * @return a collection of {@link PendingTaskManager}s.
     */
    Collection<PendingTaskManager> getPendingTaskManagers();

    /**
     * Get the {@link TaskManagerSlotInformation} of the allocated slot with the given allocationId.
     *
     * @param allocationId of the slot
     * @return An Optional of {@link TaskManagerSlotInformation}, if find, of the slot
     */
    Optional<TaskManagerSlotInformation> getAllocatedOrPendingSlot(AllocationID allocationId);

    /**
     * Get all pending task managers with given total and default slot profile.
     *
     * @param totalResourceProfile of the pending task manager
     * @param defaultSlotResourceProfile of the pending task manager
     * @return Collection of all matching pending task managers
     */
    Collection<PendingTaskManager> getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile);
}

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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;

/**
 * Bookkeeps total/allocated/available resources TaskExecutors and pending TaskExecutors.
 */
public class TaskExecutorResourceBookkeeper {

	private final HashMap<ResourceID, TaskExecutorResource<SlotID>> taskExecutorResources;
	private final HashMap<PendingTaskManagerId, TaskExecutorResource<TaskManagerSlotId>> pendingTaskExecutorResources;

	public TaskExecutorResourceBookkeeper() {
		taskExecutorResources = new HashMap<>();
		pendingTaskExecutorResources = new HashMap<>();
	}

	public void addResource(ResourceID resourceID, ResourceProfile totalResourceProfile) {
		taskExecutorResources.put(resourceID, new TaskExecutorResource<>(totalResourceProfile));
	}

	public void addResource(PendingTaskManagerId pendingTaskManagerId, ResourceProfile totalResourceProfile) {
		pendingTaskExecutorResources.put(pendingTaskManagerId, new TaskExecutorResource<>(totalResourceProfile));
	}

	public void removeResource(ResourceID resourceID) {
		taskExecutorResources.remove(resourceID);
	}

	public void removeResource(PendingTaskManagerId pendingTaskManagerId) {
		pendingTaskExecutorResources.remove(pendingTaskManagerId);
	}

	public boolean hasEnoughResource(TaskManagerSlot slot, ResourceProfile resourceProfile) {
		ResourceID resourceID = slot.getSlotId().getResourceID();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(taskExecutorResources.get(resourceID));

		ResourceProfile requestedResourceProfile = resourceProfile.equals(ResourceProfile.UNKNOWN) ?
			slot.getResourceProfile() : resourceProfile;
		ResourceProfile availableResourceProfile = taskExecutorResource.getAvailableResource();
		return availableResourceProfile.isMatching(requestedResourceProfile);
	}

	public boolean hasEnoughResource(PendingTaskManagerSlot pendingSlot, ResourceProfile resourceProfile) {
		PendingTaskManagerId pendingTaskManagerId = pendingSlot.getPendingTaskManagerId();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(pendingTaskExecutorResources.get(pendingTaskManagerId));

		ResourceProfile requestedResourceProfile = resourceProfile.equals(ResourceProfile.UNKNOWN) ?
			pendingSlot.getResourceProfile() : resourceProfile;
		ResourceProfile availableResourceProfile = taskExecutorResource.getAvailableResource();
		return availableResourceProfile.isMatching(requestedResourceProfile);
	}

	public void recordAllocatedResource(TaskManagerSlot slot, ResourceProfile resourceProfile) {
		ResourceID resourceID = slot.getSlotId().getResourceID();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(taskExecutorResources.get(resourceID));

		ResourceProfile allocatedResourceProfile = resourceProfile.equals(ResourceProfile.UNKNOWN) ?
			slot.getResourceProfile() : resourceProfile;
		taskExecutorResource.addSlot(slot.getSlotId(), allocatedResourceProfile);
	}

	public void recordAllocatedResource(PendingTaskManagerSlot pendingSlot, ResourceProfile resourceProfile) {
		PendingTaskManagerId pendingTaskManagerId = pendingSlot.getPendingTaskManagerId();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(pendingTaskExecutorResources.get(pendingTaskManagerId));

		ResourceProfile allocatedResourceProfile = resourceProfile.equals(ResourceProfile.UNKNOWN) ?
			pendingSlot.getResourceProfile() : resourceProfile;
		taskExecutorResource.addSlot(pendingTaskManagerId, allocatedResourceProfile);
	}

	public void removeAllocatedResource(TaskManagerSlot slot) {
		ResourceID resourceID = slot.getSlotId().getResourceID();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(taskExecutorResources.get(resourceID));

		taskExecutorResource.removeSlot(slot.getSlotId());
	}

	public void removeAllocatedResource(PendingTaskManagerSlot pendingSlot) {
		PendingTaskManagerId pendingTaskManagerId = pendingSlot.getPendingTaskManagerId();
		TaskExecutorResource taskExecutorResource = Preconditions.checkNotNull(pendingTaskExecutorResources.get(pendingTaskManagerId));

		taskExecutorResource.removeSlot(pendingTaskManagerId);
	}
}

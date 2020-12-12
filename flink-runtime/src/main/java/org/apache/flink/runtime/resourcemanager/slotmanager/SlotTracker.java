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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotStatus;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Tracks slots and their {@link SlotState}.
 */
interface SlotTracker {

	/**
	 * Registers the given listener with this tracker.
	 *
	 * @param slotStatusUpdateListener listener to register
	 */
	void registerSlotStatusUpdateListener(SlotStatusUpdateListener slotStatusUpdateListener);

	/**
	 * Adds the given slot to this tracker. The given slot may already be allocated for a job.
	 * This method must be called before the tracker is notified of any state transition or slot status notification.
	 *
	 * @param slotId ID of the slot
	 * @param resourceProfile resource of the slot
	 * @param taskManagerConnection connection to the hosting task executor
	 * @param initialJob job that the slot is allocated for, or null if it is free
	 */
	void addSlot(
		SlotID slotId,
		ResourceProfile resourceProfile,
		TaskExecutorConnection taskManagerConnection,
		@Nullable JobID initialJob);

	/**
	 * Removes the given set of slots from the slot manager. If a removed slot was not free at the time of removal, then
	 * this method will automatically transition the slot to a free state.
	 *
	 * @param slotsToRemove identifying the slots to remove from the slot manager
	 */
	void removeSlots(Iterable<SlotID> slotsToRemove);

	/**
	 * Notifies the tracker that the allocation for the given slot, for the given job, has started.
	 *
	 * @param slotId slot being allocated
	 * @param jobId job for which the slot is being allocated
	 */
	void notifyAllocationStart(SlotID slotId, JobID jobId);

	/**
	 * Notifies the tracker that the allocation for the given slot, for the given job, has completed successfully.
	 *
	 * @param slotId slot being allocated
	 * @param jobId job for which the slot is being allocated
	 */
	void notifyAllocationComplete(SlotID slotId, JobID jobId);

	/**
	 * Notifies the tracker that the given slot was freed.
	 *
	 * @param slotId slot being freed
	 */
	void notifyFree(SlotID slotId);

	/**
	 * Notifies the tracker about the slot statuses.
	 *
	 * @param slotStatuses slot statues
	 */
	void notifySlotStatus(Iterable<SlotStatus> slotStatuses);

	/**
	 * Returns a view over free slots. The returned collection cannot be modified directly, but reflects changes to the
	 * set of free slots.
	 *
	 * @return free slots
	 */
	Collection<TaskManagerSlotInformation> getFreeSlots();
}

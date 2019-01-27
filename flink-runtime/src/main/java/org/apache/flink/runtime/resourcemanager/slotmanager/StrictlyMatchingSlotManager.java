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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * A slot manager that answers request with the slot which exactly the same resource with the request.
 */
public class StrictlyMatchingSlotManager extends SlotManager {

	public StrictlyMatchingSlotManager(
		ScheduledExecutor scheduledExecutor,
		Time taskManagerRequestTimeout,
		Time slotRequestTimeout,
		Time taskManagerTimeout,
		Time taskManagerCheckerInitialDelay) {
		super(scheduledExecutor, taskManagerRequestTimeout, slotRequestTimeout, taskManagerTimeout, taskManagerCheckerInitialDelay);
	}

	@Override
	protected TaskManagerSlot findMatchingSlot(SlotRequest slotRequest) {
		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = freeSlots.entrySet().iterator();
		while (iterator.hasNext()) {
			TaskManagerSlot taskManagerSlot = iterator.next().getValue();

			// sanity check
			Preconditions.checkState(taskManagerSlot.getState() == TaskManagerSlot.State.FREE,
				String.format("Slot %s is in state %s", taskManagerSlot.getSlotId(), taskManagerSlot.getState()));

			if (taskManagerSlot.getResourceProfile().equals(slotRequest.getResourceProfile()) &&
				placementConstraintManager.check(
					slotRequest.getJobId(),
					allocationIdTags.get(slotRequest.getAllocationId()),
					getTaskExecutorSlotTags(taskManagerSlot.getSlotId()))) {
				freeSlots.remove(taskManagerSlot.getSlotId());
				return taskManagerSlot;
			}
		}
		return null;
	}

	@Override
	protected PendingSlotRequest findMatchingRequest(TaskManagerSlot taskManagerSlot) {
		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			if (taskManagerSlot.getResourceProfile().equals(pendingSlotRequest.getResourceProfile()) &&
				!pendingSlotRequest.isAssigned() &&
				placementConstraintManager.check(
					pendingSlotRequest.getJobId(),
					allocationIdTags.get(pendingSlotRequest.getAllocationId()),
					getTaskExecutorSlotTags(taskManagerSlot.getSlotId()))) {
				return pendingSlotRequest;
			}
		}
		return null;
	}

}

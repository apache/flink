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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.util.Preconditions;

import java.util.Iterator;
import java.util.Map;

/**
 * A slot manager that answers request with the slot which matches the request best.
 * The slot which can match the request and has minimal distance with the request will be chosen.
 */
public class OptimalMatchingSlotManager extends SlotManager {

	public OptimalMatchingSlotManager(
			ScheduledExecutor scheduledExecutor,
			Time taskManagerRequestTimeout,
			Time slotRequestTimeout,
			Time taskManagerTimeout) {
		super(scheduledExecutor, taskManagerRequestTimeout, slotRequestTimeout, taskManagerTimeout);
	}

	@Override
	protected TaskManagerSlot findMatchingSlot(ResourceProfile requestResourceProfile) {
		TaskManagerSlot chosenSlot = null;
		double minScore = Double.MAX_VALUE;

		Iterator<Map.Entry<SlotID, TaskManagerSlot>> iterator = freeSlots.entrySet().iterator();
		while (iterator.hasNext()) {
			TaskManagerSlot taskManagerSlot = iterator.next().getValue();

			// sanity check
			Preconditions.checkState(taskManagerSlot.isFree());

			double score = taskManagerSlot.getResourceProfile().matchingScore(requestResourceProfile);
			if (score < minScore) {
				minScore = score;
				chosenSlot = taskManagerSlot;
			}
			if (score <= 0) {
				break;
			}
		}
		if (chosenSlot != null) {
			freeSlots.remove(chosenSlot.getSlotId());
		}
		return chosenSlot;
	}

	@Override
	protected PendingSlotRequest findMatchingRequest(ResourceProfile slotResourceProfile) {
		PendingSlotRequest chosenRequest = null;
		double minScore = Double.MAX_VALUE;

		for (PendingSlotRequest pendingSlotRequest : pendingSlotRequests.values()) {
			double score = pendingSlotRequest.getResourceProfile().matchingScore(slotResourceProfile);
			if (!pendingSlotRequest.isAssigned() && score < minScore) {
				minScore = score;
				chosenRequest = pendingSlotRequest;
			}
			if (score <= 0) {
				break;
			}
		}
		return chosenRequest;
	}

}

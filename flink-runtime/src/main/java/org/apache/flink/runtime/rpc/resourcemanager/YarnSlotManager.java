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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.rpc.resourcemanager;

import org.apache.flink.runtime.clusterframework.types.AllocationJobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

import java.util.Map;

/**
 * The Yarn slotManager implementation. It is responsible for slot management in yarn mode.
 */
public class YarnSlotManager extends SlotManager {

	public YarnSlotManager(ResourceManager resourceManager) {
		super(resourceManager);
	}

	@Override
	protected void handleNoAvailableSlotForSlotRequest(SlotRequest slotRequest) {
		pendingSlotRequest.add(slotRequest);
		// TODO, send request container to yarn cluster
	}

	/**
	 * in yarn mode, find the best matched profile in the availableslots
	 *
	 * @param slotRequest
	 * @return
	 */
	@Override
	protected SlotID fetchAndOccupySlotIfMatched(SlotRequest slotRequest) {
		ResourceProfile profile = slotRequest.getProfile();
		double maxMatchedDegree = 0;
		SlotID maxMatchedSlot = null;
		for (Map.Entry<SlotID, ResourceProfile> entry : availableSlots.entrySet()) {
			double matchedDegree = entry.getValue().matchDegree(profile);
			if (matchedDegree > maxMatchedDegree) {
				maxMatchedDegree = matchedDegree;
				maxMatchedSlot = entry.getKey();
			}
		}
		if (maxMatchedSlot != null) {
			availableSlots.remove(maxMatchedSlot);
			allocatedSlots.put(
				maxMatchedSlot,
				new AllocationJobID(
					slotRequest.getAllocationID(),
					slotRequest.getJobID()));
		}
		return maxMatchedSlot;
	}

	@Override
	protected SlotRequest offerSlotForPendingRequestIfMatched(
		SlotID slotID,
		ResourceProfile profile) {
		double maxMatchedDegree = 0;
		SlotRequest maxMatchedRequest = null;
		for (SlotRequest request : pendingSlotRequest) {
			double matchedDegree = profile.matchDegree(request.getProfile());
			if (matchedDegree > maxMatchedDegree) {
				maxMatchedDegree = matchedDegree;
				maxMatchedRequest = request;
			}
		}
		if (maxMatchedRequest != null) {
			pendingSlotRequest.remove(maxMatchedRequest);
			allocatedSlots.put(
				slotID,
				new AllocationJobID(
					maxMatchedRequest.getAllocationID(),
					maxMatchedRequest.getJobID()));
		}
		return maxMatchedRequest;
	}
}

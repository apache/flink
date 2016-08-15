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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Base SlotManager implementation. It is responsible for slot managementment in resourceManager.
 */
public abstract class SlotManager {
	protected List<SlotRequest> pendingSlotRequest = new LinkedList<>();
	protected Map<SlotID, AllocationJobID> allocatedSlots = new HashMap<>();
	protected Map<SlotID, ResourceProfile> availableSlots = new HashMap<>();
	private ResourceManager resourceManager;

	public SlotManager(ResourceManager resourceManager) {
		this.resourceManager = resourceManager;
	}

	/**
	 * request new slot from slotManager
	 *
	 * @param slotRequest
	 * @return whether to find the free matched slot for the request or not
	 */
	public boolean requestSlot(SlotRequest slotRequest) {
		SlotID slotID = fetchAndOccupySlotIfMatched(slotRequest);
		if (slotID != null) {
			resourceManager.getSelf().sendRequestSlotToTaskManager(slotRequest, slotID);
			return true;
		} else {
			handleNoAvailableSlotForSlotRequest(slotRequest);
			return false;
		}
	}

	/**
	 * offer new slot to slotManager
	 *
	 * @param slotID
	 * @param profile
	 * @return whether to apply input slot for pendingSlotRequest at once or not
	 */
	public boolean offerSlot(SlotID slotID, ResourceProfile profile) {
		if (allocatedSlots.containsKey(slotID)) {
			allocatedSlots.remove(slotID);
		}
		SlotRequest request = offerSlotForPendingRequestIfMatched(slotID, profile);
		if (request != null) {
			resourceManager.getSelf().sendRequestSlotToTaskManager(request, slotID);
			return true;
		} else {
			// cannot find match pending request for the offered slot
			availableSlots.put(slotID, profile);
			return false;
		}
	}

	public void declineSlotRequestFromTaskManager(SlotRequest slotRequest) {
		pendingSlotRequest.add(slotRequest);
		// TODO whether need to add occupied slot back to available slots
	}


	protected abstract SlotID fetchAndOccupySlotIfMatched(SlotRequest slotRequest);

	protected abstract void handleNoAvailableSlotForSlotRequest(SlotRequest slotRequest);

	protected abstract SlotRequest offerSlotForPendingRequestIfMatched(
		SlotID slotID,
		ResourceProfile profile);

}

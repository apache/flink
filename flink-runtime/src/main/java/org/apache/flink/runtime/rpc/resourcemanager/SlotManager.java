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

package org.apache.flink.runtime.rpc.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.rpc.taskexecutor.SlotReport;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Base SlotManager implementation. It is responsible for slot managementment in resourceManager.
 */
public abstract class SlotManager {
	protected List<SlotRequest> pendingSlotRequest = new LinkedList<>();
	protected Map<SlotID, Tuple2<AllocationID, JobID>> allocatedSlots = new HashMap<>();
	protected Map<SlotID, ResourceProfile> availableSlots = new HashMap<>();
	private ResourceManager resourceManager;

	public SlotManager(ResourceManager resourceManager) {
		this.resourceManager = resourceManager;
	}

	/**
	 * request new slot from slotManager
	 *
	 * @param slotRequest
	 *
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
	 *
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

	public void handleSlotRequestFailedAtTaskManager(final SlotRequest slotRequest, final SlotID slotID) {
		// TODO
	}

	/**
	 * try to fetch a matched slot from available slots for input slotRequest;
	 * if find one, occupy it and return its SlotID; if not, return null
	 *
	 * @param slotRequest slot allocation request
	 *
	 * @return if find the matched slot for slotRequest, return its SlotID, else return null
	 */
	protected abstract SlotID fetchAndOccupySlotIfMatched(SlotRequest slotRequest);

	/**
	 * handle the case where there is no available slot for input slotRequest
	 *
	 * @param slotRequest slot allocationRequet
	 */
	protected abstract void handleNoAvailableSlotForSlotRequest(SlotRequest slotRequest);

	/**
	 * try to offer a slot for a matched SlotRequest in pending requests list
	 *
	 * @param slotID  the identify of the offered slot
	 * @param profile the resourceProfile of the offered slot
	 *
	 * @return if find one matched slotRequest, return it; else return null
	 */
	protected abstract SlotRequest offerSlotForPendingRequestIfMatched(
		SlotID slotID,
		ResourceProfile profile
	);

}

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

package org.apache.flink.yarn.slotmanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceSlot;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServices;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;

import java.util.Map;

/**
 * A strictly matching and release fast slot manager.
 * It answers requests with slots whose resource strictly match the resource requested,
 * and if all slots on a task executor are free by job master,
 * will release the task executor fast instead of keeping them for later use(TODO).
 */
public class SMRFSlotManager extends SlotManager {

	public SMRFSlotManager(ResourceManagerServices rmServices) {
		super(rmServices);
	}

	@Override
	protected ResourceSlot chooseSlotToUse(SlotRequest request, Map<SlotID, ResourceSlot> freeSlots) {
		for (ResourceSlot slot : freeSlots.values()) {
			if (request.getResourceProfile().equals(slot.getResourceProfile())) {
				return slot;
			}
		}
		return null;
	}

	@Override
	protected SlotRequest chooseRequestToFulfill(ResourceSlot offeredSlot, Map<AllocationID, SlotRequest> pendingRequests) {
		for (SlotRequest request : pendingRequests.values()) {
			if (request.getResourceProfile().equals(offeredSlot.getResourceProfile())) {
				return request;
			}
		}
		return null;
	}

	// ------------------------------------------------------------------------

	public static class Factory implements SlotManagerFactory {

		@Override
		public SlotManager create(ResourceManagerServices rmServices) {
			return new SMRFSlotManager(rmServices);
		}
	}

}

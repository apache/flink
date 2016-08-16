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

import com.google.common.collect.Iterables;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;

/**
 * Standalone slotManager implementation. The StandaloneSlotManager is reponsible for slot management in standalone
 * mode.
 */
public class StandaloneSlotManager extends SlotManager {

	public StandaloneSlotManager(ResourceManager resourceManager) {
		super(resourceManager);
	}

	@Override
	protected void handleNoAvailableSlotForSlotRequest(SlotRequest slotRequest) {
		pendingSlotRequest.add(slotRequest);
	}

	/**
	 * in standalone mode, ignore the profile in the request, allocate the first available slot
	 *
	 * @param slotRequest
	 *
	 * @return
	 */
	@Override
	protected SlotID fetchAndOccupySlotIfMatched(SlotRequest slotRequest) {
		SlotID slotID = Iterables.getFirst(availableSlots.keySet(), null);
		if (slotID != null) {
			availableSlots.remove(slotID);
			allocatedSlots.put(
				slotID,
				Tuple2.of(slotRequest.getAllocationID(), slotRequest.getJobID())
			);
		}
		return slotID;
	}

	/**
	 * in standalone mode, ignore the profile in the request, offer slot to the first pending request
	 *
	 * @param slotID
	 * @param profile
	 *
	 * @return
	 */
	@Override
	protected SlotRequest offerSlotForPendingRequestIfMatched(
		SlotID slotID,
		ResourceProfile profile
	)
	{
		SlotRequest slotRequest = Iterables.getFirst(pendingSlotRequest, null);
		if (slotRequest != null) {
			pendingSlotRequest.remove(slotRequest);
			allocatedSlots.put(
				slotID,
				Tuple2.of(slotRequest.getAllocationID(), slotRequest.getJobID())
			);
		}
		return slotRequest;
	}
}

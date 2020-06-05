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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents a bulk of physical slot requests.
 */
class PhysicalSlotRequestBulk {

	private final Map<SlotRequestId, ResourceProfile> pendingRequests;

	private final Map<SlotRequestId, AllocationID> fulfilledRequests = new HashMap<>();

	private long unfulfillableTimestamp = Long.MAX_VALUE;

	PhysicalSlotRequestBulk(final Collection<PhysicalSlotRequest> physicalSlotRequests) {
		this.pendingRequests = physicalSlotRequests.stream()
			.collect(Collectors.toMap(
				PhysicalSlotRequest::getSlotRequestId,
				r -> r.getSlotProfile().getPhysicalSlotResourceProfile()));
	}

	void markRequestFulfilled(final SlotRequestId slotRequestId, final AllocationID allocationID) {
		pendingRequests.remove(slotRequestId);
		fulfilledRequests.put(slotRequestId, allocationID);
	}

	Map<SlotRequestId, ResourceProfile> getPendingRequests() {
		return Collections.unmodifiableMap(pendingRequests);
	}

	Map<SlotRequestId, AllocationID> getFulfilledRequests() {
		return Collections.unmodifiableMap(fulfilledRequests);
	}

	void markFulfillable() {
		unfulfillableTimestamp = Long.MAX_VALUE;
	}

	void markUnfulfillable(final long currentTimestamp) {
		if (isFulfillable()) {
			unfulfillableTimestamp = currentTimestamp;
		}
	}

	long getUnfulfillableSince() {
		return unfulfillableTimestamp;
	}

	private boolean isFulfillable() {
		return unfulfillableTimestamp == Long.MAX_VALUE;
	}
}

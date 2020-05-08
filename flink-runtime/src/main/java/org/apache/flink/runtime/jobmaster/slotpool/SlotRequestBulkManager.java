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

import org.apache.flink.runtime.util.clock.Clock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Manages slot request bulks.
 */
class SlotRequestBulkManager {

	private final Clock clock;

	private final HashMap<Long, Set<SlotPoolImpl.PendingRequest>> slotRequestBulks;

	/** Timestamps indicate when bulks become unfulfillable. */
	private final HashMap<Long, Long> bulkUnfulfillableTimestamps;

	SlotRequestBulkManager(final Clock clock) {
		this.clock = clock;
		this.slotRequestBulks = new HashMap<>();
		this.bulkUnfulfillableTimestamps = new HashMap<>();
	}

	HashMap<Long, Set<SlotPoolImpl.PendingRequest>> getSlotRequestBulks() {
		return slotRequestBulks;
	}

	Set<SlotPoolImpl.PendingRequest> getSlotRequestBulk(final long bulkId) {
		return slotRequestBulks.get(bulkId);
	}

	boolean hasBulk(final long bulkId) {
		return slotRequestBulks.containsKey(bulkId);
	}

	void addRequestToBulk(final SlotPoolImpl.PendingRequest request, final long bulkId) {
		final Set<SlotPoolImpl.PendingRequest> requestBulk = slotRequestBulks.computeIfAbsent(bulkId, k -> new HashSet<>());

		if (requestBulk.isEmpty()) {
			markBulkUnfulfillable(bulkId, clock.relativeTimeMillis());
		}

		requestBulk.add(request);
		request.getAllocatedSlotFuture().whenComplete((ignored, failure) -> {
			requestBulk.remove(request);
			if (requestBulk.isEmpty()) {
				slotRequestBulks.remove(bulkId);
			}
		});
	}

	void markBulkFulfillable(final long bulkId) {
		bulkUnfulfillableTimestamps.put(bulkId, Long.MAX_VALUE);
	}

	void markBulkUnfulfillable(final long bulkId, final long currentTimestamp) {
		if (isBulkFulfillable(bulkId)) {
			bulkUnfulfillableTimestamps.put(bulkId, currentTimestamp);
		}
	}

	private boolean isBulkFulfillable(final long bulkId) {
		return getBulkUnfulfillableSince(bulkId) == Long.MAX_VALUE;
	}

	long getBulkUnfulfillableSince(final long bulkId) {
		return bulkUnfulfillableTimestamps.getOrDefault(bulkId, Long.MAX_VALUE);
	}
}

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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

/**
 * Represents a request for a physical slot.
 */
public class PhysicalSlotRequest {

	private final SlotRequestId slotRequestId;

	private final ResourceProfile resourceProfile;

	private final boolean slotWillBeOccupiedIndefinitely;

	private final long bulkId;

	private PhysicalSlotRequest(
		final SlotRequestId slotRequestId,
		final ResourceProfile resourceProfile,
		final boolean slotWillBeOccupiedIndefinitely,
		final long bulkId) {

		this.slotRequestId = slotRequestId;
		this.resourceProfile = resourceProfile;
		this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
		this.bulkId = bulkId;
	}

	SlotRequestId getSlotRequestId() {
		return slotRequestId;
	}

	ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	boolean getSlotWillBeOccupiedIndefinitely() {
		return slotWillBeOccupiedIndefinitely;
	}

	long getBulkId() {
		return bulkId;
	}

	static PhysicalSlotRequest createPhysicalSlotRequest(
			final SlotRequestId slotRequestId,
			final ResourceProfile resourceProfile,
			final boolean slotWillBeOccupiedIndefinitely,
			final long bulkId) {

		return new PhysicalSlotRequest(slotRequestId, resourceProfile, slotWillBeOccupiedIndefinitely, bulkId);
	}
}

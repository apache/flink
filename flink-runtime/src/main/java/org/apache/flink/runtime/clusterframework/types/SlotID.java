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

package org.apache.flink.runtime.clusterframework.types;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Unique identifier for a slot on a TaskManager. This ID is constant across the
 * life time of the TaskManager.
 * 
 * <p>In contrast, the {@link AllocationID} represents the a slot allocation and changes
 * every time the slot is allocated by a JobManager.
 */
public class SlotID implements ResourceIDRetrievable, Serializable {

	private static final long serialVersionUID = -6399206032549807771L;

	/** The resource id which this slot located */
	private final ResourceID resourceId;

	/** The numeric id for single slot */
	private final int slotNumber;
	
	public SlotID(ResourceID resourceId, int slotNumber) {
		checkArgument(0 <= slotNumber, "Slot number must be positive.");
		this.resourceId = checkNotNull(resourceId, "ResourceID must not be null");
		this.slotNumber = slotNumber;
	}

	private SlotID(ResourceID resourceID) {
		this.resourceId = checkNotNull(resourceID, "ResourceID must not be null");
		this.slotNumber = -1;
	}

	// ------------------------------------------------------------------------

	@Override
	public ResourceID getResourceID() {
		return resourceId;
	}

	public int getSlotNumber() {
		return slotNumber;
	}

	// ------------------------------------------------------------------------

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SlotID slotID = (SlotID) o;

		return slotNumber == slotID.slotNumber && resourceId.equals(slotID.resourceId);
	}

	@Override
	public int hashCode() {
		int result = resourceId.hashCode();
		result = 31 * result + slotNumber;
		return result;
	}

	@Override
	public String toString() {
		return resourceId + "_" + (slotNumber >= 0 ? slotNumber : "dynamic");
	}

	/**
	 * Generate a SlotID without actual slot index for dynamic slot allocation.
	 */
	public static SlotID generateDynamicSlotID(ResourceID resourceID) {
		return new SlotID(resourceID);
	}
}

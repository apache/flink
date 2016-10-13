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

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Unique identifier for a slot which located in TaskManager.
 */
public class SlotID implements ResourceIDRetrievable, Serializable {

	private static final long serialVersionUID = -6399206032549807771L;

	/** The resource id which this slot located */
	private final ResourceID resourceId;

	/** The numeric id for single slot */
	private final int slotNumber;

	public SlotID(ResourceID resourceId, int slotNumber) {
		this.resourceId = checkNotNull(resourceId, "ResourceID must not be null");
		this.slotNumber = slotNumber;
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

		if (slotNumber != slotID.slotNumber) {
			return false;
		}
		return resourceId.equals(slotID.resourceId);
	}

	@Override
	public int hashCode() {
		int result = resourceId.hashCode();
		result = 31 * result + slotNumber;
		return result;
	}

	@Override
	public String toString() {
		return resourceId + "_" + slotNumber;
	}

	/**
	 * Generate a random slot id.
	 *
	 * @return A random slot id.
	 */
	public static SlotID generate() {
		return new SlotID(ResourceID.generate(), 0);
	}
}

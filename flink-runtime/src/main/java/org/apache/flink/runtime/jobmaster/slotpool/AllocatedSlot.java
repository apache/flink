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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@code AllocatedSlot} represents a slot that the JobMaster allocated from a TaskExecutor.
 * It represents a slice of allocated resources from the TaskExecutor.
 * 
 * <p>To allocate an {@code AllocatedSlot}, the requests a slot from the ResourceManager. The
 * ResourceManager picks (or starts) a TaskExecutor that will then allocate the slot to the
 * JobMaster and notify the JobMaster.
 * 
 * <p>Note: Prior to the resource management changes introduced in (Flink Improvement Proposal 6),
 * an AllocatedSlot was allocated to the JobManager as soon as the TaskManager registered at the
 * JobManager. All slots had a default unknown resource profile. 
 */
class AllocatedSlot implements SlotContext {

	/** The ID under which the slot is allocated. Uniquely identifies the slot. */
	private final AllocationID allocationId;

	/** The location information of the TaskManager to which this slot belongs */
	private final TaskManagerLocation taskManagerLocation;

	/** The resource profile of the slot provides */
	private final ResourceProfile resourceProfile;

	/** RPC gateway to call the TaskManager that holds this slot */
	private final TaskManagerGateway taskManagerGateway;

	/** The number of the slot on the TaskManager to which slot belongs. Purely informational. */
	private final int physicalSlotNumber;

	private final AtomicReference<Payload> payloadReference;

	// ------------------------------------------------------------------------

	public AllocatedSlot(
			AllocationID allocationId,
			TaskManagerLocation location,
			int physicalSlotNumber,
			ResourceProfile resourceProfile,
			TaskManagerGateway taskManagerGateway) {
		this.allocationId = checkNotNull(allocationId);
		this.taskManagerLocation = checkNotNull(location);
		this.physicalSlotNumber = physicalSlotNumber;
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerGateway = checkNotNull(taskManagerGateway);

		payloadReference = new AtomicReference<>(null);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the Slot's unique ID defined by its TaskManager.
	 */
	public SlotID getSlotId() {
		return new SlotID(getTaskManagerId(), physicalSlotNumber);
	}

	/**
	 * Gets the ID under which the slot is allocated, which uniquely identifies the slot.
	 * 
	 * @return The ID under which the slot is allocated
	 */
	public AllocationID getAllocationId() {
		return allocationId;
	}

	/**
	 * Gets the ID of the TaskManager on which this slot was allocated.
	 * 
	 * <p>This is equivalent to {@link #getTaskManagerLocation()#getTaskManagerId()}.
	 * 
	 * @return This slot's TaskManager's ID.
	 */
	public ResourceID getTaskManagerId() {
		return getTaskManagerLocation().getResourceID();
	}

	/**
	 * Gets the resource profile of the slot.
	 *
	 * @return The resource profile of the slot.
	 */
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}

	/**
	 * Gets the location info of the TaskManager that offers this slot.
	 *
	 * @return The location info of the TaskManager that offers this slot
	 */
	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	/**
	 * Gets the actor gateway that can be used to send messages to the TaskManager.
	 * <p>
	 * This method should be removed once the new interface-based RPC abstraction is in place
	 *
	 * @return The actor gateway that can be used to send messages to the TaskManager.
	 */
	public TaskManagerGateway getTaskManagerGateway() {
		return taskManagerGateway;
	}

	/**
	 * Returns the physical slot number of the allocated slot. The physical slot number corresponds
	 * to the slot index on the TaskExecutor.
	 *
	 * @return Physical slot number of the allocated slot
	 */
	public int getPhysicalSlotNumber() {
		return physicalSlotNumber;
	}

	/**
	 * Returns true if this slot is not being used (e.g. a logical slot is allocated from this slot).
	 *
	 * @return true if a logical slot is allocated from this slot, otherwise false
	 */
	public boolean isUsed() {
		return payloadReference.get() != null;
	}

	/**
	 * Tries to assign the given payload to this allocated slot. This only works if there has not
	 * been another payload assigned to this slot.
	 *
	 * @param payload to assign to this slot
	 * @return true if the payload could be assigned, otherwise false
	 */
	public boolean tryAssignPayload(Payload payload) {
		return payloadReference.compareAndSet(null, payload);
	}

	/**
	 * Triggers the release of the assigned payload. If the payload could be released,
	 * then it is removed from the slot.
	 *
	 * @param cause of the release operation
	 */
	public void releasePayload(Throwable cause) {
		final Payload payload = payloadReference.get();

		if (payload != null) {
			payload.release(cause);
			payloadReference.set(null);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * This always returns a reference hash code.
	 */
	@Override
	public final int hashCode() {
		return super.hashCode();
	}

	/**
	 * This always checks based on reference equality.
	 */
	@Override
	public final boolean equals(Object obj) {
		return this == obj;
	}

	@Override
	public String toString() {
		return "AllocatedSlot " + allocationId + " @ " + taskManagerLocation + " - " + physicalSlotNumber;
	}

	// -----------------------------------------------------------------------
	// Interfaces
	// -----------------------------------------------------------------------

	/**
	 * Payload which can be assigned to an {@link AllocatedSlot}.
	 */
	interface Payload {

		/**
		 * Releases the payload
		 *
		 * @param cause of the payload release
		 */
		void release(Throwable cause);
	}
}

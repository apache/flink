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

package org.apache.flink.runtime.instance;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.SlotContext;
import org.apache.flink.runtime.jobmanager.slots.SlotException;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@code AllocatedSlot} represents a slot that the JobManager allocated from a TaskManager.
 * It represents a slice of allocated resources from the TaskManager.
 * 
 * <p>To allocate an {@code AllocatedSlot}, the requests a slot from the ResourceManager. The
 * ResourceManager picks (or starts) a TaskManager that will then allocate the slot to the
 * JobManager and notify the JobManager.
 * 
 * <p>Note: Prior to the resource management changes introduced in (Flink Improvement Proposal 6),
 * an AllocatedSlot was allocated to the JobManager as soon as the TaskManager registered at the
 * JobManager. All slots had a default unknown resource profile. 
 */
public class AllocatedSlot {

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

	private final SlotOwner slotOwner;

	private final AtomicReference<LogicalSlot> logicalSlotReference;

	// ------------------------------------------------------------------------

	public AllocatedSlot(
			AllocationID allocationId,
			TaskManagerLocation location,
			int physicalSlotNumber,
			ResourceProfile resourceProfile,
			TaskManagerGateway taskManagerGateway,
			SlotOwner slotOwner) {
		this.allocationId = checkNotNull(allocationId);
		this.taskManagerLocation = checkNotNull(location);
		this.physicalSlotNumber = physicalSlotNumber;
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerGateway = checkNotNull(taskManagerGateway);
		this.slotOwner = checkNotNull(slotOwner);

		logicalSlotReference = new AtomicReference<>(null);
	}

	// ------------------------------------------------------------------------

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
	 * Gets the number of the slot.
	 *
	 * @return The number of the slot on the TaskManager.
	 */
	public int getPhysicalSlotNumber() {
		return physicalSlotNumber;
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
	 * Triggers the release of the logical slot.
	 */
	public void triggerLogicalSlotRelease() {
		final LogicalSlot logicalSlot = logicalSlotReference.get();

		if (logicalSlot != null) {
			logicalSlot.releaseSlot();
		}
	}

	/**
	 * Releases the logical slot.
	 *
	 * @return true if the logical slot could be released, false otherwise.
	 */
	public boolean releaseLogicalSlot() {
		final LogicalSlot logicalSlot = logicalSlotReference.get();

		if (logicalSlot != null) {
			if (logicalSlot instanceof Slot) {
				final Slot slot = (Slot) logicalSlot;
				if (slot.markReleased()) {
					logicalSlotReference.set(null);
					return true;
				}
			} else {
				throw new RuntimeException("Unsupported logical slot type encountered " + logicalSlot.getClass());
			}

		}

		return false;
	}

	/**
	 * Allocates a logical {@link SimpleSlot}.
	 *
	 * @param slotRequestId identifying the corresponding slot request
	 * @param locality specifying the locality of the allocated slot
	 * @return an allocated logical simple slot
	 * @throws SlotException if we could not allocate a simple slot
	 */
	public SimpleSlot allocateSimpleSlot(SlotRequestID slotRequestId, Locality locality) throws SlotException {
		final AllocatedSlotContext allocatedSlotContext = new AllocatedSlotContext(
			slotRequestId);

		final SimpleSlot simpleSlot = new SimpleSlot(allocatedSlotContext, slotOwner, physicalSlotNumber);

		if (logicalSlotReference.compareAndSet(null, simpleSlot)) {
			simpleSlot.setLocality(locality);
			return simpleSlot;
		} else {
			throw new SlotException("Could not allocate logical simple slot because the allocated slot is already used.");
		}
	}

	/**
	 * Allocates a logical {@link SharedSlot}.
	 *
	 * @param slotRequestId identifying the corresponding slot request
	 * @param slotSharingGroupAssignment the slot sharing group to which the shared slot shall belong
	 * @return an allocated logical shared slot
	 * @throws SlotException if we could not allocate a shared slot
	 */
	public SharedSlot allocateSharedSlot(SlotRequestID slotRequestId, SlotSharingGroupAssignment slotSharingGroupAssignment) throws SlotException {

		final AllocatedSlotContext allocatedSlotContext = new AllocatedSlotContext(
			slotRequestId);
		final SharedSlot sharedSlot = new SharedSlot(allocatedSlotContext, slotOwner, slotSharingGroupAssignment);

		if (logicalSlotReference.compareAndSet(null, sharedSlot)) {


			return sharedSlot;
		} else {
			throw new SlotException("Could not allocate logical shared slot because the allocated slot is already used.");
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

	/**
	 * Slot context for {@link AllocatedSlot}.
	 */
	private final class AllocatedSlotContext implements SlotContext {

		private final SlotRequestID slotRequestId;

		private AllocatedSlotContext(SlotRequestID slotRequestId) {
			this.slotRequestId = Preconditions.checkNotNull(slotRequestId);
		}

		@Override
		public SlotRequestID getSlotRequestId() {
			return slotRequestId;
		}

		@Override
		public AllocationID getAllocationId() {
			return allocationId;
		}

		@Override
		public TaskManagerLocation getTaskManagerLocation() {
			return taskManagerLocation;
		}

		@Override
		public int getPhysicalSlotNumber() {
			return physicalSlotNumber;
		}

		@Override
		public TaskManagerGateway getTaskManagerGateway() {
			return taskManagerGateway;
		}
	}
}

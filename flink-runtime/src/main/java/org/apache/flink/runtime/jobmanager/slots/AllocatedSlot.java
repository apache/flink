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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

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
	private final AllocationID slotAllocationId;

	/** The ID of the job this slot is allocated for */
	private final JobID jobID;

	/** The location information of the TaskManager to which this slot belongs */
	private final TaskManagerLocation taskManagerLocation;

	/** The resource profile of the slot provides */
	private final ResourceProfile resourceProfile;

	/** RPC gateway to call the TaskManager that holds this slot */
	private final TaskManagerGateway taskManagerGateway;

	/** The number of the slot on the TaskManager to which slot belongs. Purely informational. */
	private final int slotNumber;

	// ------------------------------------------------------------------------

	public AllocatedSlot(
			AllocationID slotAllocationId,
			JobID jobID,
			TaskManagerLocation location,
			int slotNumber,
			ResourceProfile resourceProfile,		
			TaskManagerGateway taskManagerGateway) {
		this.slotAllocationId = checkNotNull(slotAllocationId);
		this.jobID = checkNotNull(jobID);
		this.taskManagerLocation = checkNotNull(location);
		this.slotNumber = slotNumber;
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerGateway = checkNotNull(taskManagerGateway);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the ID under which the slot is allocated, which uniquely identifies the slot.
	 * 
	 * @return The ID under which the slot is allocated
	 */
	public AllocationID getSlotAllocationId() {
		return slotAllocationId;
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
	 * Returns the ID of the job this allocated slot belongs to.
	 *
	 * @return the ID of the job this allocated slot belongs to
	 */
	public JobID getJobID() {
		return jobID;
	}

	/**
	 * Gets the number of the slot.
	 *
	 * @return The number of the slot on the TaskManager.
	 */
	public int getSlotNumber() {
		return slotNumber;
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
		return "AllocatedSlot " + slotAllocationId + " @ " + taskManagerLocation + " - " + slotNumber;
	}
}

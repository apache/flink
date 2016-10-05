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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The description of slots, TaskManagers offer one or more task slots, which define a slice of
 * their resources. This description will contain some static information about the slot, such
 * as the location and numeric id of the slot, rpc gateway to communicate with the TaskManager which
 * owns the slot.
 */
public class SlotDescriptor {

	/** The ID of the job this slice belongs to. */
	private final JobID jobID;

	/** The location information of the TaskManager to which this slot belongs */
	private final TaskManagerLocation taskManagerLocation;

	/** The number of the slot on which the task is deployed */
	private final int slotNumber;

	/** The resource profile of the slot provides */
	private final ResourceProfile resourceProfile;

	/** TEMP until the new RPC is in place: The actor gateway to communicate with the TaskManager */
	private final TaskManagerGateway taskManagerGateway;

	public SlotDescriptor(
		final JobID jobID,
		final TaskManagerLocation location,
		final int slotNumber,
		final ResourceProfile resourceProfile,
		final TaskManagerGateway actorGateway)
	{
		this.jobID = checkNotNull(jobID);
		this.taskManagerLocation = checkNotNull(location);
		this.slotNumber = slotNumber;
		this.resourceProfile = checkNotNull(resourceProfile);
		this.taskManagerGateway = checkNotNull(actorGateway);
	}

	public SlotDescriptor(final SlotDescriptor other) {
		this.jobID = other.jobID;
		this.taskManagerLocation = other.taskManagerLocation;
		this.slotNumber = other.slotNumber;
		this.resourceProfile = other.resourceProfile;
		this.taskManagerGateway = other.taskManagerGateway;
	}
	
	// TODO - temporary workaround until we have the SlotDesriptor in the Slot
	public SlotDescriptor(final Slot slot) {
		this.jobID = slot.getJobID();
		this.taskManagerLocation = slot.getTaskManagerLocation();
		this.slotNumber = slot.getRootSlotNumber();
		this.resourceProfile = new ResourceProfile(0, 0);
		this.taskManagerGateway = slot.getTaskManagerGateway();
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

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SlotDescriptor that = (SlotDescriptor) o;

		if (slotNumber != that.slotNumber) {
			return false;
		}
		if (!jobID.equals(that.jobID)) {
			return false;
		}
		return taskManagerLocation.equals(that.taskManagerLocation);

	}

	@Override
	public int hashCode() {
		int result = jobID.hashCode();
		result = 31 * result + taskManagerLocation.hashCode();
		result = 31 * result + slotNumber;
		return result;
	}

	@Override
	public String toString() {
		return taskManagerLocation + " - " + slotNumber;
	}
}

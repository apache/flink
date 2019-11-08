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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A CoLocationConstraint manages the location of a set of tasks
 * (Execution Vertices). In co-location groups, the different subtasks of
 * different JobVertices need to be executed on the same slot.
 * This is realized by creating a special shared slot that holds these tasks.
 * 
 * <p>This class tracks the location and the shared slot for this set of tasks.
 */
public class CoLocationConstraint {

	private final CoLocationGroup group;

	private volatile TaskManagerLocation lockedLocation;

	private volatile SlotRequestId slotRequestId;

	CoLocationConstraint(CoLocationGroup group) {
		Preconditions.checkNotNull(group);
		this.group = group;
		this.slotRequestId = null;
	}

	// ------------------------------------------------------------------------
	//  Status & Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID that identifies the co-location group.
	 * 
	 * @return The ID that identifies the co-location group.
	 */
	public AbstractID getGroupId() {
		return this.group.getId();
	}

	/**
	 * Checks whether the location of this constraint has been assigned.
	 * The location is locked via the {@link #lockLocation(TaskManagerLocation)}
	 * method.
	 * 
	 * @return True if the location has been assigned, false otherwise.
	 */
	public boolean isAssigned() {
		return lockedLocation != null;
	}

	/**
	 * Gets the location assigned to this slot. This method only succeeds after
	 * the location has been locked via the {@link #lockLocation(TaskManagerLocation)}
	 * method and {@link #isAssigned()} returns true.
	 *
	 * @return The instance describing the location for the tasks of this constraint.
	 * @throws IllegalStateException Thrown if the location has not been assigned, yet.
	 */
	public TaskManagerLocation getLocation() {
		if (lockedLocation != null) {
			return lockedLocation;
		} else {
			throw new IllegalStateException("Location not yet locked");
		}
	}

	// ------------------------------------------------------------------------
	//  Assigning resources and location
	// ------------------------------------------------------------------------

	/**
	 * Locks the location of this slot. The location can be locked only once
	 * and only after a shared slot has been assigned.
	 *
	 * <p>Note: This method exists for compatibility reasons with the new {@link SlotPool}.
	 *
	 * @param taskManagerLocation to lock this co-location constraint to
	 */
	public void lockLocation(TaskManagerLocation taskManagerLocation) {
		checkNotNull(taskManagerLocation);
		checkState(lockedLocation == null, "Location is already locked.");

		lockedLocation = taskManagerLocation;
	}

	/**
	 * Sets the slot request id of the currently assigned slot to the co-location constraint.
	 * All other tasks belonging to this co-location constraint will be deployed to the same slot.
	 *
	 * @param slotRequestId identifying the assigned slot for this co-location constraint
	 */
	public void setSlotRequestId(@Nullable SlotRequestId slotRequestId) {
		this.slotRequestId = slotRequestId;
	}

	/**
	 * Returns the currently assigned slot request id identifying the slot to which tasks
	 * belonging to this co-location constraint will be deployed to.
	 *
	 * @return Slot request id of the assigned slot or null if none
	 */
	@Nullable
	public SlotRequestId getSlotRequestId() {
		return slotRequestId;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "CoLocationConstraint{" +
			"group=" + group +
			", lockedLocation=" + lockedLocation +
			", slotRequestId=" + slotRequestId +
			'}';
	}
}

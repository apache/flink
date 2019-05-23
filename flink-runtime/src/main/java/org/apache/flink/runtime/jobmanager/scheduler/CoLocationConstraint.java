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

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A CoLocationConstraint manages the location of a set of tasks
 * (Execution Vertices). In co-location groups, the different subtasks of
 * different JobVertices need to be executed on the same {@link Instance}.
 * This is realized by creating a special shared slot that holds these tasks.
 * 
 * <p>This class tracks the location and the shared slot for this set of tasks.
 */
public class CoLocationConstraint {

	private final CoLocationGroup group;

	private volatile SharedSlot sharedSlot;

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
	 * Gets the shared slot into which this constraint's tasks are places.
	 * 
	 * @return The shared slot into which this constraint's tasks are places.
	 */
	public SharedSlot getSharedSlot() {
		return sharedSlot;
	}

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
	 * The location is assigned once a slot has been set, via the
	 * {@link #setSharedSlot(org.apache.flink.runtime.instance.SharedSlot)} method,
	 * and the location is locked via the {@link #lockLocation()} method.
	 * 
	 * @return True if the location has been assigned, false otherwise.
	 */
	public boolean isAssigned() {
		return lockedLocation != null;
	}

	/**
	 * Checks whether the location of this constraint has been assigned
	 * (as defined in the {@link #isAssigned()} method, and the current
	 * shared slot is alive.
	 *
	 * @return True if the location has been assigned and the shared slot is alive,
	 *         false otherwise.
	 * @deprecated Should only be called by legacy code (if using {@link Scheduler})
	 */
	@Deprecated
	public boolean isAssignedAndAlive() {
		return lockedLocation != null && sharedSlot != null && sharedSlot.isAlive();
	}

	/**
	 * Gets the location assigned to this slot. This method only succeeds after
	 * the location has been locked via the {@link #lockLocation()} method and
	 * {@link #isAssigned()} returns true.
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
	 * Assigns a new shared slot to this co-location constraint. The shared slot
	 * will hold the subtasks that are executed under this co-location constraint.
	 * If the constraint's location is assigned, then this slot needs to be from
	 * the same location (instance) as the one assigned to this constraint.
	 * 
	 * <p>If the constraint already has a slot, the current one will be released.</p>
	 *
	 * @param newSlot The new shared slot to assign to this constraint.
	 * @throws IllegalArgumentException If the constraint's location has been assigned and
	 *                                  the new slot is from a different location.
	 */
	public void setSharedSlot(SharedSlot newSlot) {
		checkNotNull(newSlot);

		if (this.sharedSlot == null) {
			this.sharedSlot = newSlot;
		}
		else if (newSlot != this.sharedSlot){
			if (lockedLocation != null && !Objects.equals(lockedLocation, newSlot.getTaskManagerLocation())) {
				throw new IllegalArgumentException(
						"Cannot assign different location to a constraint whose location is locked.");
			}
			if (this.sharedSlot.isAlive()) {
				this.sharedSlot.releaseSlot(new FlinkException("Setting new shared slot for co-location constraint."));
			}

			this.sharedSlot = newSlot;
		}
	}

	/**
	 * Locks the location of this slot. The location can be locked only once
	 * and only after a shared slot has been assigned.
	 * 
	 * @throws IllegalStateException Thrown, if the location is already locked,
	 *                               or is no slot has been set, yet.
	 */
	public void lockLocation() throws IllegalStateException {
		checkState(lockedLocation == null, "Location is already locked");
		checkState(sharedSlot != null, "Cannot lock location without a slot.");

		lockedLocation = sharedSlot.getTaskManagerLocation();
	}

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
		return "CoLocation constraint id " + getGroupId() + " shared slot " + sharedSlot;
	}
}

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

import org.apache.flink.util.AbstractID;
import org.apache.flink.runtime.instance.Instance;

import com.google.common.base.Preconditions;
import org.apache.flink.runtime.instance.SharedSlot;

/**
 * A CoLocationConstraint manages the location of a set of tasks
 * (Execution Vertices). In co-location groups, the different subtasks of
 * different JobVertices need to be executed on the same {@link Instance}.
 * This is realized by creating a special shared slot that holds these tasks.
 * 
 * <p>This class tracks the location and the shared slot for this set of tasks.</p>
 */
public class CoLocationConstraint {
	
	private final CoLocationGroup group;
	
	private volatile SharedSlot sharedSlot;
	
	private volatile boolean locationLocked;
	
	
	CoLocationConstraint(CoLocationGroup group) {
		Preconditions.checkNotNull(group);
		this.group = group;
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
		return locationLocked;
	}

	/**
	 * Checks whether the location of this constraint has been assigned
	 * (as defined in the {@link #isAssigned()} method, and the current
	 * shared slot is alive.
	 *
	 * @return True if the location has been assigned and the shared slot is alive,
	 *         false otherwise.
	 */
	public boolean isAssignedAndAlive() {
		return locationLocked && sharedSlot.isAlive();
	}

	/**
	 * Gets the location assigned to this slot. This method only succeeds after
	 * the location has been locked via the {@link #lockLocation()} method and
	 * {@link #isAssigned()} returns true.
	 *
	 * @return The instance describing the location for the tasks of this constraint.
	 * @throws IllegalStateException Thrown if the location has not been assigned, yet.
	 */
	public Instance getLocation() {
		if (locationLocked) {
			return sharedSlot.getInstance();
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
		if (this.sharedSlot == null) {
			this.sharedSlot = newSlot;
		}
		else if (newSlot != this.sharedSlot){
			if (locationLocked && this.sharedSlot.getInstance() != newSlot.getInstance()) {
				throw new IllegalArgumentException(
						"Cannot assign different location to a constraint whose location is locked.");
			}
			if (this.sharedSlot.isAlive()) {
				this.sharedSlot.releaseSlot();
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
		if (locationLocked) {
			throw new IllegalStateException("Location is already locked");
		}
		if (sharedSlot == null) {
			throw new IllegalStateException("Cannot lock location without a slot.");
		}
		locationLocked = true;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "CoLocation constraint id " + getGroupId() + " shared slot " + sharedSlot;
	}
}

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

import org.apache.flink.util.AbstractID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroupAssignment;

import java.util.HashSet;
import java.util.Set;

/**
 * This class represents a shared slot. A shared slot can have multiple
 * {@link org.apache.flink.runtime.instance.SimpleSlot} instances within itself. This allows to
 * schedule multiple tasks simultaneously, enabling Flink's streaming capabilities.
 *
 * IMPORTANT: This class contains no synchronization. Thus, the caller has to guarantee proper
 * synchronization. In the current implementation, all concurrently modifying operations are
 * passed through a {@link SlotSharingGroupAssignment} object which is responsible for
 * synchronization.
 *
 */
public class SharedSlot extends Slot {

	private final SlotSharingGroupAssignment assignmentGroup;

	private final Set<Slot> subSlots;

	public SharedSlot(JobID jobID, Instance instance, int slotNumber,
					SlotSharingGroupAssignment assignmentGroup, SharedSlot parent,
					AbstractID groupID) {
		super(jobID, instance, slotNumber, parent, groupID);

		this.assignmentGroup = assignmentGroup;
		this.subSlots = new HashSet<Slot>();
	}

	public Set<Slot> getSubSlots() {
		return subSlots;
	}

	/**
	 * Removes the simple slot from the {@link org.apache.flink.runtime.instance.SharedSlot}. Should
	 * only be called through the
	 * {@link org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroupAssignment} attribute
	 * assignmnetGroup.
	 *
	 * @param slot slot to be removed from the set of sub slots.
	 * @return Number of remaining sub slots
	 */
	public int freeSubSlot(Slot slot){
		if(!subSlots.remove(slot)){
			throw new IllegalArgumentException("Wrong shared slot for sub slot.");
		}

		return subSlots.size();
	}

	@Override
	public int getNumberLeaves() {
		int result = 0;

		for(Slot slot: subSlots){
			result += slot.getNumberLeaves();
		}

		return result;
	}

	@Override
	public void cancel() {
		// Guarantee that the operation is only executed once
		if (markCancelled()) {
			assignmentGroup.releaseSharedSlot(this);
		}
	}

	/**
	 * Release this shared slot. In order to do this:
	 *
	 * 1. Cancel and release all sub slots atomically with respect to the assigned assignment group.
	 * 2. Set the state of the shared slot to be cancelled.
	 * 3. Dispose the shared slot (returning the slot to the instance).
	 *
	 * After cancelAndReleaseSubSlots, the shared slot is marked to be dead. This prevents further
	 * sub slot creation by the scheduler.
	 */
	@Override
	public void releaseSlot() {
		assignmentGroup.releaseSharedSlot(this);
	}

	/**
	 * Creates a new sub slot if the slot is not dead, yet. This method should only be called from
	 * the assignment group instance to guarantee synchronization.
	 *
	 * @param jID id to identify tasks which can be deployed in this sub slot
	 * @return new sub slot if the shared slot is still alive, otherwise null
	 */
	public SimpleSlot allocateSubSlot(AbstractID jID){
		if(isDead()){
			return null;
		} else {
			SimpleSlot slot = new SimpleSlot(getJobID(), getInstance(), subSlots.size(), this, jID);
			subSlots.add(slot);

			return slot;
		}
	}

	public SharedSlot allocateSharedSlot(AbstractID jID){
		if(isDead()){
			return null;
		} else {
			SharedSlot slot = new SharedSlot(getJobID(), getInstance(), subSlots.size(), assignmentGroup, this, jID);
			subSlots.add(slot);

			return slot;
		}
	}

	/**
	 * Disposes the given sub slot. This
	 * is done by the means of the assignmentGroup in order to synchronize the method. If the
	 * disposed slot was the last sub slot, then the shared slot is marked to be cancelled and is
	 * disposed/returned to the owning instance.
	 *
	 * @param slot sub slot which shall be removed from the shared slot
	 */
	public void disposeChild(SimpleSlot slot){
		assignmentGroup.releaseSimpleSlot(slot);
	}

	@Override
	public String toString() {
		return "Shared " + super.toString();
	}
}

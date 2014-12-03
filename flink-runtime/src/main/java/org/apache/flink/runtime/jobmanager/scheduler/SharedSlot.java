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

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * 
 * NOTE: This class does no synchronization by itself and its mutating
 *       methods may only be called from within the synchronization scope of
 *       it associated SlotSharingGroupAssignment.
 */
class SharedSlot implements Serializable {

	static final long serialVersionUID = 42L;

	private final AllocatedSlot allocatedSlot;
	
	private final SlotSharingGroupAssignment assignmentGroup;
	
	private final Set<SubSlot> subSlots;
	
	private int subSlotNumber;
	
	private volatile boolean disposed;
	
	// --------------------------------------------------------------------------------------------
	
	public SharedSlot(AllocatedSlot allocatedSlot, SlotSharingGroupAssignment assignmentGroup) {
		if (allocatedSlot == null || assignmentGroup == null) {
			throw new NullPointerException();
		}
		
		this.allocatedSlot = allocatedSlot;
		this.assignmentGroup = assignmentGroup;
		this.subSlots = new HashSet<SubSlot>();
	}
	
	// --------------------------------------------------------------------------------------------
	
	AllocatedSlot getAllocatedSlot() {
		return this.allocatedSlot;
	}
	
	boolean isDisposed() {
		return disposed;
	}
	
	int getNumberOfAllocatedSubSlots() {
		return this.subSlots.size();
	}
	
	SubSlot allocateSubSlot(JobVertexID jid) {
		if (disposed) {
			return null;
		} else {
			SubSlot ss = new SubSlot(this, subSlotNumber++, jid);
			this.subSlots.add(ss);
			return ss;
		}
	}
	
	void returnAllocatedSlot(SubSlot slot) {
		if (!slot.isReleased()) {
			throw new IllegalArgumentException("SubSlot is not released.");
		}
		
		this.assignmentGroup.releaseSubSlot(slot, this);
	}
	
	int releaseSlot(SubSlot slot) {
		if (!this.subSlots.remove(slot)) {
			throw new IllegalArgumentException("Wrong shared slot for subslot.");
		}
		return subSlots.size();
	}
	
	void dispose() {
		if (subSlots.isEmpty()) {
			disposed = true;
			this.allocatedSlot.releaseSlot();
		} else {
			throw new IllegalStateException("Cannot dispose while subslots are still alive.");
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "Shared " + allocatedSlot.toString();
	}
}

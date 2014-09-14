/**
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

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class SharedSlot {

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
	
	public SharedSlot(AllocatedSlot allocatedSlot) {
		if (allocatedSlot == null) {
			throw new NullPointerException();
		}
		
		this.allocatedSlot = allocatedSlot;
		this.assignmentGroup = null;;
		this.subSlots = new HashSet<SubSlot>();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public AllocatedSlot getAllocatedSlot() {
		return this.allocatedSlot;
	}
	
	public boolean isDisposed() {
		return disposed;
	}
	
	public int getNumberOfAllocatedSubSlots() {
		synchronized (this.subSlots) {
			return this.subSlots.size();
		}
	}
	
	public SubSlot allocateSubSlot(JobVertexID jid) {
		synchronized (this.subSlots) {
			if (isDisposed()) {
				return null;
			} else {
				SubSlot ss = new SubSlot(this, subSlotNumber++, jid);
				this.subSlots.add(ss);
				return ss;
			}
		}
	}
	
	public void rease() {
		synchronized (this.subSlots) {
			disposed = true;
			for (SubSlot ss : subSlots) {
				ss.releaseSlot();
			}
		}
		
		allocatedSlot.releaseSlot();
	}
	
	void returnAllocatedSlot(SubSlot slot) {
		boolean release;
		
		synchronized (this.subSlots) {
			if (!this.subSlots.remove(slot)) {
				throw new IllegalArgumentException("Wrong shared slot for subslot.");
			}
			
			if (assignmentGroup != null) {
				release = assignmentGroup.sharedSlotAvailableForJid(this, slot.getJobVertexId(), this.subSlots.isEmpty());
			} else {
				release = subSlots.isEmpty();
			}
			
			if (release) {
				disposed = true;
			}
		}
		
		// do this call outside the lock, because releasing the allocated slot may go into further scheduler calls
		if (release) {
			this.allocatedSlot.releaseSlot();
		}
	}
}

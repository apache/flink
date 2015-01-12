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

import org.apache.flink.runtime.instance.SimpleSlot;

public class SlotAllocationFuture {
	
	private final Object monitor = new Object();
	
	private volatile SimpleSlot slot;
	
	private volatile SlotAllocationFutureAction action;
	
	// --------------------------------------------------------------------------------------------

	public SlotAllocationFuture() {}
	
	public SlotAllocationFuture(SimpleSlot slot) {
		this.slot = slot;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public SimpleSlot waitTillAllocated() throws InterruptedException {
		return waitTillAllocated(0);
	}
	
	public SimpleSlot waitTillAllocated(long timeout) throws InterruptedException {
		synchronized (monitor) {
			while (slot == null) {
				monitor.wait(timeout);
			}
			
			return slot;
		}
	}
	
	public void setFutureAction(SlotAllocationFutureAction action) {
		synchronized (monitor) {
			if (this.action != null) {
				throw new IllegalStateException("Future already has an action registered.");
			}
			
			this.action = action;
			
			if (this.slot != null) {
				action.slotAllocated(this.slot);
			}
		}
	}
	
	public void setSlot(SimpleSlot slot) {
		if (slot == null) {
			throw new NullPointerException();
		}
		
		synchronized (monitor) {
			if (this.slot != null) {
				throw new IllegalStateException("The future has already been assigned a slot.");
			}
			
			this.slot = slot;
			monitor.notifyAll();
			
			if (action != null) {
				action.slotAllocated(slot);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return slot == null ? "PENDING" : "DONE";
	}
}

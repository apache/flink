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

import org.apache.flink.runtime.AbstractID;
import org.apache.flink.runtime.instance.AllocatedSlot;

public class SubSlot extends AllocatedSlot {

	private static final long serialVersionUID = 1361615219044538497L;
	

	private final SharedSlot sharedSlot;
	
	private final AbstractID groupId;
	
	private final int subSlotNumber;
	
	
	public SubSlot(SharedSlot sharedSlot, int subSlotNumber, AbstractID groupId) {
		super(sharedSlot.getAllocatedSlot().getJobID(),
				sharedSlot.getAllocatedSlot().getInstance(),
				sharedSlot.getAllocatedSlot().getSlotNumber());
		
		this.sharedSlot = sharedSlot;
		this.groupId = groupId;
		this.subSlotNumber = subSlotNumber;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public void releaseSlot() {
		// cancel everything, if there is something. since this is atomically status based,
		// it will not happen twice if another attempt happened before or concurrently
		try {
			cancel();
		}
		finally {
			if (markReleased()) {
				this.sharedSlot.returnAllocatedSlot(this);
			}
		}
	}
	
	public SharedSlot getSharedSlot() {
		return this.sharedSlot;
	}
	
	public AbstractID getGroupId() {
		return groupId;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "SubSlot " + subSlotNumber + " (" + super.toString() + ')';
	}
}

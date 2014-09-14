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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class CoLocationConstraint {
	
	private static final AtomicReferenceFieldUpdater<CoLocationConstraint, SharedSlot> UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(CoLocationConstraint.class, SharedSlot.class, "slot");
	
	private volatile SharedSlot slot;

	
	public boolean isUnassigned() {
		return slot == null;
	}
	
	public SharedSlot getSlot() {
		return slot;
	}
	
	public SharedSlot swapInNewSlot(AllocatedSlot newSlot) {
		SharedSlot newShared = new SharedSlot(newSlot);
		
		// atomic swap/release-other to prevent resource leaks
		while (true) {
			SharedSlot current = this.slot;
			if (UPDATER.compareAndSet(this, current, newShared)) {
				if (current != null) {
					current.rease();
				}
				return newShared;
			}
		}
	}
	
	public SubSlot allocateSubSlot(JobVertexID jid) {
		if (this.slot == null) {
			throw new IllegalStateException("Location constraint has not yet been assigned a slot.");
		}
		
		return slot.allocateSubSlot(jid);
	}
}

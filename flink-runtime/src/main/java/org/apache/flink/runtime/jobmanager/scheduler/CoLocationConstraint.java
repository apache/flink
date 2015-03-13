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

public class CoLocationConstraint {
	
	private final CoLocationGroup group;
	
	private volatile SharedSlot sharedSlot;
	
	
	CoLocationConstraint(CoLocationGroup group) {
		Preconditions.checkNotNull(group);
		this.group = group;
	}
	
	
	public SharedSlot getSharedSlot() {
		return sharedSlot;
	}
	
	public Instance getLocation() {
		if (sharedSlot != null) {
			return sharedSlot.getInstance();
		} else {
			throw new IllegalStateException("Not assigned");
		}
	}
	
	public void setSharedSlot(SharedSlot sharedSlot) {
		if (this.sharedSlot == sharedSlot) {
			return;
		}
		else if (this.sharedSlot == null || this.sharedSlot.isDead()) {
			this.sharedSlot = sharedSlot;
		} else {
			throw new IllegalStateException("Overriding shared slot that is still alive.");
		}
	}
	
	public boolean isUnassigned() {
		return this.sharedSlot == null;
	}
	
	public boolean isUnassignedOrDisposed() {
		return this.sharedSlot == null || this.sharedSlot.isDead();
	}
	
	public AbstractID getGroupId() {
		return this.group.getId();
	}

	@Override
	public String toString() {
		return "CoLocation constraint id " + getGroupId() + " shared slot " + sharedSlot;
	}
	

}

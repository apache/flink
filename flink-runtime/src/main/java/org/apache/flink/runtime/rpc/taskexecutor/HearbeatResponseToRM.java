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
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.runtime.rpc.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.io.Serializable;
import java.util.List;

public class HearbeatResponseToRM implements Serializable {
	private final List<AllocatedSlotDescription> allocatedSlots;
	private final List<AvailableSlotDescription> freeSlots;
	private final ResourceID resourceID;

	public HearbeatResponseToRM(
		List<AllocatedSlotDescription> allocatedSlots,
		List<AvailableSlotDescription> freeSlots,
		ResourceID resourceID) {
		this.allocatedSlots = allocatedSlots;
		this.freeSlots = freeSlots;
		this.resourceID = resourceID;
	}

	public List<AllocatedSlotDescription> getAllocatedSlots() {
		return allocatedSlots;
	}

	public List<AvailableSlotDescription> getFreeSlots() {
		return freeSlots;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}
}

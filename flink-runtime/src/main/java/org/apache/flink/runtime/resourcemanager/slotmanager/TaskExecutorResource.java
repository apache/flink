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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Bookkeeping of total/allocated/available resources of a TaskExecutor.
 */
public class TaskExecutorResource<SlotIdType> {

	/** Total resource of the TaskExecutor. */
	private final ResourceProfile totalResource;

	/** Resources of allocated slots. */
	private final Map<SlotIdType, ResourceProfile> allocatedSlotsResources;

	/** Remaining available resource. */
	private ResourceProfile availableResource;

	public TaskExecutorResource(ResourceProfile totalResource) {
		this.totalResource = totalResource;
		this.allocatedSlotsResources = new HashMap<>();
		this.availableResource = totalResource;
	}

	public ResourceProfile getTotalResource() {
		return totalResource;
	}

	public Map<SlotIdType, ResourceProfile> getAllocatedSlotsResources() {
		return Collections.unmodifiableMap(allocatedSlotsResources);
	}

	public ResourceProfile getAvailableResource() {
		return new ResourceProfile(availableResource);
	}

	public void addSlot(SlotIdType slotID, ResourceProfile slotResource) {
		allocatedSlotsResources.put(slotID, slotResource);
		availableResource = availableResource.subtract(slotResource);
	}

	public void removeSlot(SlotIdType slotID) {
		ResourceProfile slotResource = allocatedSlotsResources.remove(slotID);
		if (slotResource != null) {
			availableResource = availableResource.merge(slotResource);
		}
	}
}

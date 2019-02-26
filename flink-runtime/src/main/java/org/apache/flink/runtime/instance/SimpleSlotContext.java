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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

/**
 * Simple implementation of the {@link SlotContext} interface for the legacy code.
 */
public class SimpleSlotContext implements SlotContext {

	private final AllocationID allocationId;

	private final TaskManagerLocation taskManagerLocation;

	private final int physicalSlotNumber;

	private final TaskManagerGateway taskManagerGateway;

	private final ResourceProfile resourceProfile;

	public SimpleSlotContext(
			AllocationID allocationId,
			TaskManagerLocation taskManagerLocation,
			int physicalSlotNumber,
			TaskManagerGateway taskManagerGateway) {
		this(allocationId, taskManagerLocation, physicalSlotNumber, taskManagerGateway, ResourceProfile.UNKNOWN);
	}

	public SimpleSlotContext(
			AllocationID allocationId,
			TaskManagerLocation taskManagerLocation,
			int physicalSlotNumber,
			TaskManagerGateway taskManagerGateway,
			ResourceProfile resourceProfile) {
		this.allocationId = Preconditions.checkNotNull(allocationId);
		this.taskManagerLocation = Preconditions.checkNotNull(taskManagerLocation);
		this.physicalSlotNumber = physicalSlotNumber;
		this.taskManagerGateway = Preconditions.checkNotNull(taskManagerGateway);
		this.resourceProfile = resourceProfile;
	}

	@Override
	public AllocationID getAllocationId() {
		return allocationId;
	}

	@Override
	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	@Override
	public int getPhysicalSlotNumber() {
		return physicalSlotNumber;
	}

	@Override
	public TaskManagerGateway getTaskManagerGateway() {
		return taskManagerGateway;
	}

	@Override
	public ResourceProfile getResourceProfile() {
		return resourceProfile;
	}
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.slots.DummySlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * Builder for the {@link TestingLogicalSlot}.
 */
public class TestingLogicalSlotBuilder {
	private TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
	private TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
	private int slotNumber = 0;
	private AllocationID allocationId = new AllocationID();
	private SlotRequestId slotRequestId = new SlotRequestId();
	private SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();
	private SlotOwner slotOwner = new DummySlotOwner();
	private boolean automaticallyCompleteReleaseFuture = true;

	public TestingLogicalSlotBuilder setTaskManagerGateway(TaskManagerGateway taskManagerGateway) {
		this.taskManagerGateway = taskManagerGateway;
		return this;
	}

	public TestingLogicalSlotBuilder setTaskManagerLocation(TaskManagerLocation taskManagerLocation) {
		this.taskManagerLocation = taskManagerLocation;
		return this;
	}

	public TestingLogicalSlotBuilder setSlotNumber(int slotNumber) {
		this.slotNumber = slotNumber;
		return this;
	}

	public TestingLogicalSlotBuilder setAllocationId(AllocationID allocationId) {
		this.allocationId = allocationId;
		return this;
	}

	public TestingLogicalSlotBuilder setSlotRequestId(SlotRequestId slotRequestId) {
		this.slotRequestId = slotRequestId;
		return this;
	}

	public TestingLogicalSlotBuilder setSlotSharingGroupId(SlotSharingGroupId slotSharingGroupId) {
		this.slotSharingGroupId = slotSharingGroupId;
		return this;
	}

	public TestingLogicalSlotBuilder setAutomaticallyCompleteReleaseFuture(boolean automaticallyCompleteReleaseFuture) {
		this.automaticallyCompleteReleaseFuture = automaticallyCompleteReleaseFuture;
		return this;
	}

	public TestingLogicalSlotBuilder setSlotOwner(SlotOwner slotOwner) {
		this.slotOwner = slotOwner;
		return this;
	}

	public TestingLogicalSlot createTestingLogicalSlot() {
		return new TestingLogicalSlot(
			taskManagerLocation,
			taskManagerGateway,
			slotNumber,
			allocationId,
			slotRequestId,
			slotSharingGroupId,
			automaticallyCompleteReleaseFuture,
			slotOwner);
	}
}

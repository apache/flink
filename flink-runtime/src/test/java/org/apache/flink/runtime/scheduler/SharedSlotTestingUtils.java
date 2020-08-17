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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlotContext;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

class SharedSlotTestingUtils {
	private SharedSlotTestingUtils() {}

	static ExecutionSlotSharingGroup createExecutionSlotSharingGroup(ExecutionVertexID... executions) {
		ExecutionSlotSharingGroup group = new ExecutionSlotSharingGroup();
		for (ExecutionVertexID execution : executions) {
			group.addVertex(execution);
		}
		return group;
	}

	static class TestingPhysicalSlot extends SimpleSlotContext implements PhysicalSlot {
		@Nullable
		private Payload payload;

		TestingPhysicalSlot(ResourceProfile resourceProfile, AllocationID allocationId) {
			this(
				allocationId,
				new LocalTaskManagerLocation(),
				0,
				new SimpleAckingTaskManagerGateway(),
				resourceProfile);
		}

		TestingPhysicalSlot(
				AllocationID allocationId,
				TaskManagerLocation taskManagerLocation,
				int physicalSlotNumber,
				TaskManagerGateway taskManagerGateway,
				ResourceProfile resourceProfile) {
			super(
				allocationId,
				taskManagerLocation,
				physicalSlotNumber,
				taskManagerGateway,
				resourceProfile);
		}

		@Override
		public boolean tryAssignPayload(Payload payload) {
			this.payload = payload;
			return true;
		}

		@Nullable
		Payload getPayload() {
			return payload;
		}
	}
}

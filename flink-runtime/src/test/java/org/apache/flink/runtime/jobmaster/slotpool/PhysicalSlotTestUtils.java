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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TestingSlotOwner;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;

/**
 * Test utils of {@link PhysicalSlot}.
 */
public class PhysicalSlotTestUtils {

	public static PhysicalSlot createPhysicalSlot() {
		return createPhysicalSlot(ResourceProfile.ANY);
	}

	public static PhysicalSlot createPhysicalSlot(ResourceProfile resourceProfile) {
		return new AllocatedSlot(
			new AllocationID(),
			new LocalTaskManagerLocation(),
			0,
			resourceProfile,
			new SimpleAckingTaskManagerGateway());
	}

	public static LogicalSlot occupyPhysicalSlot(
			final PhysicalSlot physicalSlot,
			final boolean slotWillBeOccupiedIndefinitely) {

		return SingleLogicalSlot.allocateFromPhysicalSlot(
			new SlotRequestId(),
			physicalSlot,
			Locality.UNKNOWN,
			new TestingSlotOwner(),
			slotWillBeOccupiedIndefinitely);
	}

	private PhysicalSlotTestUtils() {
	}
}

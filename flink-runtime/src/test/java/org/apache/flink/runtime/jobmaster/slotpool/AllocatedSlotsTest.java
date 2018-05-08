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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(New.class)
public class AllocatedSlotsTest extends TestLogger {

	@Test
	public void testOperations() throws Exception {
		SlotPool.AllocatedSlots allocatedSlots = new SlotPool.AllocatedSlots();

		final AllocationID allocation1 = new AllocationID();
		final SlotRequestId slotRequestID = new SlotRequestId();
		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		final ResourceID resource1 = taskManagerLocation.getResourceID();
		final AllocatedSlot slot1 = createSlot(allocation1, taskManagerLocation);

		allocatedSlots.add(slotRequestID, slot1);

		assertTrue(allocatedSlots.contains(slot1.getAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.size());

		final AllocationID allocation2 = new AllocationID();
		final SlotRequestId slotRequestID2 = new SlotRequestId();
		final AllocatedSlot slot2 = createSlot(allocation2, taskManagerLocation);

		allocatedSlots.add(slotRequestID2, slot2);

		assertTrue(allocatedSlots.contains(slot1.getAllocationId()));
		assertTrue(allocatedSlots.contains(slot2.getAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(slot2, allocatedSlots.get(allocation2));
		assertEquals(2, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(2, allocatedSlots.size());

		final AllocationID allocation3 = new AllocationID();
		final SlotRequestId slotRequestID3 = new SlotRequestId();
		final TaskManagerLocation taskManagerLocation2 = new LocalTaskManagerLocation();
		final ResourceID resource2 = taskManagerLocation2.getResourceID();
		final AllocatedSlot slot3 = createSlot(allocation3, taskManagerLocation2);

		allocatedSlots.add(slotRequestID3, slot3);

		assertTrue(allocatedSlots.contains(slot1.getAllocationId()));
		assertTrue(allocatedSlots.contains(slot2.getAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(slot2, allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(2, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(3, allocatedSlots.size());

		allocatedSlots.remove(slot2.getAllocationId());

		assertTrue(allocatedSlots.contains(slot1.getAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(2, allocatedSlots.size());

		allocatedSlots.remove(slot1.getAllocationId());

		assertFalse(allocatedSlots.contains(slot1.getAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocationId()));
		assertFalse(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertNull(allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(1, allocatedSlots.size());

		allocatedSlots.remove(slot3.getAllocationId());

		assertFalse(allocatedSlots.contains(slot1.getAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocationId()));
		assertFalse(allocatedSlots.contains(slot3.getAllocationId()));
		assertFalse(allocatedSlots.containResource(resource1));
		assertFalse(allocatedSlots.containResource(resource2));

		assertNull(allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertNull(allocatedSlots.get(allocation3));
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(0, allocatedSlots.size());
	}

	private AllocatedSlot createSlot(final AllocationID allocationId, final TaskManagerLocation taskManagerLocation) {
		return new AllocatedSlot(
			allocationId,
			taskManagerLocation,
			0,
			ResourceProfile.UNKNOWN,
			new SimpleAckingTaskManagerGateway());
	}
}

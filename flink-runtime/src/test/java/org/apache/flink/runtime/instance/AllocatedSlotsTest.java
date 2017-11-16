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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllocatedSlotsTest {

	@Test
	public void testOperations() throws Exception {
		SlotPool.AllocatedSlots allocatedSlots = new SlotPool.AllocatedSlots();

		final AllocationID allocation1 = new AllocationID();
		final SlotPoolGateway.SlotRequestID slotRequestID = new SlotPoolGateway.SlotRequestID();
		final ResourceID resource1 = new ResourceID("resource1");
		final Slot slot1 = createSlot(resource1, allocation1);

		allocatedSlots.add(slotRequestID, slot1);

		assertTrue(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.size());

		final AllocationID allocation2 = new AllocationID();
		final SlotPoolGateway.SlotRequestID slotRequestID2 = new SlotPoolGateway.SlotRequestID();
		final Slot slot2 = createSlot(resource1, allocation2);

		allocatedSlots.add(slotRequestID2, slot2);

		assertTrue(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.contains(slot2.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(slot2, allocatedSlots.get(allocation2));
		assertEquals(2, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(2, allocatedSlots.size());

		final AllocationID allocation3 = new AllocationID();
		final SlotPoolGateway.SlotRequestID slotRequestID3 = new SlotPoolGateway.SlotRequestID();
		final ResourceID resource2 = new ResourceID("resource2");
		final Slot slot3 = createSlot(resource2, allocation3);

		allocatedSlots.add(slotRequestID3, slot3);

		assertTrue(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.contains(slot2.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertEquals(slot2, allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(2, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(3, allocatedSlots.size());

		allocatedSlots.remove(slot2);

		assertTrue(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertEquals(slot1, allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(2, allocatedSlots.size());

		allocatedSlots.remove(slot1);

		assertFalse(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocatedSlot().getSlotAllocationId()));
		assertTrue(allocatedSlots.contains(slot3.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.containResource(resource1));
		assertTrue(allocatedSlots.containResource(resource2));

		assertNull(allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertEquals(slot3, allocatedSlots.get(allocation3));
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(1, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(1, allocatedSlots.size());

		allocatedSlots.remove(slot3);

		assertFalse(allocatedSlots.contains(slot1.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.contains(slot2.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.contains(slot3.getAllocatedSlot().getSlotAllocationId()));
		assertFalse(allocatedSlots.containResource(resource1));
		assertFalse(allocatedSlots.containResource(resource2));

		assertNull(allocatedSlots.get(allocation1));
		assertNull(allocatedSlots.get(allocation2));
		assertNull(allocatedSlots.get(allocation3));
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource1).size());
		assertEquals(0, allocatedSlots.getSlotsForTaskManager(resource2).size());
		assertEquals(0, allocatedSlots.size());
	}

	private Slot createSlot(final ResourceID resourceId, final AllocationID allocationId) {
		AllocatedSlot mockAllocatedSlot = mock(AllocatedSlot.class);
		Slot slot = mock(Slot.class);
		when(slot.getTaskManagerID()).thenReturn(resourceId);
		when(slot.getAllocatedSlot()).thenReturn(mockAllocatedSlot);

		when(mockAllocatedSlot.getSlotAllocationId()).thenReturn(allocationId);
		return slot;
	}
}

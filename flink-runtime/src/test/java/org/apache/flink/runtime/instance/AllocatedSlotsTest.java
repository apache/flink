///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.runtime.instance;
//
//import org.apache.flink.runtime.clusterframework.types.AllocationID;
//import org.apache.flink.runtime.clusterframework.types.ResourceID;
//import org.junit.Test;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertFalse;
//import static org.junit.Assert.assertNull;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.when;
//
//public class AllocatedSlotsTest {
//
//	@Test
//	public void testOperations() throws Exception {
//		SlotPool.AllocatedSlots allocatedSlots = new SlotPool.AllocatedSlots();
//
//		final AllocationID allocation1 = new AllocationID();
//		final ResourceID resource1 = new ResourceID("resource1");
//		final Slot slot1 = createSlot(resource1);
//
//		allocatedSlots.add(allocation1, new SlotDescriptor(slot1), slot1);
//
//		assertTrue(allocatedSlots.contains(slot1));
//		assertTrue(allocatedSlots.containResource(resource1));
//
//		assertEquals(slot1, allocatedSlots.get(allocation1));
//		assertEquals(1, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(1, allocatedSlots.size());
//
//		final AllocationID allocation2 = new AllocationID();
//		final Slot slot2 = createSlot(resource1);
//
//		allocatedSlots.add(allocation2, new SlotDescriptor(slot2), slot2);
//
//		assertTrue(allocatedSlots.contains(slot1));
//		assertTrue(allocatedSlots.contains(slot2));
//		assertTrue(allocatedSlots.containResource(resource1));
//
//		assertEquals(slot1, allocatedSlots.get(allocation1));
//		assertEquals(slot2, allocatedSlots.get(allocation2));
//		assertEquals(2, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(2, allocatedSlots.size());
//
//		final AllocationID allocation3 = new AllocationID();
//		final ResourceID resource2 = new ResourceID("resource2");
//		final Slot slot3 = createSlot(resource2);
//
//		allocatedSlots.add(allocation3, new SlotDescriptor(slot2), slot3);
//
//		assertTrue(allocatedSlots.contains(slot1));
//		assertTrue(allocatedSlots.contains(slot2));
//		assertTrue(allocatedSlots.contains(slot3));
//		assertTrue(allocatedSlots.containResource(resource1));
//		assertTrue(allocatedSlots.containResource(resource2));
//
//		assertEquals(slot1, allocatedSlots.get(allocation1));
//		assertEquals(slot2, allocatedSlots.get(allocation2));
//		assertEquals(slot3, allocatedSlots.get(allocation3));
//		assertEquals(2, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(1, allocatedSlots.getSlotsByResource(resource2).size());
//		assertEquals(3, allocatedSlots.size());
//
//		allocatedSlots.remove(slot2);
//
//		assertTrue(allocatedSlots.contains(slot1));
//		assertFalse(allocatedSlots.contains(slot2));
//		assertTrue(allocatedSlots.contains(slot3));
//		assertTrue(allocatedSlots.containResource(resource1));
//		assertTrue(allocatedSlots.containResource(resource2));
//
//		assertEquals(slot1, allocatedSlots.get(allocation1));
//		assertNull(allocatedSlots.get(allocation2));
//		assertEquals(slot3, allocatedSlots.get(allocation3));
//		assertEquals(1, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(1, allocatedSlots.getSlotsByResource(resource2).size());
//		assertEquals(2, allocatedSlots.size());
//
//		allocatedSlots.remove(slot1);
//
//		assertFalse(allocatedSlots.contains(slot1));
//		assertFalse(allocatedSlots.contains(slot2));
//		assertTrue(allocatedSlots.contains(slot3));
//		assertFalse(allocatedSlots.containResource(resource1));
//		assertTrue(allocatedSlots.containResource(resource2));
//
//		assertNull(allocatedSlots.get(allocation1));
//		assertNull(allocatedSlots.get(allocation2));
//		assertEquals(slot3, allocatedSlots.get(allocation3));
//		assertEquals(0, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(1, allocatedSlots.getSlotsByResource(resource2).size());
//		assertEquals(1, allocatedSlots.size());
//
//		allocatedSlots.remove(slot3);
//
//		assertFalse(allocatedSlots.contains(slot1));
//		assertFalse(allocatedSlots.contains(slot2));
//		assertFalse(allocatedSlots.contains(slot3));
//		assertFalse(allocatedSlots.containResource(resource1));
//		assertFalse(allocatedSlots.containResource(resource2));
//
//		assertNull(allocatedSlots.get(allocation1));
//		assertNull(allocatedSlots.get(allocation2));
//		assertNull(allocatedSlots.get(allocation3));
//		assertEquals(0, allocatedSlots.getSlotsByResource(resource1).size());
//		assertEquals(0, allocatedSlots.getSlotsByResource(resource2).size());
//		assertEquals(0, allocatedSlots.size());
//	}
//
//	private Slot createSlot(final ResourceID resourceId) {
//		Slot slot = mock(Slot.class);
//		when(slot.getTaskManagerID()).thenReturn(resourceId);
//		return slot;
//	}
//}

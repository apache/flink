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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvailableSlotsTest {

	static final ResourceProfile DEFAULT_TESTING_PROFILE = new ResourceProfile(1.0, 512);

	static final ResourceProfile DEFAULT_TESTING_BIG_PROFILE = new ResourceProfile(2.0, 1024);

	@Test
	public void testAddAndRemove() throws Exception {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();

		final ResourceID resource1 = new ResourceID("resource1");
		final ResourceID resource2 = new ResourceID("resource2");

		final SlotDescriptor slot1 = createSlotDescriptor(resource1);
		final SlotDescriptor slot2 = createSlotDescriptor(resource1);
		final SlotDescriptor slot3 = createSlotDescriptor(resource2);

		availableSlots.add(slot1);
		availableSlots.add(slot2);
		availableSlots.add(slot3);

		assertEquals(3, availableSlots.size());
		assertTrue(availableSlots.contains(slot1));
		assertTrue(availableSlots.contains(slot2));
		assertTrue(availableSlots.contains(slot3));
		assertTrue(availableSlots.containResource(resource1));
		assertTrue(availableSlots.containResource(resource2));

		availableSlots.removeByResource(resource1);

		assertEquals(1, availableSlots.size());
		assertFalse(availableSlots.contains(slot1));
		assertFalse(availableSlots.contains(slot2));
		assertTrue(availableSlots.contains(slot3));
		assertFalse(availableSlots.containResource(resource1));
		assertTrue(availableSlots.containResource(resource2));

		availableSlots.removeByResource(resource2);

		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1));
		assertFalse(availableSlots.contains(slot2));
		assertFalse(availableSlots.contains(slot3));
		assertFalse(availableSlots.containResource(resource1));
		assertFalse(availableSlots.containResource(resource2));
	}

	@Test
	public void testPollFreeSlot() {
		SlotPool.AvailableSlots availableSlots = new SlotPool.AvailableSlots();

		final ResourceID resource1 = new ResourceID("resource1");
		final SlotDescriptor slot1 = createSlotDescriptor(resource1);

		availableSlots.add(slot1);

		assertEquals(1, availableSlots.size());
		assertTrue(availableSlots.contains(slot1));
		assertTrue(availableSlots.containResource(resource1));

		assertNull(availableSlots.poll(DEFAULT_TESTING_BIG_PROFILE));

		assertEquals(slot1, availableSlots.poll(DEFAULT_TESTING_PROFILE));
		assertEquals(0, availableSlots.size());
		assertFalse(availableSlots.contains(slot1));
		assertFalse(availableSlots.containResource(resource1));
	}

	static SlotDescriptor createSlotDescriptor(final ResourceID resourceID) {
		return createSlotDescriptor(resourceID, new JobID());
	}

	static SlotDescriptor createSlotDescriptor(final ResourceID resourceID, final JobID jobID) {
		return createSlotDescriptor(resourceID, jobID, DEFAULT_TESTING_PROFILE);
	}

	static SlotDescriptor createSlotDescriptor(final ResourceID resourceID, final JobID jobID,
		final ResourceProfile resourceProfile)
	{
		return createSlotDescriptor(resourceID, jobID, resourceProfile, 0);
	}

	static SlotDescriptor createSlotDescriptor(final ResourceID resourceID, final JobID jobID,
		final ResourceProfile resourceProfile, final int slotNumber)
	{
		TaskManagerLocation location = mock(TaskManagerLocation.class);
		when(location.getResourceID()).thenReturn(resourceID);
		return new SlotDescriptor(jobID, location, slotNumber, resourceProfile, mock(TaskManagerGateway.class));
	}
}

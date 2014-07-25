/**
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

package org.apache.flink.runtime.jobmanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

import static org.junit.Assert.*;

import org.junit.Test;

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class SharedSlotsTest {

	@Test
	public void createAndDoNotRelease() {
		try {
			SlotSharingGroupAssignment assignment = mock(SlotSharingGroupAssignment.class);
			when(assignment.sharedSlotAvailableForJid(any(SharedSlot.class), any(JobVertexID.class), any(boolean.class))).thenReturn(false);
			
			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot slot = new SharedSlot(instance.allocateSlot(new JobID()), assignment);
			assertFalse(slot.isDisposed());
			
			SubSlot ss1 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss1);
			
			// verify resources
			assertEquals(instance, ss1.getInstance());
			assertEquals(0, ss1.getSlotNumber());
			assertEquals(slot.getAllocatedSlot().getJobID(), ss1.getJobID());
			
			SubSlot ss2 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss2);
			
			assertEquals(2, slot.getNumberOfAllocatedSubSlots());
			
			// release first slot, should not trigger release
			ss1.releaseSlot();
			assertFalse(slot.isDisposed());
			
			ss2.releaseSlot();
			assertFalse(slot.isDisposed());
			
			// the shared slot should now dispose itself
			assertEquals(0, slot.getNumberOfAllocatedSubSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void createAndRelease() {
		try {
			SlotSharingGroupAssignment assignment = mock(SlotSharingGroupAssignment.class);
			when(assignment.sharedSlotAvailableForJid(any(SharedSlot.class), any(JobVertexID.class), eq(false))).thenReturn(false);
			when(assignment.sharedSlotAvailableForJid(any(SharedSlot.class), any(JobVertexID.class), eq(true))).thenReturn(true);
			
			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot slot = new SharedSlot(instance.allocateSlot(new JobID()), assignment);
			assertFalse(slot.isDisposed());
			
			SubSlot ss1 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss1);
			
			// verify resources
			assertEquals(instance, ss1.getInstance());
			assertEquals(0, ss1.getSlotNumber());
			assertEquals(slot.getAllocatedSlot().getJobID(), ss1.getJobID());
			
			SubSlot ss2 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss2);
			
			assertEquals(2, slot.getNumberOfAllocatedSubSlots());
			
			// release first slot, should not trigger release
			ss1.releaseSlot();
			assertFalse(slot.isDisposed());
			
			ss2.releaseSlot();
			assertTrue(slot.isDisposed());
			
			// the shared slot should now dispose itself
			assertEquals(0, slot.getNumberOfAllocatedSubSlots());
			
			assertNull(slot.allocateSubSlot(new JobVertexID()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

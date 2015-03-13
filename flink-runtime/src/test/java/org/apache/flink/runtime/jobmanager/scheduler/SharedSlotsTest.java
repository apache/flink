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

package org.apache.flink.runtime.jobmanager.scheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Matchers.any;
import static org.junit.Assert.*;

import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class SharedSlotsTest {

	@Test
	public void createAndDoNotRelease() {
		try {
			SlotSharingGroupAssignment assignment = mock(SlotSharingGroupAssignment.class);
			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					final SimpleSlot simpleSlot = (SimpleSlot) invocation.getArguments()[0];
					final SharedSlot sharedSlot = simpleSlot.getParent();

					sharedSlot.freeSubSlot(simpleSlot);

					return null;
				}
				
			}).when(assignment).releaseSimpleSlot(any(SimpleSlot.class));

			JobVertexID id1 = new JobVertexID();
			
			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot slot = instance.allocateSharedSlot(new JobID(), assignment, id1);
			assertFalse(slot.isDead());
			
			SimpleSlot ss1 = slot.allocateSubSlot(id1);
			assertNotNull(ss1);
			
			// verify resources
			assertEquals(instance, ss1.getInstance());
			assertEquals(0, ss1.getSlotNumber());
			assertEquals(slot.getJobID(), ss1.getJobID());
			
			SimpleSlot ss2 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss2);
			
			assertEquals(2, slot.getNumberLeaves());
			
			// release first slot, should not trigger release
			ss1.releaseSlot();
			assertFalse(slot.isDead());
			
			ss2.releaseSlot();
			assertFalse(slot.isDead());
			
			// the shared slot should now dispose itself
			assertEquals(0, slot.getNumberLeaves());
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
			doAnswer(new Answer<Boolean>() {
				@Override
				public Boolean answer(InvocationOnMock invocation) throws Throwable {
					final SimpleSlot slot = (SimpleSlot) invocation.getArguments()[0];
					final SharedSlot shared = slot.getParent();
					if (shared.freeSubSlot(slot) == 0) {
						shared.markDead();
						return true;
					}
					return false;
				}

			}).when(assignment).releaseSimpleSlot(any(SimpleSlot.class));

			JobVertexID id1 = new JobVertexID();

			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot slot = instance.allocateSharedSlot(new JobID(), assignment, id1);
			assertFalse(slot.isDead());
			
			SimpleSlot ss1 = slot.allocateSubSlot(id1);
			assertNotNull(ss1);
			
			// verify resources
			assertEquals(instance, ss1.getInstance());
			assertEquals(0, ss1.getSlotNumber());
			assertEquals(slot.getJobID(), ss1.getJobID());
			
			SimpleSlot ss2 = slot.allocateSubSlot(new JobVertexID());
			assertNotNull(ss2);
			
			assertEquals(2, slot.getNumberLeaves());
			
			// release first slot, should not trigger release
			ss1.releaseSlot();
			assertFalse(slot.isDead());
			
			ss2.releaseSlot();
			assertTrue(slot.isDead());
			
			// the shared slot should now dispose itself
			assertEquals(0, slot.getNumberLeaves());
			
			assertNull(slot.allocateSubSlot(new JobVertexID()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

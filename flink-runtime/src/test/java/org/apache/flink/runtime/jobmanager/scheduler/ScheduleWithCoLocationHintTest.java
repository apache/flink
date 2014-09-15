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

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;

public class ScheduleWithCoLocationHintTest {

	@Test
	public void scheduleAllSharedAndCoLocated() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			scheduler.newInstanceAvailable(getRandomInstance(2));
			scheduler.newInstanceAvailable(getRandomInstance(2));
			scheduler.newInstanceAvailable(getRandomInstance(2));
			
			assertEquals(6, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationConstraint c1 = new CoLocationConstraint();
			CoLocationConstraint c2 = new CoLocationConstraint();
			CoLocationConstraint c3 = new CoLocationConstraint();
			CoLocationConstraint c4 = new CoLocationConstraint();
			CoLocationConstraint c5 = new CoLocationConstraint();
			CoLocationConstraint c6 = new CoLocationConstraint();
			
			// schedule 4 tasks from the first vertex group
			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 0, 6), sharingGroup, c1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 1, 6), sharingGroup, c2));
			AllocatedSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 2, 6), sharingGroup, c3));
			AllocatedSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 3, 6), sharingGroup, c4));
			AllocatedSlot s5 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 0, 6), sharingGroup, c1));
			AllocatedSlot s6 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 1, 6), sharingGroup, c2));
			AllocatedSlot s7 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 2, 6), sharingGroup, c3));
			AllocatedSlot s8 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 4, 6), sharingGroup, c5));
			AllocatedSlot s9 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 5, 6), sharingGroup, c6));
			AllocatedSlot s10 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 3, 6), sharingGroup, c4));
			AllocatedSlot s11 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 4, 6), sharingGroup, c5));
			AllocatedSlot s12 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 5, 6), sharingGroup, c6));

			assertNotNull(s1);
			assertNotNull(s2);
			assertNotNull(s3);
			assertNotNull(s4);
			assertNotNull(s5);
			assertNotNull(s6);
			assertNotNull(s7);
			assertNotNull(s8);
			assertNotNull(s9);
			assertNotNull(s10);
			assertNotNull(s11);
			assertNotNull(s12);
			
			// check that each slot got exactly two tasks
			assertEquals(2, ((SubSlot) s1).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s2).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s3).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s4).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s5).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s6).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s7).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s8).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s9).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s10).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s11).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s12).getSharedSlot().getNumberOfAllocatedSubSlots());
			
			assertEquals(s1.getInstance(), s5.getInstance());
			assertEquals(s2.getInstance(), s6.getInstance());
			assertEquals(s3.getInstance(), s7.getInstance());
			assertEquals(s4.getInstance(), s10.getInstance());
			assertEquals(s8.getInstance(), s11.getInstance());
			assertEquals(s9.getInstance(), s12.getInstance());
			
			assertEquals(c1.getLocation(), s1.getInstance());
			assertEquals(c2.getLocation(), s2.getInstance());
			assertEquals(c3.getLocation(), s3.getInstance());
			assertEquals(c4.getLocation(), s4.getInstance());
			assertEquals(c5.getLocation(), s8.getInstance());
			assertEquals(c6.getLocation(), s9.getInstance());
			
			// check the scheduler's bookkeeping
			assertEquals(0, scheduler.getNumberOfAvailableSlots());
			
			// the first assignments are unconstrained, co.-scheduling is constrained
			assertEquals(6, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(6, scheduler.getNumberOfUnconstrainedAssignments());
			
			// release some slots, be sure that new available ones come up
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();
			s7.releaseSlot();
			s10.releaseSlot();
			s11.releaseSlot();
			s12.releaseSlot();
			assertTrue(scheduler.getNumberOfAvailableSlots() >= 1);
			
			AllocatedSlot single = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(new JobVertexID(), 0, 1)));
			assertNotNull(single);
			
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s5.releaseSlot();
			s6.releaseSlot();
			s7.releaseSlot();
			s8.releaseSlot();
			s9.releaseSlot();
			s11.releaseSlot();
			s12.releaseSlot();
			
			assertEquals(5, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(6, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(7, scheduler.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void scheduleWithIntermediateRelease() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			JobVertexID jid4 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			CoLocationConstraint c1 = new CoLocationConstraint();
			
			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 0, 1), sharingGroup, c1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 0, 1), sharingGroup, c1));
			
			AllocatedSlot sSolo = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid4, 0, 1)));
			
			Instance loc = s1.getInstance();
			
			s1.releaseSlot();
			s2.releaseSlot();
			sSolo.releaseSlot();
			
			AllocatedSlot sNew = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 0, 1), sharingGroup, c1));
			assertEquals(loc, sNew.getInstance());
			
			assertEquals(2, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(2, scheduler.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void scheduleWithReleaseNoResource() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			CoLocationConstraint c1 = new CoLocationConstraint();
			
			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 0, 1), sharingGroup, c1));
			s1.releaseSlot();
			
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 0, 1)));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 1, 2)));
			
			
			try {
				scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 0, 1), sharingGroup, c1));
				fail("Scheduled even though no resource was available.");
			} catch (NoResourceAvailableException e) {
				// expected
			}
			
			assertEquals(0, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(3, scheduler.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void scheduleMixedCoLocationSlotSharing() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			JobVertexID jid4 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			scheduler.newInstanceAvailable(getRandomInstance(1));
			scheduler.newInstanceAvailable(getRandomInstance(1));
			scheduler.newInstanceAvailable(getRandomInstance(1));
			scheduler.newInstanceAvailable(getRandomInstance(1));
			
			assertEquals(4, scheduler.getNumberOfAvailableSlots());
			
			CoLocationConstraint clc1 = new CoLocationConstraint();
			CoLocationConstraint clc2 = new CoLocationConstraint();
			CoLocationConstraint clc3 = new CoLocationConstraint();
			CoLocationConstraint clc4 = new CoLocationConstraint();
			SlotSharingGroup shareGroup = new SlotSharingGroup();

			// first wave
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 0, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 2, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 1, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid1, 3, 4), shareGroup));
			
			// second wave
			AllocatedSlot s21 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 0, 4), shareGroup, clc1));
			AllocatedSlot s22 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 2, 4), shareGroup, clc2));
			AllocatedSlot s23 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 1, 4), shareGroup, clc3));
			AllocatedSlot s24 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid2, 3, 4), shareGroup, clc4));
			
			// third wave
			AllocatedSlot s31 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 1, 4), shareGroup, clc2));
			AllocatedSlot s32 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 2, 4), shareGroup, clc3));
			AllocatedSlot s33 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 3, 4), shareGroup, clc4));
			AllocatedSlot s34 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid3, 0, 4), shareGroup, clc1));
			
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid4, 0, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid4, 1, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid4, 2, 4), shareGroup));
			scheduler.scheduleImmediately(new ScheduledUnit(getTestVertex(jid4, 3, 4), shareGroup));
			
			assertEquals(s21.getInstance(), s34.getInstance());
			assertEquals(s22.getInstance(), s31.getInstance());
			assertEquals(s23.getInstance(), s32.getInstance());
			assertEquals(s24.getInstance(), s33.getInstance());
			
			assertEquals(4, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(12, scheduler.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

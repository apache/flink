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

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertex;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertexWithLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ScheduleWithCoLocationHintTest {

	private static ActorSystem system;

	@BeforeClass
	public static void setup(){
		system = ActorSystem.create("TestingActorSystem", TestingUtils.testConfig());
		TestingUtils.setCallingThreadDispatcher(system);
	}

	@AfterClass
	public static void teardown(){
		TestingUtils.setGlobalExecutionContext();
		JavaTestKit.shutdownActorSystem(system);
	}

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
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint c1 = new CoLocationConstraint(ccg);
			CoLocationConstraint c2 = new CoLocationConstraint(ccg);
			CoLocationConstraint c3 = new CoLocationConstraint(ccg);
			CoLocationConstraint c4 = new CoLocationConstraint(ccg);
			CoLocationConstraint c5 = new CoLocationConstraint(ccg);
			CoLocationConstraint c6 = new CoLocationConstraint(ccg);
			
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
			CoLocationConstraint c1 = new CoLocationConstraint(new CoLocationGroup());
			
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
			CoLocationConstraint c1 = new CoLocationConstraint(new CoLocationGroup());
			
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
			
			CoLocationGroup grp = new CoLocationGroup();
			CoLocationConstraint clc1 = new CoLocationConstraint(grp);
			CoLocationConstraint clc2 = new CoLocationConstraint(grp);
			CoLocationConstraint clc3 = new CoLocationConstraint(grp);
			CoLocationConstraint clc4 = new CoLocationConstraint(grp);
			
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

	
	@Test
	public void testGetsNonLocalFromSharingGroupFirst() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			// schedule something into the shared group so that both instances are in the sharing group
			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, i1), sharingGroup));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, i2), sharingGroup));
			
			// schedule one locally to instance 1
			AllocatedSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, i1), sharingGroup, cc1));

			// schedule with co location constraint (yet unassigned) and a preference for
			// instance 1, but it can only get instance 2
			AllocatedSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, i1), sharingGroup, cc2));
			
			// schedule something into the assigned co-location constraints and check that they override the
			// other preferences
			AllocatedSlot s5 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid3, 0, 2, i2), sharingGroup, cc1));
			AllocatedSlot s6 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid3, 1, 2, i1), sharingGroup, cc2));
			
			// check that each slot got three
			assertEquals(3, ((SubSlot) s1).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(3, ((SubSlot) s2).getSharedSlot().getNumberOfAllocatedSubSlots());
			
			assertEquals(s1.getInstance(), s3.getInstance());
			assertEquals(s2.getInstance(), s4.getInstance());
			assertEquals(s1.getInstance(), s5.getInstance());
			assertEquals(s2.getInstance(), s6.getInstance());
			
			// check the scheduler's bookkeeping
			assertEquals(0, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(5, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(1, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfUnconstrainedAssignments());
			
			// release some slots, be sure that new available ones come up
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();
			s5.releaseSlot();
			s6.releaseSlot();
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSlotReleasedInBetween() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, i1), sharingGroup, cc1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, i2), sharingGroup, cc2));
			
			s1.releaseSlot();
			s2.releaseSlot();
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());

			AllocatedSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, i2), sharingGroup, cc1));
			AllocatedSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, i1), sharingGroup, cc2));
			
			// still preserves the previous instance mapping)
			assertEquals(i1, s3.getInstance());
			assertEquals(i2, s4.getInstance());
			
			s3.releaseSlot();
			s4.releaseSlot();

			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(4, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSlotReleasedInBetweenAndNoNewLocal() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jidx = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, i1), sharingGroup, cc1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, i2), sharingGroup, cc2));
			
			s1.releaseSlot();
			s2.releaseSlot();
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());

			AllocatedSlot sa = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jidx, 0, 2)));
			AllocatedSlot sb = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jidx, 1, 2)));
			
			try {
				scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, i2), sharingGroup, cc1));
				fail("should not be able to find a resource");
			} catch (NoResourceAvailableException e) {
				// good
			} catch (Exception e) {
				fail("wrong exception");
			}
			
			sa.releaseSlot();
			sb.releaseSlot();

			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
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
	public void testScheduleOutOfOrder() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			// schedule something from the second job vertex id before the first is filled,
			// and give locality preferences that hint at using the same shared slot for both
			// co location constraints (which we seek to prevent)
			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, i1), sharingGroup, cc1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, i1), sharingGroup, cc2));

			AllocatedSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, i1), sharingGroup, cc1));
			AllocatedSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, i1), sharingGroup, cc2));
			
			// check that each slot got three
			assertEquals(2, ((SubSlot) s1).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s2).getSharedSlot().getNumberOfAllocatedSubSlots());
			
			assertEquals(s1.getInstance(), s3.getInstance());
			assertEquals(s2.getInstance(), s4.getInstance());
			
			// check the scheduler's bookkeeping
			assertEquals(0, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(3, scheduler.getNumberOfLocalizedAssignments());
			assertEquals(1, scheduler.getNumberOfNonLocalizedAssignments());
			assertEquals(0, scheduler.getNumberOfUnconstrainedAssignments());
			
			// release some slots, be sure that new available ones come up
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForJid(jid1));
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForJid(jid2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void nonColocationFollowsCoLocation() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			Scheduler scheduler = new Scheduler();
			
			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);
			
			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			AllocatedSlot s1 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, i1), sharingGroup, cc1));
			AllocatedSlot s2 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, i2), sharingGroup, cc2));

			AllocatedSlot s3 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, i1), sharingGroup));
			AllocatedSlot s4 = scheduler.scheduleImmediately(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, i1), sharingGroup));
			
			// check that each slot got three
			assertEquals(2, ((SubSlot) s1).getSharedSlot().getNumberOfAllocatedSubSlots());
			assertEquals(2, ((SubSlot) s2).getSharedSlot().getNumberOfAllocatedSubSlots());
			
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForJid(jid1));
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForJid(jid2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class ScheduleWithCoLocationHintTest {

	@Test
	public void scheduleAllSharedAndCoLocated() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

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
			SimpleSlot s1 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 6), sharingGroup, c1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 6), sharingGroup, c2), false).get();
			SimpleSlot s3 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 6), sharingGroup, c3), false).get();
			SimpleSlot s4 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 6), sharingGroup, c4), false).get();
			SimpleSlot s5 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 6), sharingGroup, c1), false).get();
			SimpleSlot s6 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 6), sharingGroup, c2), false).get();
			SimpleSlot s7 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 6), sharingGroup, c3), false).get();
			SimpleSlot s8 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 6), sharingGroup, c5), false).get();
			SimpleSlot s9 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 5, 6), sharingGroup, c6), false).get();
			SimpleSlot s10 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 6), sharingGroup, c4), false).get();
			SimpleSlot s11 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 4, 6), sharingGroup, c5), false).get();
			SimpleSlot s12 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 5, 6), sharingGroup, c6), false).get();

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
			assertEquals(2, s1.getRoot().getNumberLeaves());
			assertEquals(2, s2.getRoot().getNumberLeaves());
			assertEquals(2, s3.getRoot().getNumberLeaves());
			assertEquals(2, s4.getRoot().getNumberLeaves());
			assertEquals(2, s5.getRoot().getNumberLeaves());
			assertEquals(2, s6.getRoot().getNumberLeaves());
			assertEquals(2, s7.getRoot().getNumberLeaves());
			assertEquals(2, s8.getRoot().getNumberLeaves());
			assertEquals(2, s9.getRoot().getNumberLeaves());
			assertEquals(2, s10.getRoot().getNumberLeaves());
			assertEquals(2, s11.getRoot().getNumberLeaves());
			assertEquals(2, s12.getRoot().getNumberLeaves());

			assertEquals(s1.getTaskManagerID(), s5.getTaskManagerID());
			assertEquals(s2.getTaskManagerID(), s6.getTaskManagerID());
			assertEquals(s3.getTaskManagerID(), s7.getTaskManagerID());
			assertEquals(s4.getTaskManagerID(), s10.getTaskManagerID());
			assertEquals(s8.getTaskManagerID(), s11.getTaskManagerID());
			assertEquals(s9.getTaskManagerID(), s12.getTaskManagerID());

			assertEquals(c1.getLocation(), s1.getTaskManagerLocation());
			assertEquals(c2.getLocation(), s2.getTaskManagerLocation());
			assertEquals(c3.getLocation(), s3.getTaskManagerLocation());
			assertEquals(c4.getLocation(), s4.getTaskManagerLocation());
			assertEquals(c5.getLocation(), s8.getTaskManagerLocation());
			assertEquals(c6.getLocation(), s9.getTaskManagerLocation());

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

			SimpleSlot single = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(new JobVertexID(), 0, 1)), false).get();
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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);

			assertEquals(2, scheduler.getNumberOfAvailableSlots());

			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			CoLocationConstraint c1 = new CoLocationConstraint(new CoLocationGroup());

			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid1, 0, 1), sharingGroup, c1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid2, 0, 1), sharingGroup, c1), false).get();

			SimpleSlot sSolo = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 0, 1)), false).get();

			ResourceID taskManager = s1.getTaskManagerID();

			s1.releaseSlot();
			s2.releaseSlot();
			sSolo.releaseSlot();

			SimpleSlot sNew = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid3, 0, 1), sharingGroup, c1), false).get();
			assertEquals(taskManager, sNew.getTaskManagerID());

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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			scheduler.newInstanceAvailable(i1);
			scheduler.newInstanceAvailable(i2);

			assertEquals(2, scheduler.getNumberOfAvailableSlots());

			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			CoLocationConstraint c1 = new CoLocationConstraint(new CoLocationGroup());

			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid1, 0, 1), sharingGroup, c1), false).get();
			s1.releaseSlot();

			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 1)), false).get();
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 2)), false).get();

			try {
				scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 1), sharingGroup, c1), false).get();
				fail("Scheduled even though no resource was available.");
			} catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
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
			
			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());
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
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 4), shareGroup), false);
			
			// second wave
			SimpleSlot s21 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid2, 0, 4), shareGroup, clc1), false).get();
			SimpleSlot s22 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid2, 2, 4), shareGroup, clc2), false).get();
			SimpleSlot s23 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid2, 1, 4), shareGroup, clc3), false).get();
			SimpleSlot s24 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid2, 3, 4), shareGroup, clc4), false).get();
			
			// third wave
			SimpleSlot s31 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid3, 1, 4), shareGroup, clc2), false).get();
			SimpleSlot s32 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid3, 2, 4), shareGroup, clc3), false).get();
			SimpleSlot s33 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid3, 3, 4), shareGroup, clc4), false).get();
			SimpleSlot s34 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertex(jid3, 0, 4), shareGroup, clc1), false).get();
			
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 0, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 1, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 2, 4), shareGroup), false);
			scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 3, 4), shareGroup), false);
			
			assertEquals(s21.getTaskManagerID(), s34.getTaskManagerID());
			assertEquals(s22.getTaskManagerID(), s31.getTaskManagerID());
			assertEquals(s23.getTaskManagerID(), s32.getTaskManagerID());
			assertEquals(s24.getTaskManagerID(), s33.getTaskManagerID());
			
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
			
			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			TaskManagerLocation loc1 = i1.getTaskManagerLocation();
			TaskManagerLocation loc2 = i2.getTaskManagerLocation();

			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);
			
			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();
			
			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			// schedule something into the shared group so that both instances are in the sharing group
			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, loc1), sharingGroup), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, loc2), sharingGroup), false).get();
			
			// schedule one locally to instance 1
			SimpleSlot s3 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, loc1), sharingGroup, cc1), false).get();

			// schedule with co location constraint (yet unassigned) and a preference for
			// instance 1, but it can only get instance 2
			SimpleSlot s4 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, loc1), sharingGroup, cc2), false).get();
			
			// schedule something into the assigned co-location constraints and check that they override the
			// other preferences
			SimpleSlot s5 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid3, 0, 2, loc2), sharingGroup, cc1), false).get();
			SimpleSlot s6 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid3, 1, 2, loc1), sharingGroup, cc2), false).get();
			
			// check that each slot got three
			assertEquals(3, s1.getRoot().getNumberLeaves());
			assertEquals(3, s2.getRoot().getNumberLeaves());
			
			assertEquals(s1.getTaskManagerID(), s3.getTaskManagerID());
			assertEquals(s2.getTaskManagerID(), s4.getTaskManagerID());
			assertEquals(s1.getTaskManagerID(), s5.getTaskManagerID());
			assertEquals(s2.getTaskManagerID(), s6.getTaskManagerID());
			
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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			TaskManagerLocation loc1 = i1.getTaskManagerLocation();
			TaskManagerLocation loc2 = i2.getTaskManagerLocation();

			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);

			assertEquals(2, scheduler.getNumberOfAvailableSlots());

			SlotSharingGroup sharingGroup = new SlotSharingGroup();

			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, loc1), sharingGroup, cc1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, loc2), sharingGroup, cc2), false).get();

			s1.releaseSlot();
			s2.releaseSlot();

			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());

			SimpleSlot s3 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, loc2), sharingGroup, cc1), false).get();
			SimpleSlot s4 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, loc1), sharingGroup, cc2), false).get();

			// still preserves the previous instance mapping)
			assertEquals(i1.getTaskManagerID(), s3.getTaskManagerID());
			assertEquals(i2.getTaskManagerID(), s4.getTaskManagerID());

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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			TaskManagerLocation loc1 = i1.getTaskManagerLocation();
			TaskManagerLocation loc2 = i2.getTaskManagerLocation();

			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);

			assertEquals(2, scheduler.getNumberOfAvailableSlots());

			SlotSharingGroup sharingGroup = new SlotSharingGroup();

			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, loc1), sharingGroup, cc1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, loc2), sharingGroup, cc2), false).get();

			s1.releaseSlot();
			s2.releaseSlot();

			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());

			SimpleSlot sa = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jidx, 0, 2)), false).get();
			SimpleSlot sb = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jidx, 1, 2)), false).get();

			try {
				scheduler.allocateSlot(
						new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, loc2), sharingGroup, cc1), false).get();
				fail("should not be able to find a resource");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			TaskManagerLocation loc1 = i1.getTaskManagerLocation();

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
			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, loc1), sharingGroup, cc1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, loc1), sharingGroup, cc2), false).get();

			SimpleSlot s3 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, loc1), sharingGroup, cc1), false).get();
			SimpleSlot s4 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, loc1), sharingGroup, cc2), false).get();

			// check that each slot got three
			assertEquals(2, s1.getRoot().getNumberLeaves());
			assertEquals(2, s2.getRoot().getNumberLeaves());

			assertEquals(s1.getTaskManagerID(), s3.getTaskManagerID());
			assertEquals(s2.getTaskManagerID(), s4.getTaskManagerID());

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
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForGroup(jid1));
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForGroup(jid2));
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

			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());

			Instance i1 = getRandomInstance(1);
			Instance i2 = getRandomInstance(1);

			TaskManagerLocation loc1 = i1.getTaskManagerLocation();
			TaskManagerLocation loc2 = i2.getTaskManagerLocation();

			scheduler.newInstanceAvailable(i2);
			scheduler.newInstanceAvailable(i1);

			assertEquals(2, scheduler.getNumberOfAvailableSlots());
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup();

			CoLocationGroup ccg = new CoLocationGroup();
			CoLocationConstraint cc1 = new CoLocationConstraint(ccg);
			CoLocationConstraint cc2 = new CoLocationConstraint(ccg);

			SimpleSlot s1 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, loc1), sharingGroup, cc1), false).get();
			SimpleSlot s2 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, loc2), sharingGroup, cc2), false).get();

			SimpleSlot s3 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, loc1), sharingGroup), false).get();
			SimpleSlot s4 = scheduler.allocateSlot(
					new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, loc1), sharingGroup), false).get();

			// check that each slot got two
			assertEquals(2, s1.getRoot().getNumberLeaves());
			assertEquals(2, s2.getRoot().getNumberLeaves());

			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();

			assertEquals(2, scheduler.getNumberOfAvailableSlots());

			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfSlots());
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForGroup(jid1));
			assertEquals(0, sharingGroup.getTaskAssignment().getNumberOfAvailableSlotsForGroup(jid2));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.areAllDistinct;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getRandomInstance;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertex;
import static org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils.getTestVertexWithLocation;
import static org.apache.flink.runtime.testutils.CommonTestUtils.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the scheduler when scheduling tasks in slot sharing groups.
 */
public class SchedulerSlotSharingTest extends SchedulerTestBase {

	@Test
	public void scheduleSingleVertexType() {
		try {
			JobVertexID jid1 = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1);

			final ResourceID tm1ResourceId = testingSlotProvider.addTaskManager(2).getResourceID();
			testingSlotProvider.addTaskManager(2);
			
			// schedule 4 tasks from the first vertex group
			LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1);
			assertNotNull(s2);
			assertNotNull(s3);
			assertNotNull(s4);
			
			assertTrue(areAllDistinct(s1, s2, s3, s4));
			
			// we cannot schedule another task from the first vertex group
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			// release something
			s3.releaseSlot();
			
			// allocate another slot from that group
			LogicalSlot s5 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(s5);
			
			// release all old slots
			s1.releaseSlot();
			s2.releaseSlot();
			s4.releaseSlot();
			
			LogicalSlot s6 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 5, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s7 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 6, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s8 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 7, 8, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s6);
			assertNotNull(s7);
			assertNotNull(s8);
			
			// make sure we have two slots on the first instance, and two on the second
			int c = 0;
			c += (s5.getTaskManagerLocation().getResourceID().equals(tm1ResourceId)) ? 1 : -1;
			c += (s6.getTaskManagerLocation().getResourceID().equals(tm1ResourceId)) ? 1 : -1;
			c += (s7.getTaskManagerLocation().getResourceID().equals(tm1ResourceId)) ? 1 : -1;
			c += (s8.getTaskManagerLocation().getResourceID().equals(tm1ResourceId)) ? 1 : -1;
			assertEquals(0, c);
			
			// release all
			s5.releaseSlot();
			s6.releaseSlot();
			s7.releaseSlot();
			s8.releaseSlot();
			
			// test that everything is released
			assertEquals(4, testingSlotProvider.getNumberOfAvailableSlots());

			// check the scheduler's bookkeeping
			assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(8, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void allocateSlotWithSharing() throws Exception {
		JobVertexID jid1 = new JobVertexID();
		JobVertexID jid2 = new JobVertexID();

		SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

		testingSlotProvider.addTaskManager(2);
		testingSlotProvider.addTaskManager(2);

		// schedule 4 tasks from the first vertex group
		LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

		assertNotNull(s1);
		assertNotNull(s2);
		assertNotNull(s3);
		assertNotNull(s4);

		assertTrue(areAllDistinct(s1, s2, s3, s4));

		// we cannot schedule another task from the first vertex group
		try {
			testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			fail("Scheduler accepted too many tasks at the same time");
		}
		catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof NoResourceAvailableException);
		}
		catch (Exception e) {
			fail("Wrong exception.");
		}

		// schedule some tasks from the second ID group
		LogicalSlot s1_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s3_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		LogicalSlot s4_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

		assertNotNull(s1_2);
		assertNotNull(s2_2);
		assertNotNull(s3_2);
		assertNotNull(s4_2);

		// we cannot schedule another task from the second vertex group
		try {
			testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			fail("Scheduler accepted too many tasks at the same time");
		}
		catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof NoResourceAvailableException);
		}
		catch (Exception e) {
			fail("Wrong exception.");
		}

		// now, we release some vertices (sub-slots) from the first group.
		// that should allow us to schedule more vertices from the first group
		s1.releaseSlot();
		s4.releaseSlot();

		assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));
		assertEquals(2, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
		assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));

		// we can still not schedule anything from the second group of vertices
		try {
			testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			fail("Scheduler accepted too many tasks at the same time");
		}
		catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof NoResourceAvailableException);
		}
		catch (Exception e) {
			fail("Wrong exception.");
		}

		// we can schedule something from the first vertex group
		LogicalSlot s5 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		assertNotNull(s5);

		assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));
		assertEquals(1, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
		assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));


		// now we release a slot from the second vertex group and schedule another task from that group
		s2_2.releaseSlot();
		LogicalSlot s5_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
		assertNotNull(s5_2);

		// release all slots
		s2.releaseSlot();
		s3.releaseSlot();
		s5.releaseSlot();

		s1_2.releaseSlot();
		s3_2.releaseSlot();
		s4_2.releaseSlot();
		s5_2.releaseSlot();

		// test that everything is released
		assertEquals(0, testingSlotProvider.getNumberOfSlots(sharingGroup));
		assertEquals(4, testingSlotProvider.getNumberOfAvailableSlots());

		// check the scheduler's bookkeeping
		assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
		assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
		assertEquals(10, testingSlotProvider.getNumberOfUnconstrainedAssignments());
	}
	
	@Test
	public void allocateSlotWithIntermediateTotallyEmptySharingGroup() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			testingSlotProvider.addTaskManager(2);
			testingSlotProvider.addTaskManager(2);

			// schedule 4 tasks from the first vertex group
			LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(4, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));
			
			s1.releaseSlot();
			s2.releaseSlot();
			s3.releaseSlot();
			s4.releaseSlot();
			
			assertEquals(0, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));
			
			// schedule some tasks from the second ID group
			LogicalSlot s1_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

			assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(4, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));
			
			s1_2.releaseSlot();
			s2_2.releaseSlot();
			s3_2.releaseSlot();
			s4_2.releaseSlot();
			
			assertEquals(0, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));

			// test that everything is released
			assertEquals(4, testingSlotProvider.getNumberOfAvailableSlots());
			
			// check the scheduler's bookkeeping
			assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(8, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void allocateSlotWithTemporarilyEmptyVertexGroup() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2, jid3);

			testingSlotProvider.addTaskManager(2);
			testingSlotProvider.addTaskManager(2);

			// schedule 4 tasks from the first vertex group
			LogicalSlot s1_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1_1);
			assertNotNull(s2_1);
			assertNotNull(s3_1);
			assertNotNull(s4_1);
			
			assertTrue(areAllDistinct(s1_1, s2_1, s3_1, s4_1));
			
			// schedule 4 tasks from the second vertex group
			LogicalSlot s1_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1_2);
			assertNotNull(s2_2);
			assertNotNull(s3_2);
			assertNotNull(s4_2);
			
			assertTrue(areAllDistinct(s1_2, s2_2, s3_2, s4_2));
			
			// schedule 4 tasks from the third vertex group
			LogicalSlot s1_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1_3);
			assertNotNull(s2_3);
			assertNotNull(s3_3);
			assertNotNull(s4_3);
			
			assertTrue(areAllDistinct(s1_3, s2_3, s3_3, s4_3));
			
			
			// we cannot schedule another task from the second vertex group
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			// release the second vertex group
			s1_2.releaseSlot();
			s2_2.releaseSlot();
			s3_2.releaseSlot();
			s4_2.releaseSlot();
			
			LogicalSlot s5_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 5, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s6_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 6, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s7_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 7, 7, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s5_2);
			assertNotNull(s6_2);
			assertNotNull(s7_2);
			
			// release the slots
			s1_1.releaseSlot();
			s2_1.releaseSlot();
			s3_1.releaseSlot();
			s4_1.releaseSlot();
			
			s5_2.releaseSlot();
			s6_2.releaseSlot();
			s7_2.releaseSlot();
			
			// test that everything is released
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlots());
			
			s1_3.releaseSlot();
			s2_3.releaseSlot();
			s3_3.releaseSlot();
			s4_3.releaseSlot();
			
			// test that everything is released
			assertEquals(4, testingSlotProvider.getNumberOfAvailableSlots());
			
			// check the scheduler's bookkeeping
			assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(15, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void allocateSlotWithTemporarilyEmptyVertexGroup2() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			testingSlotProvider.addTaskManager(2);

			// schedule 1 tasks from the first vertex group and 2 from the second
			LogicalSlot s1_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 2, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 2, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 2, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1_1);
			assertNotNull(s2_1);
			assertNotNull(s2_2);
			
			assertEquals(2, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(1, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));
			
			// release the two from the second
			s2_1.releaseSlot();
			s2_2.releaseSlot();
			
			
			// this should free one slot so we can allocate one non-shared
			LogicalSlot sx = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 1, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sx);
			
			assertEquals(1, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(0, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid1));
			assertEquals(1, testingSlotProvider.getNumberOfAvailableSlotsForGroup(sharingGroup, jid2));
			
			// check the scheduler's bookkeeping
			assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(4, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void scheduleMixedSharingAndNonSharing() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			
			JobVertexID jidA = new JobVertexID();
			JobVertexID jidB= new JobVertexID();
			JobVertexID jidC = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			testingSlotProvider.addTaskManager(3);
			testingSlotProvider.addTaskManager(2);

			// schedule some individual vertices
			LogicalSlot sA2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidA, 1, 2, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot sA1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidA, 0, 2, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sA1);
			assertNotNull(sA2);
			
			// schedule some vertices in the sharing group
			LogicalSlot s1_0 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s1_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_0 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(s1_0);
			assertNotNull(s1_1);
			assertNotNull(s2_0);
			assertNotNull(s2_1);
			
			// schedule another isolated vertex
			LogicalSlot sB1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidB, 1, 3, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sB1);
			
			// should not be able to schedule more vertices
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidB, 0, 3, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			try {
				testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidC, 0, 1, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("Scheduler accepted too many tasks at the same time");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}
			catch (Exception e) {
				fail("Wrong exception.");
			}
			
			// release some isolated task and check that the sharing group may grow
			sA1.releaseSlot();
			
			LogicalSlot s1_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(s1_2);
			assertNotNull(s2_2);
			
			// release three of the previously allocated sub slots, which guarantees to return one shared slot
			s1_0.releaseSlot();
			s1_1.releaseSlot();
			s2_0.releaseSlot();
			
			assertEquals(1, testingSlotProvider.getNumberOfAvailableSlots());
			
			// schedule one more no-shared task
			LogicalSlot sB0 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidB, 0, 3, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sB0);
			
			// release the last of the original shared slots and allocate one more non-shared slot
			s2_1.releaseSlot();
			LogicalSlot sB2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidB, 2, 3, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sB2);
			
			
			// release on non-shared and add some shared slots
			sA2.releaseSlot();
			LogicalSlot s1_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(s1_3);
			assertNotNull(s2_3);
			
			// release all shared and allocate all in non-shared
			s1_2.releaseSlot();
			s2_2.releaseSlot();
			s1_3.releaseSlot();
			s2_3.releaseSlot();
			
			LogicalSlot sC0 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidC, 1, 2, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot sC1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jidC, 0, 2, null)), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			assertNotNull(sC0);
			assertNotNull(sC1);
			
			
			sB0.releaseSlot();
			sB1.releaseSlot();
			sB2.releaseSlot();
			sC0.releaseSlot();
			sC1.releaseSlot();
			
			// test that everything is released
			assertEquals(5, testingSlotProvider.getNumberOfAvailableSlots());
			
			// check the scheduler's bookkeeping
			assertEquals(0, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(15, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * Tests that the scheduler assigns the correct existing shared slots
	 */
	@Test
	public void testLocalizedAssignment1() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			TaskManagerLocation loc1 = testingSlotProvider.addTaskManager(2);
			TaskManagerLocation loc2 = testingSlotProvider.addTaskManager(2);
			
			// schedule one to each instance
			LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, sharingGroup, loc2), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc2), TestingUtils.infiniteTime()).get();
			assertNotNull(s1);
			assertNotNull(s2);
			
			assertEquals(2, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(loc1, s1.getTaskManagerLocation());
			assertEquals(loc2, s2.getTaskManagerLocation());
			
			// schedule one from the other group to each instance
			LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, sharingGroup, loc2), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc2), TestingUtils.infiniteTime()).get();
			assertNotNull(s3);
			assertNotNull(s4);
			
			assertEquals(2, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(loc1, s3.getTaskManagerLocation());
			assertEquals(loc2, s4.getTaskManagerLocation());
			assertEquals(2, testingSlotProvider.getNumberOfAvailableSlots());
			
			// check the scheduler's bookkeeping
			assertEquals(4, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * Tests that the scheduler assigns to new local slots, rather than to existing non-local slots
	 */
	@Test
	public void testLocalizedAssignment2() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			TaskManagerLocation loc1 = testingSlotProvider.addTaskManager(2);
			TaskManagerLocation loc2 = testingSlotProvider.addTaskManager(2);

			// schedule one to each instance
			LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			assertNotNull(s1);
			assertNotNull(s2);
			
			assertEquals(2, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(loc1, s1.getTaskManagerLocation());
			assertEquals(loc1, s2.getTaskManagerLocation());
			
			// schedule one from the other group to each instance
			LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 2, sharingGroup, loc2), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc2), TestingUtils.infiniteTime()).get();
			LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 2, sharingGroup, loc2), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc2), TestingUtils.infiniteTime()).get();
			assertNotNull(s3);
			assertNotNull(s4);
			
			assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));
			assertEquals(loc2, s3.getTaskManagerLocation());
			assertEquals(loc2, s4.getTaskManagerLocation());
			
			// check the scheduler's bookkeeping
			assertEquals(4, testingSlotProvider.getNumberOfLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfNonLocalizedAssignments());
			assertEquals(0, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * Tests that the scheduler can fall back to non-local
	 */
	@Test
	public void testLocalizedAssignment3() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2);

			TaskManagerLocation loc1 = testingSlotProvider.addTaskManager(2);
			TaskManagerLocation loc2 = testingSlotProvider.addTaskManager(2);

			// schedule until the one instance is full
			LogicalSlot s1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 0, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid1, 1, 2, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 0, 4, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 1, 4, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();

			// schedule two more with preference of same instance --> need to go to other instance
			LogicalSlot s5 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 3, 4, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			LogicalSlot s6 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertexWithLocation(jid2, 4, 4, sharingGroup, loc1), sharingGroup.getSlotSharingGroupId()), false, slotProfileForLocation(loc1), TestingUtils.infiniteTime()).get();
			
			assertNotNull(s1);
			assertNotNull(s2);
			assertNotNull(s3);
			assertNotNull(s4);
			assertNotNull(s5);
			assertNotNull(s6);
			
			assertEquals(4, testingSlotProvider.getNumberOfSlots(sharingGroup));

			assertEquals(loc1, s1.getTaskManagerLocation());
			assertEquals(loc1, s2.getTaskManagerLocation());
			assertEquals(loc1, s3.getTaskManagerLocation());
			assertEquals(loc1, s4.getTaskManagerLocation());
			assertEquals(loc2, s5.getTaskManagerLocation());
			assertEquals(loc2, s6.getTaskManagerLocation());
			
			// check the scheduler's bookkeeping
			assertEquals(4, testingSlotProvider.getNumberOfLocalizedAssignments());
			// Flink supports host localized assignments which happen in this case because all TaskManagerLocations point to the loopback address
			assertTrue(2 == testingSlotProvider.getNumberOfNonLocalizedAssignments() || 2 == testingSlotProvider.getNumberOfHostLocalizedAssignments());

			assertEquals(0, testingSlotProvider.getNumberOfUnconstrainedAssignments());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSequentialAllocateAndRelease() {
		try {
			final JobVertexID jid1 = new JobVertexID();
			final JobVertexID jid2 = new JobVertexID();
			final JobVertexID jid3 = new JobVertexID();
			final JobVertexID jid4 = new JobVertexID();
			
			final SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2, jid3, jid4);
			
			testingSlotProvider.addTaskManager(4);

			// allocate something from group 1 and 2 interleaved with schedule for group 3
			LogicalSlot slot_1_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_1_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

			LogicalSlot slot_2_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_2_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			LogicalSlot slot_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 1, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			LogicalSlot slot_1_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_1_4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			LogicalSlot slot_2_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_2_4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			// release groups 1 and 2
			
			slot_1_1.releaseSlot();
			slot_1_2.releaseSlot();
			slot_1_3.releaseSlot();
			slot_1_4.releaseSlot();
			
			slot_2_1.releaseSlot();
			slot_2_2.releaseSlot();
			slot_2_3.releaseSlot();
			slot_2_4.releaseSlot();
			
			// allocate group 4
			
			LogicalSlot slot_4_1 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_4_2 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_4_3 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot slot_4_4 = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			// release groups 3 and 4
			
			slot_3.releaseSlot();
			
			slot_4_1.releaseSlot();
			slot_4_2.releaseSlot();
			slot_4_3.releaseSlot();
			slot_4_4.releaseSlot();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testConcurrentAllocateAndRelease() {
		final ExecutorService executor = Executors.newFixedThreadPool(20);

		try {
			testingSlotProvider.addTaskManager(4);

			for (int run = 0; run < 50; run++) {
				final JobVertexID jid1 = new JobVertexID();
				final JobVertexID jid2 = new JobVertexID();
				final JobVertexID jid3 = new JobVertexID();
				final JobVertexID jid4 = new JobVertexID();
				
				final SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2, jid3, jid4);

				final AtomicInteger enumerator1 = new AtomicInteger();
				final AtomicInteger enumerator2 = new AtomicInteger();
				final AtomicBoolean flag3 = new AtomicBoolean();
				final AtomicInteger enumerator4 = new AtomicInteger();
				
				final Random rnd = new Random();
				
				// use atomic boolean as a mutable boolean reference
				final AtomicBoolean failed = new AtomicBoolean(false);
				
				// use atomic integer as a mutable integer reference
				final AtomicInteger completed = new AtomicInteger();
				
				final Runnable deploy4 = new Runnable() {
					@Override
					public void run() {
						try {
							LogicalSlot slot = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid4, enumerator4.getAndIncrement(), 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
							sleepUninterruptibly(rnd.nextInt(5));
							slot.releaseSlot();

							if (completed.incrementAndGet() == 13) {
								synchronized (completed) {
									completed.notifyAll();
								}
							}
						}
						catch (Throwable t) {
							t.printStackTrace();
							failed.set(true);
						}
					}
				};
				
				final Runnable deploy3 = new Runnable() {
					@Override
					public void run() {
						try {
							if (flag3.compareAndSet(false, true)) {
								LogicalSlot slot = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 1, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
								sleepUninterruptibly(5);
								
								executor.execute(deploy4);
								executor.execute(deploy4);
								executor.execute(deploy4);
								executor.execute(deploy4);
								
								slot.releaseSlot();
								
								if (completed.incrementAndGet() == 13) {
									synchronized (completed) {
										completed.notifyAll();
									}
								}
							}
						}
						catch (Throwable t) {
							t.printStackTrace();
							failed.set(true);
						}
					}
				};
				
				final Runnable deploy2 = new Runnable() {
					@Override
					public void run() {
						try {
							LogicalSlot slot = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid2, enumerator2.getAndIncrement(), 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

							// wait a bit till scheduling the successor
							sleepUninterruptibly(rnd.nextInt(5));
							executor.execute(deploy3);
							
							// wait a bit until release
							sleepUninterruptibly(rnd.nextInt(5));
							slot.releaseSlot();
							
							if (completed.incrementAndGet() == 13) {
								synchronized (completed) {
									completed.notifyAll();
								}
							}
						}
						catch (Throwable t) {
							t.printStackTrace();
							failed.set(true);
						}
					}
				};
				
				final Runnable deploy1 = new Runnable() {
					@Override
					public void run() {
						try {
							LogicalSlot slot = testingSlotProvider.allocateSlot(new ScheduledUnit(getTestVertex(jid1, enumerator1.getAndIncrement(), 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();

							// wait a bit till scheduling the successor
							sleepUninterruptibly(rnd.nextInt(5));
							executor.execute(deploy2);
							
							// wait a bit until release
							sleepUninterruptibly(rnd.nextInt(5));
							slot.releaseSlot();
							
							if (completed.incrementAndGet() == 13) {
								synchronized (completed) {
									completed.notifyAll();
								}
							}
						}
						catch (Throwable t) {
							t.printStackTrace();
							failed.set(true);
						}
					}
				};
				
				final Runnable deploy0 = new Runnable() {
					@Override
					public void run() {
						sleepUninterruptibly(rnd.nextInt(10));
						executor.execute(deploy1);
					}
				};
				executor.execute(deploy0);
				executor.execute(deploy0);
				executor.execute(deploy0);
				executor.execute(deploy0);
				
				// wait until all tasks have finished
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (completed) {
					while (!failed.get() && completed.get() < 13) {
						completed.wait(1000);
					}
				}
				
				assertFalse("Thread failed", failed.get());
				
				while (testingSlotProvider.getNumberOfAvailableSlots() < 4) {
					sleepUninterruptibly(5);
				}
				
				assertEquals(4, testingSlotProvider.getNumberOfAvailableSlots());
				assertEquals(13 * (run + 1), testingSlotProvider.getNumberOfUnconstrainedAssignments());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testDopIncreases() {
		try {
			JobVertexID jid1 = new JobVertexID();
			JobVertexID jid2 = new JobVertexID();
			JobVertexID jid3 = new JobVertexID();
			JobVertexID jid4 = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(jid1, jid2, jid3, jid4);
			
			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());
			scheduler.newInstanceAvailable(getRandomInstance(4));
			
			// schedule one task for the first and second vertex
			LogicalSlot s1 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid1, 0, 1, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s2 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid2, 0, 1, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			assertEquals( s1.getTaskManagerLocation(), s2.getTaskManagerLocation() );
			assertEquals(3, scheduler.getNumberOfAvailableSlots());
			
			LogicalSlot s3_0 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 0, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_1 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 1, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_0 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 0, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_1 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 1, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			s1.releaseSlot();
			s2.releaseSlot();
			
			LogicalSlot s3_2 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 2, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s3_3 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 3, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_2 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 2, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			LogicalSlot s4_3 = scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid4, 3, 4, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
			
			try {
				scheduler.allocateSlot(new ScheduledUnit(getTestVertex(jid3, 4, 5, sharingGroup), sharingGroup.getSlotSharingGroupId()), false, SlotProfile.noRequirements(), TestingUtils.infiniteTime()).get();
				fail("should throw an exception");
			}
			catch (ExecutionException e) {
				assertTrue(e.getCause() instanceof NoResourceAvailableException);
			}

			assertEquals(0, scheduler.getNumberOfAvailableSlots());
			
			s3_0.releaseSlot();
			s3_1.releaseSlot();
			s3_2.releaseSlot();
			s3_3.releaseSlot();
			s4_0.releaseSlot();
			s4_1.releaseSlot();
			s4_2.releaseSlot();
			s4_3.releaseSlot();
			
			assertEquals(4, scheduler.getNumberOfAvailableSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static SlotProfile slotProfileForLocation(TaskManagerLocation location) {
		return new SlotProfile(ResourceProfile.UNKNOWN, Collections.singletonList(location), Collections.emptyList());
	}
}

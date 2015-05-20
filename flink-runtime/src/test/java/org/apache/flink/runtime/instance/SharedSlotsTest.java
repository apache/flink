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
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.util.AbstractID;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Tests for the allocation, properties, and release of shared slots.
 */
public class SharedSlotsTest {
	
	@Test
	public void allocateAndReleaseEmptySlot() {
		try {
			JobID jobId = new JobID();
			JobVertexID vertexId = new JobVertexID();
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(vertexId);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();
			
			assertEquals(0, assignment.getNumberOfSlots());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vertexId));
			
			Instance instance = SchedulerTestUtils.getRandomInstance(2);
			
			assertEquals(2, instance.getTotalNumberOfSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
			assertEquals(2, instance.getNumberOfAvailableSlots());
			
			// allocate a shared slot
			SharedSlot slot = instance.allocateSharedSlot(jobId, assignment);
			assertEquals(2, instance.getTotalNumberOfSlots());
			assertEquals(1, instance.getNumberOfAllocatedSlots());
			assertEquals(1, instance.getNumberOfAvailableSlots());
			
			// check that the new slot is fresh
			assertTrue(slot.isAlive());
			assertFalse(slot.isCanceled());
			assertFalse(slot.isReleased());
			assertEquals(0, slot.getNumberLeaves());
			assertFalse(slot.hasChildren());
			assertTrue(slot.isRootAndEmpty());
			assertNotNull(slot.toString());
			assertTrue(slot.getSubSlots().isEmpty());
			assertEquals(0, slot.getSlotNumber());
			assertEquals(0, slot.getRootSlotNumber());
			
			// release the slot immediately.
			slot.releaseSlot();

			assertTrue(slot.isCanceled());
			assertTrue(slot.isReleased());
			
			// the slot sharing group and instance should not
			assertEquals(2, instance.getTotalNumberOfSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
			assertEquals(2, instance.getNumberOfAvailableSlots());

			assertEquals(0, assignment.getNumberOfSlots());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vertexId));
			
			// we should not be able to allocate any children from this released slot
			assertNull(slot.allocateSharedSlot(new AbstractID()));
			assertNull(slot.allocateSubSlot(new AbstractID()));
			
			// we cannot add this slot to the assignment group
			assertNull(assignment.addSharedSlotAndAllocateSubSlot(slot, Locality.NON_LOCAL, vertexId));
			assertEquals(0, assignment.getNumberOfSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateSimpleSlotsAndReleaseFromRoot() {
		try {
			JobID jobId = new JobID();
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			JobVertexID vid3 = new JobVertexID();
			JobVertexID vid4 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(vid1, vid2, vid3, vid4);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();

			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			// allocate a shared slot
			SharedSlot sharedSlot = instance.allocateSharedSlot(jobId, assignment);
			
			// allocate a series of sub slots
			
			SimpleSlot sub1 = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.LOCAL, vid1);
			assertNotNull(sub1);
			
			assertNull(sub1.getExecutedVertex());
			assertEquals(Locality.LOCAL, sub1.getLocality());
			assertEquals(1, sub1.getNumberLeaves());
			assertEquals(vid1, sub1.getGroupID());
			assertEquals(instance, sub1.getInstance());
			assertEquals(jobId, sub1.getJobID());
			assertEquals(sharedSlot, sub1.getParent());
			assertEquals(sharedSlot, sub1.getRoot());
			assertEquals(0, sub1.getRootSlotNumber());
			assertEquals(0, sub1.getSlotNumber());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid4));
			
			SimpleSlot sub2 = assignment.getSlotForTask(vid2, Collections.<Instance>emptySet());
			assertNotNull(sub2);
			
			assertNull(sub2.getExecutedVertex());
			assertEquals(Locality.UNCONSTRAINED, sub2.getLocality());
			assertEquals(1, sub2.getNumberLeaves());
			assertEquals(vid2, sub2.getGroupID());
			assertEquals(instance, sub2.getInstance());
			assertEquals(jobId, sub2.getJobID());
			assertEquals(sharedSlot, sub2.getParent());
			assertEquals(sharedSlot, sub2.getRoot());
			assertEquals(0, sub2.getRootSlotNumber());
			assertEquals(1, sub2.getSlotNumber());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid4));
			
			SimpleSlot sub3 = assignment.getSlotForTask(vid3, Collections.singleton(instance));
			assertNotNull(sub3);
			
			assertNull(sub3.getExecutedVertex());
			assertEquals(Locality.LOCAL, sub3.getLocality());
			assertEquals(1, sub3.getNumberLeaves());
			assertEquals(vid3, sub3.getGroupID());
			assertEquals(instance, sub3.getInstance());
			assertEquals(jobId, sub3.getJobID());
			assertEquals(sharedSlot, sub3.getParent());
			assertEquals(sharedSlot, sub3.getRoot());
			assertEquals(0, sub3.getRootSlotNumber());
			assertEquals(2, sub3.getSlotNumber());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid4));

			SimpleSlot sub4 = assignment.getSlotForTask(vid4,
					Collections.singleton(SchedulerTestUtils.getRandomInstance(1)));
			assertNotNull(sub4);
			
			assertNull(sub4.getExecutedVertex());
			assertEquals(Locality.NON_LOCAL, sub4.getLocality());
			assertEquals(1, sub4.getNumberLeaves());
			assertEquals(vid4, sub4.getGroupID());
			assertEquals(instance, sub4.getInstance());
			assertEquals(jobId, sub4.getJobID());
			assertEquals(sharedSlot, sub4.getParent());
			assertEquals(sharedSlot, sub4.getRoot());
			assertEquals(0, sub4.getRootSlotNumber());
			assertEquals(3, sub4.getSlotNumber());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid4));
			
			// release from the root.
			sharedSlot.releaseSlot();

			assertTrue(sharedSlot.isReleased());
			assertTrue(sub1.isReleased());
			assertTrue(sub2.isReleased());
			assertTrue(sub3.isReleased());
			assertTrue(sub4.isReleased());
			
			assertEquals(0, sharedSlot.getNumberLeaves());
			assertFalse(sharedSlot.hasChildren());
			
			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, assignment.getNumberOfSlots());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid4));

			assertNull(sharedSlot.allocateSharedSlot(new AbstractID()));
			assertNull(sharedSlot.allocateSubSlot(new AbstractID()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateSimpleSlotsAndReleaseFromleaves() {
		try {
			JobID jobId = new JobID();
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			JobVertexID vid3 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(vid1, vid2, vid3);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();

			Instance instance = SchedulerTestUtils.getRandomInstance(1);

			// allocate a shared slot
			SharedSlot sharedSlot = instance.allocateSharedSlot(jobId, assignment);

			// allocate a series of sub slots

			SimpleSlot sub1 = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.UNCONSTRAINED, vid1);
			SimpleSlot sub2 = assignment.getSlotForTask(vid2, Collections.<Instance>emptySet());
			SimpleSlot sub3 = assignment.getSlotForTask(vid3, Collections.<Instance>emptySet());
			
			assertNotNull(sub1);
			assertNotNull(sub2);
			assertNotNull(sub3);

			assertEquals(3, sharedSlot.getNumberLeaves());

			assertEquals(1, assignment.getNumberOfSlots());
			
			// release from the leaves.
			
			sub2.releaseSlot();

			assertTrue(sharedSlot.isAlive());
			assertTrue(sub1.isAlive());
			assertTrue(sub2.isReleased());
			assertTrue(sub3.isAlive());
			
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfSlots());
			
			assertEquals(2, sharedSlot.getNumberLeaves());

			
			sub1.releaseSlot();

			assertTrue(sharedSlot.isAlive());
			assertTrue(sub1.isReleased());
			assertTrue(sub2.isReleased());
			assertTrue(sub3.isAlive());
			
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfSlots());
			
			assertEquals(1, sharedSlot.getNumberLeaves());

			sub3.releaseSlot();

			assertTrue(sharedSlot.isReleased());
			assertTrue(sub1.isReleased());
			assertTrue(sub2.isReleased());
			assertTrue(sub3.isReleased());

			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(0, assignment.getNumberOfSlots());
			
			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, assignment.getNumberOfSlots());

			assertNull(sharedSlot.allocateSharedSlot(new AbstractID()));
			assertNull(sharedSlot.allocateSubSlot(new AbstractID()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void allocateAndReleaseInMixedOrder() {
		try {
			JobID jobId = new JobID();
			JobVertexID vid1 = new JobVertexID();
			JobVertexID vid2 = new JobVertexID();
			JobVertexID vid3 = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(vid1, vid2, vid3);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();

			Instance instance = SchedulerTestUtils.getRandomInstance(1);

			// allocate a shared slot
			SharedSlot sharedSlot = instance.allocateSharedSlot(jobId, assignment);

			// allocate a series of sub slots

			SimpleSlot sub1 = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.UNCONSTRAINED, vid1);
			SimpleSlot sub2 = assignment.getSlotForTask(vid2, Collections.<Instance>emptySet());

			assertNotNull(sub1);
			assertNotNull(sub2);

			assertEquals(2, sharedSlot.getNumberLeaves());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfSlots());
			
			
			sub2.releaseSlot();

			assertEquals(1, sharedSlot.getNumberLeaves());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfSlots());
			
			
			SimpleSlot sub3 = assignment.getSlotForTask(vid3, Collections.<Instance>emptySet());
			assertNotNull(sub3);
			
			assertEquals(2, sharedSlot.getNumberLeaves());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(1, assignment.getNumberOfSlots());
			
			sub3.releaseSlot();
			sub1.releaseSlot();

			assertTrue(sharedSlot.isReleased());
			assertEquals(0, sharedSlot.getNumberLeaves());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid1));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid2));
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(vid3));
			assertEquals(0, assignment.getNumberOfSlots());
			
			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, assignment.getNumberOfSlots());

			assertNull(sharedSlot.allocateSharedSlot(new AbstractID()));
			assertNull(sharedSlot.allocateSubSlot(new AbstractID()));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	/**
	 * We allocate and release the structure below, starting by allocating a simple slot in the
	 * shared slot and finishing by releasing a simple slot.
	 * 
	 * <pre>
	 *     Shared(0)(root)
	 *        |
	 *        +-- Simple(2)(sink)
	 *        |
	 *        +-- Shared(1)(co-location-group)
	 *        |      |
	 *        |      +-- Simple(0)(tail)
	 *        |      +-- Simple(1)(head)
	 *        |
	 *        +-- Simple(0)(source)
	 * </pre>
	 */
	@Test
	public void testAllocateAndReleaseTwoLevels() {
		try {
			JobVertexID sourceId = new JobVertexID();
			JobVertexID headId = new JobVertexID();
			JobVertexID tailId = new JobVertexID();
			JobVertexID sinkId = new JobVertexID();

			AbstractJobVertex headVertex = new AbstractJobVertex("head", headId);
			AbstractJobVertex tailVertex = new AbstractJobVertex("tail", tailId);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(sourceId, headId, tailId, sinkId);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();
			assertEquals(0, assignment.getNumberOfSlots());
			
			CoLocationGroup coLocationGroup = new CoLocationGroup(headVertex, tailVertex);
			CoLocationConstraint constraint = coLocationGroup.getLocationConstraint(0);
			assertFalse(constraint.isAssigned());
			
			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			// allocate a shared slot
			SharedSlot sharedSlot = instance.allocateSharedSlot(new JobID(), assignment);
			
			// get the first simple slot
			SimpleSlot sourceSlot = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.LOCAL, sourceId);
			
			assertEquals(1, sharedSlot.getNumberLeaves());
			
			// get the first slot in the nested shared slot from the co-location constraint
			SimpleSlot headSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			assertEquals(2, sharedSlot.getNumberLeaves());

			assertNotNull(constraint.getSharedSlot());
			assertTrue(constraint.getSharedSlot().isAlive());
			assertFalse(constraint.isAssigned());
			
			// we do not immediately lock the location
			headSlot.releaseSlot();
			assertEquals(1, sharedSlot.getNumberLeaves());

			assertNotNull(constraint.getSharedSlot());
			assertTrue(constraint.getSharedSlot().isReleased());
			assertFalse(constraint.isAssigned());
			
			// re-allocate the head slot
			headSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			
			constraint.lockLocation();
			assertNotNull(constraint.getSharedSlot());
			assertTrue(constraint.isAssigned());
			assertTrue(constraint.isAssignedAndAlive());
			assertEquals(instance, constraint.getLocation());
			
			SimpleSlot tailSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			
			assertEquals(constraint.getSharedSlot(), headSlot.getParent());
			assertEquals(constraint.getSharedSlot(), tailSlot.getParent());
			
			SimpleSlot sinkSlot = assignment.getSlotForTask(sinkId, Collections.<Instance>emptySet());
			assertEquals(4, sharedSlot.getNumberLeaves());
			
			// we release our co-location constraint tasks
			headSlot.releaseSlot();
			tailSlot.releaseSlot();

			assertEquals(2, sharedSlot.getNumberLeaves());
			assertTrue(headSlot.isReleased());
			assertTrue(tailSlot.isReleased());
			assertTrue(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			assertEquals(instance, constraint.getLocation());
			
			// we should have resources again for the co-location constraint
			assertEquals(1, assignment.getNumberOfAvailableSlotsForGroup(constraint.getGroupId()));
			
			// re-allocate head and tail from the constraint
			headSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			tailSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			
			assertEquals(4, sharedSlot.getNumberLeaves());
			assertEquals(0, assignment.getNumberOfAvailableSlotsForGroup(constraint.getGroupId()));
			
			// verify some basic properties of the slots
			assertEquals(instance, sourceSlot.getInstance());
			assertEquals(instance, headSlot.getInstance());
			assertEquals(instance, tailSlot.getInstance());
			assertEquals(instance, sinkSlot.getInstance());

			assertEquals(sourceId, sourceSlot.getGroupID());
			assertEquals(sinkId, sinkSlot.getGroupID());
			assertNull(headSlot.getGroupID());
			assertNull(tailSlot.getGroupID());
			assertEquals(constraint.getGroupId(), constraint.getSharedSlot().getGroupID());
			
			// release all
			sourceSlot.releaseSlot();
			headSlot.releaseSlot();
			tailSlot.releaseSlot();
			sinkSlot.releaseSlot();
			
			assertTrue(sharedSlot.isReleased());
			assertTrue(sourceSlot.isReleased());
			assertTrue(headSlot.isReleased());
			assertTrue(tailSlot.isReleased());
			assertTrue(sinkSlot.isReleased());
			assertTrue(constraint.getSharedSlot().isReleased());
			
			assertTrue(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, assignment.getNumberOfSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * We allocate and the structure below and release it from the root.
	 *
	 * <pre>
	 *     Shared(0)(root)
	 *        |
	 *        +-- Simple(2)(sink)
	 *        |
	 *        +-- Shared(1)(co-location-group)
	 *        |      |
	 *        |      +-- Simple(0)(tail)
	 *        |      +-- Simple(1)(head)
	 *        |
	 *        +-- Simple(0)(source)
	 * </pre>
	 */
	@Test
	public void testReleaseTwoLevelsFromRoot() {
		try {
			JobVertexID sourceId = new JobVertexID();
			JobVertexID headId = new JobVertexID();
			JobVertexID tailId = new JobVertexID();
			JobVertexID sinkId = new JobVertexID();

			AbstractJobVertex headVertex = new AbstractJobVertex("head", headId);
			AbstractJobVertex tailVertex = new AbstractJobVertex("tail", tailId);

			SlotSharingGroup sharingGroup = new SlotSharingGroup(sourceId, headId, tailId, sinkId);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();
			assertEquals(0, assignment.getNumberOfSlots());

			CoLocationGroup coLocationGroup = new CoLocationGroup(headVertex, tailVertex);
			CoLocationConstraint constraint = coLocationGroup.getLocationConstraint(0);
			assertFalse(constraint.isAssigned());

			Instance instance = SchedulerTestUtils.getRandomInstance(1);

			// allocate a shared slot
			SharedSlot sharedSlot = instance.allocateSharedSlot(new JobID(), assignment);

			// get the first simple slot
			SimpleSlot sourceSlot = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.LOCAL, sourceId);
			
			SimpleSlot headSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			constraint.lockLocation();
			SimpleSlot tailSlot = assignment.getSlotForTask(constraint, Collections.<Instance>emptySet());
			
			SimpleSlot sinkSlot = assignment.getSlotForTask(sinkId, Collections.<Instance>emptySet());
			
			assertEquals(4, sharedSlot.getNumberLeaves());

			// release all
			sourceSlot.releaseSlot();
			headSlot.releaseSlot();
			tailSlot.releaseSlot();
			sinkSlot.releaseSlot();

			assertTrue(sharedSlot.isReleased());
			assertTrue(sourceSlot.isReleased());
			assertTrue(headSlot.isReleased());
			assertTrue(tailSlot.isReleased());
			assertTrue(sinkSlot.isReleased());
			assertTrue(constraint.getSharedSlot().isReleased());

			assertTrue(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());

			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
			
			assertEquals(0, assignment.getNumberOfSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	@Test
	public void testImmediateReleaseOneLevel() {
		try {
			JobID jobId = new JobID();
			JobVertexID vid = new JobVertexID();

			SlotSharingGroup sharingGroup = new SlotSharingGroup(vid);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();

			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot sharedSlot = instance.allocateSharedSlot(jobId, assignment);

			SimpleSlot sub = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.UNCONSTRAINED, vid);
			sub.releaseSlot();
			
			assertTrue(sub.isReleased());
			assertTrue(sharedSlot.isReleased());
			
			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testImmediateReleaseTwoLevel() {
		try {
			JobID jobId = new JobID();
			JobVertexID vid = new JobVertexID();
			AbstractJobVertex vertex = new AbstractJobVertex("vertex", vid);
			
			SlotSharingGroup sharingGroup = new SlotSharingGroup(vid);
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();

			CoLocationGroup coLocationGroup = new CoLocationGroup(vertex);
			CoLocationConstraint constraint = coLocationGroup.getLocationConstraint(0);
			
			Instance instance = SchedulerTestUtils.getRandomInstance(1);
			
			SharedSlot sharedSlot = instance.allocateSharedSlot(jobId, assignment);

			SimpleSlot sub = assignment.addSharedSlotAndAllocateSubSlot(sharedSlot, Locality.UNCONSTRAINED, constraint);
			
			assertNull(sub.getGroupID());
			assertEquals(constraint.getSharedSlot(), sub.getParent());
			
			sub.releaseSlot();

			assertTrue(sub.isReleased());
			assertTrue(sharedSlot.isReleased());

			assertEquals(1, instance.getNumberOfAvailableSlots());
			assertEquals(0, instance.getNumberOfAllocatedSlots());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

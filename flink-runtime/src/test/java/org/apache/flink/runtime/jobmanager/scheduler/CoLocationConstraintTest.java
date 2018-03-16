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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.SharedSlot;
import org.apache.flink.runtime.instance.SlotSharingGroupAssignment;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for the {@link CoLocationConstraint}.
 */
public class CoLocationConstraintTest {
	
	@Test
	public void testCreateConstraints() {
		try {
			JobVertexID id1 = new JobVertexID();
			JobVertexID id2 = new JobVertexID();

			JobVertex vertex1 = new JobVertex("vertex1", id1);
			vertex1.setParallelism(2);
			
			JobVertex vertex2 = new JobVertex("vertex2", id2);
			vertex2.setParallelism(3);
			
			CoLocationGroup group = new CoLocationGroup(vertex1, vertex2);
			
			AbstractID groupId = group.getId();
			assertNotNull(groupId);
			
			CoLocationConstraint constraint1 = group.getLocationConstraint(0);
			CoLocationConstraint constraint2 = group.getLocationConstraint(1);
			CoLocationConstraint constraint3 = group.getLocationConstraint(2);
			
			assertFalse(constraint1 == constraint2);
			assertFalse(constraint1 == constraint3);
			assertFalse(constraint2 == constraint3);
			
			assertEquals(groupId, constraint1.getGroupId());
			assertEquals(groupId, constraint2.getGroupId());
			assertEquals(groupId, constraint3.getGroupId());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAssignSlotAndLockLocation() {
		try {
			JobID jid = new JobID();
					
			JobVertex vertex = new JobVertex("vertex");
			vertex.setParallelism(1);

			SlotSharingGroup sharingGroup = new SlotSharingGroup(vertex.getID());
			SlotSharingGroupAssignment assignment = sharingGroup.getTaskAssignment();
			
			CoLocationGroup constraintGroup = new CoLocationGroup(vertex);
			CoLocationConstraint constraint = constraintGroup.getLocationConstraint(0);

			// constraint is completely unassigned
			assertFalse(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			Instance instance1 = SchedulerTestUtils.getRandomInstance(2);
			Instance instance2 = SchedulerTestUtils.getRandomInstance(2);
			
			SharedSlot slot1_1 = instance1.allocateSharedSlot(assignment);
			SharedSlot slot1_2 = instance1.allocateSharedSlot(assignment);
			SharedSlot slot2_1 = instance2.allocateSharedSlot(assignment);
			SharedSlot slot2_2 = instance2.allocateSharedSlot(assignment);
			
			// constraint is still completely unassigned
			assertFalse(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			// set the slot, but do not lock the location yet
			constraint.setSharedSlot(slot1_1);

			assertFalse(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			// try to get the location
			try {
				constraint.getLocation();
				fail("should throw an IllegalStateException");
			}
			catch (IllegalStateException e) {
				// as expected
			}
			catch (Exception e) {
				fail("wrong exception, should be IllegalStateException");
			}

			// check that we can reassign the slot as long as the location is not locked
			constraint.setSharedSlot(slot2_1);
			
			// the previous slot should have been released now
			assertTrue(slot1_1.isReleased());

			// still the location is not assigned
			assertFalse(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			// we can do an identity re-assign
			constraint.setSharedSlot(slot2_1);
			assertFalse(slot2_1.isReleased());

			// still the location is not assigned
			assertFalse(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			
			constraint.lockLocation();

			// now, the location is assigned and we have a location
			assertTrue(constraint.isAssigned());
			assertTrue(constraint.isAssignedAndAlive());
			assertEquals(instance2.getTaskManagerLocation(), constraint.getLocation());
			
			// release the slot
			slot2_1.releaseSlot();

			// we should still have a location
			assertTrue(constraint.isAssigned());
			assertFalse(constraint.isAssignedAndAlive());
			assertEquals(instance2.getTaskManagerLocation(), constraint.getLocation());

			// we can not assign a different location
			try {
				constraint.setSharedSlot(slot1_2);
				fail("should throw an IllegalArgumentException");
			}
			catch (IllegalArgumentException e) {
				// as expected
			}
			catch (Exception e) {
				fail("wrong exception, should be IllegalArgumentException");
			}
			
			// assign a new slot with the same location
			constraint.setSharedSlot(slot2_2);

			assertTrue(constraint.isAssigned());
			assertTrue(constraint.isAssignedAndAlive());
			assertEquals(instance2.getTaskManagerLocation(), constraint.getLocation());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}

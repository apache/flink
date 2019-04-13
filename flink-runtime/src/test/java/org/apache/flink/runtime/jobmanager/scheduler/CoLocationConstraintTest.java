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

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.AbstractID;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

/**
 * Tests for the {@link CoLocationConstraint}.
 */
public class CoLocationConstraintTest {

	@Test
	public void testCreateConstraints() {
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

	@Test
	public void testLockLocation() {
		JobVertex vertex = new JobVertex("vertex");
		vertex.setParallelism(1);

		CoLocationGroup constraintGroup = new CoLocationGroup(vertex);
		CoLocationConstraint constraint = constraintGroup.getLocationConstraint(0);

		// constraint is completely unassigned
		assertThat(constraint.getSlotRequestId(), is(nullValue()));
		assertThat(constraint.isAssigned(), is(false));

		// set the slot, but do not lock the location yet
		SlotRequestId slotRequestId = new SlotRequestId();
		constraint.setSlotRequestId(slotRequestId);
		assertThat(constraint.isAssigned(), is(false));

		// try to get the location
		try {
			constraint.getLocation();
			fail("should throw an IllegalStateException");
		} catch (IllegalStateException e) {
			// as expected
		} catch (Exception e) {
			fail("wrong exception, should be IllegalStateException");
		}

		TaskManagerLocation location = new LocalTaskManagerLocation();
		constraint.lockLocation(location);

		// now, the location is assigned and we have a location
		assertThat(constraint.isAssigned(), is(true));
		assertThat(constraint.getLocation(), is(location));

		// we can not lock a different location
		try {
			TaskManagerLocation anotherLocation = new LocalTaskManagerLocation();
			constraint.lockLocation(anotherLocation);
			fail("should throw an IllegalStateException");
		} catch (IllegalStateException e) {
			// as expected
		} catch (Exception e) {
			fail("wrong exception, should be IllegalStateException");
		}

		constraint.setSlotRequestId(null);
		assertThat(constraint.isAssigned(), is(true));
		assertThat(constraint.getLocation(), is(location));
	}
}

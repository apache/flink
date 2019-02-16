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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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
		assertThat(groupId, not(nullValue()));

		CoLocationConstraint constraint1 = group.getLocationConstraint(0);
		CoLocationConstraint constraint2 = group.getLocationConstraint(1);
		CoLocationConstraint constraint3 = group.getLocationConstraint(2);

		assertThat(constraint1, not(constraint2));
		assertThat(constraint1, not(constraint3));
		assertThat(constraint2, not(constraint3));

		assertThat(constraint1.getGroupId(), is(groupId));
		assertThat(constraint2.getGroupId(), is(groupId));
		assertThat(constraint3.getGroupId(), is(groupId));
	}

	@Test
	public void testLockLocation() {
		JobVertex vertex = new JobVertex("vertex");
		vertex.setParallelism(1);

		CoLocationGroup constraintGroup = new CoLocationGroup(vertex);
		CoLocationConstraint constraint = constraintGroup.getLocationConstraint(0);

		assertThat(constraint.getSlotRequestId(), is(nullValue()));
		assertThat(constraint.isAssigned(), is(false));

		SlotRequestId slotRequestId1 = new SlotRequestId();
		constraint.setSlotRequestId(slotRequestId1);
		assertThat(constraint.getSlotRequestId(), is(slotRequestId1));
		assertThat(constraint.isAssigned(), is(false));

		SlotRequestId slotRequestId2 = new SlotRequestId();
		constraint.setSlotRequestId(slotRequestId2);
		assertThat(constraint.getSlotRequestId(), is(slotRequestId2));
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
		assertThat(constraint.getSlotRequestId(), is(nullValue()));
		assertThat(constraint.isAssigned(), is(true));
		assertThat(constraint.getLocation(), is(location));
	}
}

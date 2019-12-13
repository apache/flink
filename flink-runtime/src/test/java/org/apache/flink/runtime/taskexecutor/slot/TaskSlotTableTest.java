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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link TaskSlotTable}.
 */
public class TaskSlotTableTest extends TestLogger {

	private static final Time SLOT_TIMEOUT = Time.seconds(100L);

	/**
	 * Tests that one can can mark allocated slots as active.
	 */
	@Test
	public void testTryMarkSlotActive() throws SlotNotFoundException {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(3);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId1 = new JobID();
			final AllocationID allocationId1 = new AllocationID();
			taskSlotTable.allocateSlot(0, jobId1, allocationId1, SLOT_TIMEOUT);
			final AllocationID allocationId2 = new AllocationID();
			taskSlotTable.allocateSlot(1, jobId1, allocationId2, SLOT_TIMEOUT);
			final AllocationID allocationId3 = new AllocationID();
			final JobID jobId2 = new JobID();
			taskSlotTable.allocateSlot(2, jobId2, allocationId3, SLOT_TIMEOUT);

			taskSlotTable.markSlotActive(allocationId1);

			assertThat(taskSlotTable.isAllocated(0, jobId1, allocationId1), is(true));
			assertThat(taskSlotTable.isAllocated(1, jobId1, allocationId2), is(true));
			assertThat(taskSlotTable.isAllocated(2, jobId2, allocationId3), is(true));

			assertThat(IteratorUtils.toList(taskSlotTable.getActiveSlots(jobId1)), is(equalTo(Arrays.asList(allocationId1))));

			assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId1), is(true));
			assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId2), is(true));
			assertThat(taskSlotTable.tryMarkSlotActive(jobId1, allocationId3), is(false));

			assertThat(Sets.newHashSet(taskSlotTable.getActiveSlots(jobId1)), is(equalTo(new HashSet<>(Arrays.asList(allocationId2, allocationId1)))));
		} finally {
			taskSlotTable.stop();
			assertThat(taskSlotTable.isStopped(), is(true));
		}
	}

	/**
	 * Tests that redundant slot allocation with the same AllocationID to a different slot is rejected.
	 */
	@Test
	public void testRedundantSlotAllocation() {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId = new AllocationID();

			assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId, SLOT_TIMEOUT), is(true));
			assertThat(taskSlotTable.allocateSlot(1, jobId, allocationId, SLOT_TIMEOUT), is(false));

			assertThat(taskSlotTable.isAllocated(0, jobId, allocationId), is(true));
			assertThat(taskSlotTable.isSlotFree(1), is(true));

			Iterator<TaskSlot> allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
			assertThat(allocatedSlots.next().getIndex(), is(0));
			assertThat(allocatedSlots.hasNext(), is(false));
		} finally {
			taskSlotTable.stop();
		}
	}

	@Test
	public void testFreeSlot() throws SlotNotFoundException {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId1 = new AllocationID();
			final AllocationID allocationId2 = new AllocationID();

			assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId1, SLOT_TIMEOUT), is(true));
			assertThat(taskSlotTable.allocateSlot(1, jobId, allocationId2, SLOT_TIMEOUT), is(true));

			assertThat(taskSlotTable.freeSlot(allocationId2), is(1));

			Iterator<TaskSlot> allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
			assertThat(allocatedSlots.next().getIndex(), is(0));
			assertThat(allocatedSlots.hasNext(), is(false));
			assertThat(taskSlotTable.isAllocated(1, jobId, allocationId1), is(false));
			assertThat(taskSlotTable.isAllocated(1, jobId, allocationId2), is(false));
			assertThat(taskSlotTable.isSlotFree(1), is(true));
		} finally {
			taskSlotTable.stop();
		}
	}

	@Test
	public void testSlotAllocationWithDynamicSlotId() {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId = new AllocationID();

			assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, SLOT_TIMEOUT), is(true));

			Iterator<TaskSlot> allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
			assertThat(allocatedSlots.next().getIndex(), is(-1));
			assertThat(allocatedSlots.hasNext(), is(false));
			assertThat(taskSlotTable.isAllocated(-1, jobId, allocationId), is(true));
		} finally {
			taskSlotTable.stop();
		}
	}

	@Test
	public void testSlotAllocationWithResourceProfile() {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId = new AllocationID();
			final ResourceProfile resourceProfile = TaskSlotUtils.DEFAULT_RESOURCE_PROFILE
				.merge(ResourceProfile.newBuilder().setCpuCores(0.1).build());

			assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, resourceProfile, SLOT_TIMEOUT), is(true));

			Iterator<TaskSlot> allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
			TaskSlot allocatedSlot = allocatedSlots.next();
			assertThat(allocatedSlot.getIndex(), is(-1));
			assertThat(allocatedSlot.getResourceProfile(), is(resourceProfile));
			assertThat(allocatedSlots.hasNext(), is(false));
		} finally {
			taskSlotTable.stop();
		}
	}

	@Test
	public void testSlotAllocationWithResourceProfileFailure() {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(2);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId = new AllocationID();
			ResourceProfile resourceProfile = TaskSlotUtils.DEFAULT_RESOURCE_PROFILE;
			resourceProfile = resourceProfile.merge(resourceProfile).merge(resourceProfile);

			assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId, resourceProfile, SLOT_TIMEOUT), is(false));

			Iterator<TaskSlot> allocatedSlots = taskSlotTable.getAllocatedSlots(jobId);
			assertThat(allocatedSlots.hasNext(), is(false));
		} finally {
			taskSlotTable.stop();
		}
	}

	@Test
	public void testGenerateSlotReport() throws SlotNotFoundException {
		final TaskSlotTable taskSlotTable = TaskSlotUtils.createTaskSlotTable(3);

		try {
			taskSlotTable.start(new TestingSlotActionsBuilder().build());

			final JobID jobId = new JobID();
			final AllocationID allocationId1 = new AllocationID();
			final AllocationID allocationId2 = new AllocationID();
			final AllocationID allocationId3 = new AllocationID();

			assertThat(taskSlotTable.allocateSlot(0, jobId, allocationId1, SLOT_TIMEOUT), is(true)); // index 0
			assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId2, SLOT_TIMEOUT), is(true)); // index 3
			assertThat(taskSlotTable.allocateSlot(-1, jobId, allocationId3, SLOT_TIMEOUT), is(true)); // index 4

			assertThat(taskSlotTable.freeSlot(allocationId2), is(-1));

			ResourceID resourceId = ResourceID.generate();
			SlotReport slotReport = taskSlotTable.createSlotReport(resourceId);
			List<SlotStatus> slotStatuses = new ArrayList<>();
			slotReport.iterator().forEachRemaining(slotStatuses::add);

			assertThat(slotStatuses.size(), is(4));
			assertThat(slotStatuses, containsInAnyOrder(
				is(new SlotStatus(new SlotID(resourceId, 0), TaskSlotUtils.DEFAULT_RESOURCE_PROFILE, jobId, allocationId1)),
				is(new SlotStatus(new SlotID(resourceId, 1), TaskSlotUtils.DEFAULT_RESOURCE_PROFILE, null, null)),
				is(new SlotStatus(new SlotID(resourceId, 2), TaskSlotUtils.DEFAULT_RESOURCE_PROFILE, null, null)),
				is(new SlotStatus(SlotID.generateDynamicSlotID(resourceId), TaskSlotUtils.DEFAULT_RESOURCE_PROFILE, jobId, allocationId3))));
		} finally {
			taskSlotTable.stop();
		}
	}
}

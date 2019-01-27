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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Test the actions on a slot table.
 */
public class TaskSlotTableTest {

	@Test
	public void testAllocateAndFree() throws SlotNotFoundException {
		JobID jobID = new JobID();
		AllocationID allocationID1 = new AllocationID();
		AllocationID allocationID2 = new AllocationID();

		final int totalCPU = 3;
		final int totalMemory = 1000;
		final int firstCPU = 1;
		final int firstMemory = 220;
		final int secondCPU = 2;
		final int secondMemory = 320;

		TaskSlotTable taskSlotTable = new TaskSlotTable(
			Arrays.asList(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
			new ResourceProfile(totalCPU, totalMemory),
			mock(TimerService.class)
		);

		taskSlotTable.start(mock(SlotActions.class));

		// The first slot
		taskSlotTable.allocateSlot(0,
			jobID,
			allocationID1,
			new ResourceProfile(firstCPU, firstMemory),
			Collections.emptyList(),
			Time.seconds(2));

		List<TaskSlot> allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 1);
		assertEquals(new ResourceProfile(firstCPU, firstMemory), allocated.get(0).getAllocationResourceProfile());

		// Un-satisfiable allocation should be fail
		assertFalse(taskSlotTable.allocateSlot(1,
			jobID,
			allocationID2,
			new ResourceProfile(totalCPU, totalMemory),
			Collections.emptyList(),
			Time.seconds(2)));

		// The second slot
		taskSlotTable.allocateSlot(1,
			jobID,
			allocationID2,
			new ResourceProfile(secondCPU, secondMemory),
			Collections.emptyList(),
			Time.seconds(2));

		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 2);
		for (TaskSlot taskSlot : allocated) {
			if (taskSlot.getAllocationId().equals(allocationID1)) {
				assertEquals(new ResourceProfile(firstCPU, firstMemory), taskSlot.getAllocationResourceProfile());
			} else if (taskSlot.getAllocationId().equals(allocationID2)) {
				assertEquals(new ResourceProfile(secondCPU, secondMemory), taskSlot.getAllocationResourceProfile());
			} else {
				fail("Unknown allocation");
			}
		}

		// Free the first
		taskSlotTable.freeSlot(allocationID1);
		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 1);
		assertEquals(new ResourceProfile(secondCPU, secondMemory), allocated.get(0).getAllocationResourceProfile());

		// Free the second
		taskSlotTable.freeSlot(allocationID2);
		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 0);
	}

	@Test
	public void testAllocateAndFreeWithUnknowTotalResource() throws SlotNotFoundException {
		JobID jobID = new JobID();
		AllocationID allocationID1 = new AllocationID();
		AllocationID allocationID2 = new AllocationID();

		final int firstCPU = 1;
		final int firstMemory = 220;
		final int secondCPU = 2;
		final int secondMemory = 320;

		TaskSlotTable taskSlotTable = new TaskSlotTable(
			Arrays.asList(ResourceProfile.UNKNOWN, ResourceProfile.UNKNOWN),
			ResourceProfile.UNKNOWN,
			mock(TimerService.class)
		);

		taskSlotTable.start(mock(SlotActions.class));

		// The first slot
		taskSlotTable.allocateSlot(0,
			jobID,
			allocationID1,
			new ResourceProfile(firstCPU, firstMemory),
			Collections.emptyList(),
			Time.seconds(2));

		List<TaskSlot> allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 1);
		assertEquals(new ResourceProfile(firstCPU, firstMemory), allocated.get(0).getAllocationResourceProfile());

		// The second slot
		taskSlotTable.allocateSlot(1,
			jobID,
			allocationID2,
			new ResourceProfile(secondCPU, secondMemory),
			Collections.emptyList(),
			Time.seconds(2));

		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 2);
		for (TaskSlot taskSlot : allocated) {
			if (taskSlot.getAllocationId().equals(allocationID1)) {
				assertEquals(new ResourceProfile(firstCPU, firstMemory), taskSlot.getAllocationResourceProfile());
			} else if (taskSlot.getAllocationId().equals(allocationID2)) {
				assertEquals(new ResourceProfile(secondCPU, secondMemory), taskSlot.getAllocationResourceProfile());
			} else {
				fail("Unknown allocation");
			}
		}

		// Free the first
		taskSlotTable.freeSlot(allocationID1);
		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 1);
		assertEquals(new ResourceProfile(secondCPU, secondMemory), allocated.get(0).getAllocationResourceProfile());

		// Free the second
		taskSlotTable.freeSlot(allocationID2);
		allocated = Lists.newArrayList(taskSlotTable.getAllocatedSlots(jobID));
		assertEquals(allocated.size(), 0);
	}
}

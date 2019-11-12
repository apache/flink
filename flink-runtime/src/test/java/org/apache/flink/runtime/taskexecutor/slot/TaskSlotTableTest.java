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
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

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
		final TaskSlotTable taskSlotTable = createTaskSlotTable(Collections.nCopies(3, ResourceProfile.UNKNOWN));

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
		}
	}

	/**
	 * Tests that redundant slot allocation with the same AllocationID to a different slot is rejected.
	 */
	@Test
	public void testRedundantSlotAllocation() {
		final TaskSlotTable taskSlotTable = createTaskSlotTable(Collections.nCopies(2, ResourceProfile.UNKNOWN));

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

	@Nonnull
	private TaskSlotTable createTaskSlotTable(final Collection<ResourceProfile> resourceProfiles) {
		return new TaskSlotTable(
			resourceProfiles,
			new TimerService<>(TestingUtils.defaultExecutor(), 10000L));
	}

}

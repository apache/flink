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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class SlotSharingGroupAssignmentTest extends TestLogger {

	/**
	 * Tests that slots are allocated in a round robin fashion from the set of available resources.
	 */
	@Test
	public void testRoundRobinPolling() throws UnknownHostException {
		final SlotSharingGroupAssignment slotSharingGroupAssignment = new SlotSharingGroupAssignment();
		final int numberTaskManagers = 2;
		final int numberSlots = 2;
		final JobVertexID sourceId = new JobVertexID();
		final JobVertexID sinkId = new JobVertexID();

		for (int i = 0; i < numberTaskManagers; i++) {
			final TaskManagerLocation taskManagerLocation = new TaskManagerLocation(ResourceID.generate(), InetAddress.getLocalHost(), i + 1000);

			for (int j = 0; j < numberSlots; j++) {
				final SharedSlot slot = new SharedSlot(
					mock(SlotOwner.class),
					taskManagerLocation,
					j,
					mock(TaskManagerGateway.class),
					slotSharingGroupAssignment);

				slotSharingGroupAssignment.addSharedSlotAndAllocateSubSlot(slot, Locality.UNKNOWN, sourceId);
			}
		}

		SimpleSlot allocatedSlot1 = slotSharingGroupAssignment.getSlotForTask(sinkId, Collections.emptyList());
		SimpleSlot allocatedSlot2 = slotSharingGroupAssignment.getSlotForTask(sinkId, Collections.emptyList());

		assertNotEquals(allocatedSlot1.getTaskManagerLocation(), allocatedSlot2.getTaskManagerLocation());

		// let's check that we can still allocate all 4 slots
		SimpleSlot allocatedSlot3 = slotSharingGroupAssignment.getSlotForTask(sinkId, Collections.emptyList());
		assertNotNull(allocatedSlot3);

		SimpleSlot allocatedSlot4 = slotSharingGroupAssignment.getSlotForTask(sinkId, Collections.emptyList());
		assertNotNull(allocatedSlot4);
	}
}

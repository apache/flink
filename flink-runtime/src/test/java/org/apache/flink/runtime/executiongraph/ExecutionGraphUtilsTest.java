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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Tests for the utility methods in the class {@link ExecutionGraphUtils}.
 */
public class ExecutionGraphUtilsTest {

	@Test
	public void testReleaseSlots() {
		final JobID jid = new JobID();
		final SlotOwner owner = mock(SlotOwner.class);

		final SimpleSlot slot1 = new SimpleSlot(createAllocatedSlot(jid, 0), owner, 0);
		final SimpleSlot slot2 = new SimpleSlot(createAllocatedSlot(jid, 1), owner, 1);
		final SimpleSlot slot3 = new SimpleSlot(createAllocatedSlot(jid, 2), owner, 2);

		final FlinkCompletableFuture<SimpleSlot> incompleteFuture = new FlinkCompletableFuture<>();

		final FlinkCompletableFuture<SimpleSlot> completeFuture = new FlinkCompletableFuture<>();
		completeFuture.complete(slot2);

		final FlinkCompletableFuture<SimpleSlot> disposedSlotFuture = new FlinkCompletableFuture<>();
		slot3.releaseSlot();
		disposedSlotFuture.complete(slot3);

		// release all futures
		ExecutionGraphUtils.releaseSlotFuture(incompleteFuture);
		ExecutionGraphUtils.releaseSlotFuture(completeFuture);
		ExecutionGraphUtils.releaseSlotFuture(disposedSlotFuture);

		// only now complete the incomplete future
		incompleteFuture.complete(slot1);

		// verify that each slot was returned once to the owner
		verify(owner, times(1)).returnAllocatedSlot(eq(slot1));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot2));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot3));
	}

	@Test
	public void testReleaseSlotsWithNulls() {
		final JobID jid = new JobID();
		final SlotOwner owner = mock(SlotOwner.class);

		final Execution mockExecution = mock(Execution.class);

		final SimpleSlot slot1 = new SimpleSlot(createAllocatedSlot(jid, 0), owner, 0);
		final SimpleSlot slot2 = new SimpleSlot(createAllocatedSlot(jid, 1), owner, 1);
		final SimpleSlot slot3 = new SimpleSlot(createAllocatedSlot(jid, 2), owner, 2);
		final SimpleSlot slot4 = new SimpleSlot(createAllocatedSlot(jid, 3), owner, 3);
		final SimpleSlot slot5 = new SimpleSlot(createAllocatedSlot(jid, 4), owner, 4);

		ExecutionAndSlot[] slots1 = new ExecutionAndSlot[] {
				null,
				new ExecutionAndSlot(mockExecution, FlinkCompletableFuture.completed(slot1)),
				null,
				new ExecutionAndSlot(mockExecution, FlinkCompletableFuture.completed(slot2)),
				null
		};

		ExecutionAndSlot[] slots2 = new ExecutionAndSlot[] {
				new ExecutionAndSlot(mockExecution, FlinkCompletableFuture.completed(slot3)),
				new ExecutionAndSlot(mockExecution, FlinkCompletableFuture.completed(slot4)),
				new ExecutionAndSlot(mockExecution, FlinkCompletableFuture.completed(slot5))
		};

		List<ExecutionAndSlot[]> resources = Arrays.asList(null, slots1, new ExecutionAndSlot[0], null, slots2);

		ExecutionGraphUtils.releaseAllSlotsSilently(resources);

		verify(owner, times(1)).returnAllocatedSlot(eq(slot1));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot2));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot3));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot4));
		verify(owner, times(1)).returnAllocatedSlot(eq(slot5));
	}

	// ------------------------------------------------------------------------

	private static AllocatedSlot createAllocatedSlot(JobID jid, int num) {
		TaskManagerLocation loc = new TaskManagerLocation(
				ResourceID.generate(), InetAddress.getLoopbackAddress(), 10000 + num);
	
		return new AllocatedSlot(new AllocationID(), jid, loc, num,
				ResourceProfile.UNKNOWN, mock(TaskManagerGateway.class));
	}
}

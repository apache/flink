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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link CoLocationConstraint} with the {@link SlotPool}.
 */
public class SlotPoolCoLocationTest extends SlotPoolSchedulingTestBase {

	/**
	 * Tests the scheduling of two tasks with a parallelism of 2 and a co-location constraint.
	 */
	@Test
	public void testSimpleCoLocatedSlotScheduling() throws ExecutionException, InterruptedException {
		final BlockingQueue<AllocationID> allocationIds = new ArrayBlockingQueue<>(2);

		testingResourceManagerGateway.setRequestSlotConsumer(
			(SlotRequest slotRequest) -> allocationIds.offer(slotRequest.getAllocationId()));

		final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();

		slotPoolGateway.registerTaskManager(taskManagerLocation.getResourceID()).get();

		CoLocationGroup group = new CoLocationGroup();
		CoLocationConstraint coLocationConstraint1 = group.getLocationConstraint(0);
		CoLocationConstraint coLocationConstraint2 = group.getLocationConstraint(1);

		final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

		JobVertexID jobVertexId1 = new JobVertexID();
		JobVertexID jobVertexId2 = new JobVertexID();

		CompletableFuture<LogicalSlot> logicalSlotFuture11 = slotProvider.allocateSlot(
			new ScheduledUnit(
				jobVertexId1,
				slotSharingGroupId,
				coLocationConstraint1),
			true,
			SlotProfile.noRequirements(),
			TestingUtils.infiniteTime());

		CompletableFuture<LogicalSlot> logicalSlotFuture22 = slotProvider.allocateSlot(
			new ScheduledUnit(
				jobVertexId2,
				slotSharingGroupId,
				coLocationConstraint2),
			true,
			SlotProfile.noRequirements(),
			TestingUtils.infiniteTime());

		CompletableFuture<LogicalSlot> logicalSlotFuture12 = slotProvider.allocateSlot(
			new ScheduledUnit(
				jobVertexId2,
				slotSharingGroupId,
				coLocationConstraint1),
			true,
			SlotProfile.noRequirements(),
			TestingUtils.infiniteTime());

		CompletableFuture<LogicalSlot> logicalSlotFuture21 = slotProvider.allocateSlot(
			new ScheduledUnit(
				jobVertexId1,
				slotSharingGroupId,
				coLocationConstraint2),
			true,
			SlotProfile.noRequirements(),
			TestingUtils.infiniteTime());

		final AllocationID allocationId1 = allocationIds.take();
		final AllocationID allocationId2 = allocationIds.take();

		CompletableFuture<Boolean> slotOfferFuture1 = slotPoolGateway.offerSlot(
			taskManagerLocation,
			new SimpleAckingTaskManagerGateway(),
			new SlotOffer(
				allocationId1,
				0,
				ResourceProfile.UNKNOWN));

		CompletableFuture<Boolean> slotOfferFuture2 = slotPoolGateway.offerSlot(
			taskManagerLocation,
			new SimpleAckingTaskManagerGateway(),
			new SlotOffer(
				allocationId2,
				0,
				ResourceProfile.UNKNOWN));

		assertTrue(slotOfferFuture1.get());
		assertTrue(slotOfferFuture2.get());

		LogicalSlot logicalSlot11 = logicalSlotFuture11.get();
		LogicalSlot logicalSlot12 = logicalSlotFuture12.get();
		LogicalSlot logicalSlot21 = logicalSlotFuture21.get();
		LogicalSlot logicalSlot22 = logicalSlotFuture22.get();

		assertEquals(logicalSlot11.getAllocationId(), logicalSlot12.getAllocationId());
		assertEquals(logicalSlot21.getAllocationId(), logicalSlot22.getAllocationId());
		assertNotEquals(logicalSlot11.getAllocationId(), logicalSlot21.getAllocationId());
	}
}

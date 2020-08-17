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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.DummyPayload;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.SharedSlotTestingUtils.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createRandomExecutionVertexId;
import static org.apache.flink.runtime.scheduler.SharedSlotTestingUtils.createExecutionSlotSharingGroup;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Test suite for {@link SharedSlot}.
 */
public class SharedSlotTest extends TestLogger {
	private static final ExecutionVertexID EV1 = createRandomExecutionVertexId();
	private static final ExecutionSlotSharingGroup SG = createExecutionSlotSharingGroup(EV1);
	private static final SlotRequestId PHYSICAL_SLOT_REQUEST_ID = new SlotRequestId();
	private static final ResourceProfile RP = ResourceProfile.newBuilder().setCpuCores(2.0).build();

	@Test
	public void testCreation() {
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.slotWillBeOccupiedIndefinitely()
			.build();
		assertThat(sharedSlot.getPhysicalSlotRequestId(), is(PHYSICAL_SLOT_REQUEST_ID));
		assertThat(sharedSlot.getPhysicalSlotResourceProfile(), is(RP));
		assertThat(sharedSlot.willOccupySlotIndefinitely(), is(true));
		assertThat(sharedSlot.isEmpty(), is(true));
	}

	@Test
	public void testAssignAsPayloadToPhysicalSlot() {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.build();
		TestingPhysicalSlot physicalSlot = new TestingPhysicalSlot(RP, new AllocationID());
		slotContextFuture.complete(physicalSlot);
		assertThat(physicalSlot.getPayload(), is(sharedSlot));
	}

	@Test
	public void testLogicalSlotAllocation() {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.slotWillBeOccupiedIndefinitely()
			.withExternalReleaseCallback(released::complete)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);

		assertThat(logicalSlotFuture.isDone(), is(false));

		AllocationID allocationId = new AllocationID();
		LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
		SimpleAckingTaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();
		int physicalSlotNumber = 3;
		slotContextFuture.complete(new TestingPhysicalSlot(
			allocationId,
			taskManagerLocation,
			physicalSlotNumber,
			taskManagerGateway,
			RP));

		assertThat(sharedSlot.isEmpty(), is(false));
		assertThat(released.isDone(), is(false));
		assertThat(logicalSlotFuture.isDone(), is(true));
		LogicalSlot logicalSlot = logicalSlotFuture.join();
		assertThat(logicalSlot.getAllocationId(), is(allocationId));
		assertThat(logicalSlot.getTaskManagerLocation(), is(taskManagerLocation));
		assertThat(logicalSlot.getTaskManagerGateway(), is(taskManagerGateway));
		assertThat(logicalSlot.getPhysicalSlotNumber(), is(physicalSlotNumber));
		assertThat(logicalSlot.getLocality(), is(Locality.UNKNOWN));
		assertThat(logicalSlot.getSlotSharingGroupId(), nullValue());
	}

	@Test
	public void testLogicalSlotFailureDueToPhysicalSlotFailure() throws InterruptedException {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(released::complete)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
		Throwable cause = new Throwable();
		slotContextFuture.completeExceptionally(cause);

		assertThat(logicalSlotFuture.isCompletedExceptionally(), is(true));
		try {
			logicalSlotFuture.get();
		} catch (ExecutionException e) {
			assertThat(e.getCause(), is(cause));
		}
		assertThat(sharedSlot.isEmpty(), is(true));
		assertThat(released.isDone(), is(true));
	}

	@Test
	public void testCancelCompletedLogicalSlotRequest() {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(released::complete)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
		slotContextFuture.complete(new TestingPhysicalSlot(RP, new AllocationID()));
		sharedSlot.cancelLogicalSlotRequest(EV1, new Throwable());

		assertThat(logicalSlotFuture.isCompletedExceptionally(), is(false));
		assertThat(sharedSlot.isEmpty(), is(false));
		assertThat(released.isDone(), is(false));
	}

	@Test
	public void testCancelPendingLogicalSlotRequest() throws InterruptedException {
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withExternalReleaseCallback(released::complete)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
		Throwable cause = new Throwable();
		sharedSlot.cancelLogicalSlotRequest(EV1, cause);

		assertThat(logicalSlotFuture.isCompletedExceptionally(), is(true));
		try {
			logicalSlotFuture.get();
		} catch (ExecutionException e) {
			assertThat(e.getCause(), is(cause));
		}
		assertThat(sharedSlot.isEmpty(), is(true));
		assertThat(released.isDone(), is(true));
	}

	@Test
	public void testReturnAllocatedLogicalSlot() {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(released::complete)
			.build();
		CompletableFuture<LogicalSlot> logicalSlotFuture = sharedSlot.allocateLogicalSlot(EV1);
		slotContextFuture.complete(new TestingPhysicalSlot(RP, new AllocationID()));
		sharedSlot.returnLogicalSlot(logicalSlotFuture.join());

		assertThat(sharedSlot.isEmpty(), is(true));
		assertThat(released.isDone(), is(true));
	}

	@Test
	public void testReleaseIfPhysicalSlotRequestIsIncomplete() {
		CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(released::complete)
			.build();
		sharedSlot.allocateLogicalSlot(EV1);

		try {
			sharedSlot.release(new Throwable());
			fail("IllegalStateException is expected trying to release a shared slot with incomplete physical slot request");
		} catch (IllegalStateException e) {
			// expected
		}
		assertThat(sharedSlot.isEmpty(), is(false));
		assertThat(released.isDone(), is(false));
	}

	@Test
	public void testReleaseIfPhysicalSlotIsAllocated() {
		CompletableFuture<PhysicalSlot> slotContextFuture = CompletableFuture
			.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
		CompletableFuture<ExecutionSlotSharingGroup> released = new CompletableFuture<>();
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(released::complete)
			.build();
		LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();
		CompletableFuture<Object> terminalFuture = new CompletableFuture<>();
		logicalSlot.tryAssignPayload(new DummyPayload(terminalFuture));
		assertThat(terminalFuture.isDone(), is(false));
		sharedSlot.release(new Throwable());

		assertThat(terminalFuture.isDone(), is(true));
		assertThat(sharedSlot.isEmpty(), is(true));
		assertThat(released.isDone(), is(true));
	}

	@Test
	public void tesDuplicatedReturnLogicalSlotFails() {
		CompletableFuture<PhysicalSlot> slotContextFuture = CompletableFuture
			.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
		AtomicInteger released = new AtomicInteger(0);
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(g -> released.incrementAndGet())
			.build();
		LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();
		sharedSlot.returnLogicalSlot(logicalSlot);
		try {
			sharedSlot.returnLogicalSlot(logicalSlot);
			fail("Duplicated 'returnLogicalSlot' call should fail with IllegalStateException");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testReleaseEmptyDoesNotCallAllocatorReleaseBack() {
		CompletableFuture<PhysicalSlot> slotContextFuture = CompletableFuture
			.completedFuture(new TestingPhysicalSlot(RP, new AllocationID()));
		CompletableFuture<SharedSlot> sharedSlotReleaseFuture = new CompletableFuture<>();
		AtomicInteger released = new AtomicInteger(0);
		SharedSlot sharedSlot = SharedSlotBuilder
			.newBuilder()
			.withSlotContextFuture(slotContextFuture)
			.withExternalReleaseCallback(g -> {
				// checks that release -> externalReleaseCallback -> release does not lead to infinite recursion
				// due to SharedSlot.state.RELEASED check
				sharedSlotReleaseFuture.join().release(new Throwable());
				released.incrementAndGet();
			})
			.build();
		sharedSlotReleaseFuture.complete(sharedSlot);
		LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot(EV1).join();

		assertThat(released.get(), is(0));
		// returns the only and last slot, calling the external release callback
		sharedSlot.returnLogicalSlot(logicalSlot);
		assertThat(released.get(), is(1));

		// slot is already released, it should not get released again
		sharedSlot.release(new Throwable());
		assertThat(released.get(), is(1));
	}

	private static class SharedSlotBuilder {
		private CompletableFuture<PhysicalSlot> slotContextFuture = new CompletableFuture<>();
		private boolean slotWillBeOccupiedIndefinitely = false;
		private Consumer<ExecutionSlotSharingGroup> externalReleaseCallback = group -> {};

		private SharedSlotBuilder withSlotContextFuture(CompletableFuture<PhysicalSlot> slotContextFuture) {
			this.slotContextFuture = slotContextFuture;
			return this;
		}

		private SharedSlotBuilder slotWillBeOccupiedIndefinitely() {
			this.slotWillBeOccupiedIndefinitely = true;
			return this;
		}

		private SharedSlotBuilder withExternalReleaseCallback(Consumer<ExecutionSlotSharingGroup> releaseCallback) {
			this.externalReleaseCallback = releaseCallback;
			return this;
		}

		private SharedSlot build() {
			return new SharedSlot(
				PHYSICAL_SLOT_REQUEST_ID,
				RP,
				SG,
				slotContextFuture,
				slotWillBeOccupiedIndefinitely,
				externalReleaseCallback);
		}

		private static SharedSlotBuilder newBuilder() {
			return new SharedSlotBuilder();
		}
	}
}

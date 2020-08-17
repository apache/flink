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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test suite for idle task managers release in slot manager.
 */
public class TaskManagerCheckInSlotManagerTest extends TestLogger {
	private static final ResourceID resourceID = ResourceID.generate();
	private static final ResourceManagerId resourceManagerId = ResourceManagerId.generate();
	private static final SlotID slotId = new SlotID(resourceID, 0);
	private static final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
	private static final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
	private static final SlotReport slotReport = new SlotReport(slotStatus);

	private final AtomicReference<CompletableFuture<Boolean>> canBeReleasedFuture = new AtomicReference<>();
	private final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
		.setCanBeReleasedSupplier(canBeReleasedFuture::get)
		.createTestingTaskExecutorGateway();
	private final TaskExecutorConnection taskManagerConnection =
		new TaskExecutorConnection(resourceID, taskExecutorGateway);

	private CompletableFuture<InstanceID> releaseFuture;
	private ResourceActions resourceManagerActions;
	private ManuallyTriggeredScheduledExecutor mainThreadExecutor;
	private final AtomicInteger allocateResourceCalls = new AtomicInteger(0);
	private final AtomicInteger releaseResourceCalls = new AtomicInteger(0);

	@Before
	public void setup() {
		canBeReleasedFuture.set(new CompletableFuture<>());
		releaseFuture = new CompletableFuture<>();
		allocateResourceCalls.getAndSet(0);
		releaseResourceCalls.getAndSet(0);
		resourceManagerActions = new TestingResourceActionsBuilder()
			.setReleaseResourceConsumer((instanceID, e) -> {
				releaseFuture.complete(instanceID);
				releaseResourceCalls.incrementAndGet();
			})
			.setAllocateResourceConsumer(ignored -> allocateResourceCalls.incrementAndGet())
			.build();
		mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
	}

	/**
	 * Tests that idle task managers time out after the configured timeout. A timed out task manager
	 * will be removed from the slot manager and the resource manager will be notified about the
	 * timeout, if it can be released.
	 */
	@Test
	public void testTaskManagerTimeout() throws Exception {
		checkTaskManagerTimeout(0);
	}

	/**
	 * If there is no job running, no need to keep redundant taskManagers and release the timeout taskManagers.
	 * This may happen in session mode without jobs running.
	 */
	@Test
	public void testTaskManagerTimeoutWithRedundantTaskManager() throws Exception {
		checkTaskManagerTimeout(1);
	}

	/**
	 * Register four taskManagers that all have two slots.
	 * For taskManager0, both slots are free.
	 * For taskManager1, both slots are allocated.
	 * For taskManager2, One slot is allocated, the other is free.
	 * For taskManager3, one slot is free, the other is allocated.
	 * If redundantTaskManagerNum is 0, the idle taskManager should be released.
	 * @throws Exception
	 */
	@Test
	public void testTaskManagerTimeoutWithZeroRedundantTaskManager() throws Exception {
		registerAndCheckMultiTaskManagers(0);

		assertThat(allocateResourceCalls.get(), is(0));
		assertThat(releaseResourceCalls.get(), is(1));
	}

	/**
	 * Register four taskManagers that all have two slots.
	 * For taskManager0, both slots are free.
	 * For taskManager1, both slots are allocated.
	 * For taskManager2, One slot is allocated, the other is free.
	 * For taskManager3, one slot is free, the other is allocated.
	 * If redundantTaskManagerNum is 1, two free slots are needed and the idle taskManager should be released.
	 * @throws Exception
	 */
	@Test
	public void testTaskManagerTimeoutWithOneRedundantTaskManager() throws Exception {
		registerAndCheckMultiTaskManagers(1);

		assertThat(allocateResourceCalls.get(), is(0));
		assertThat(releaseResourceCalls.get(), is(1));
	}

	/**
	 * Register four taskManagers that all have two slots.
	 * For taskManager0, both slots are free.
	 * For taskManager1, both slots are allocated.
	 * For taskManager2, One slot is allocated, the other is free.
	 * For taskManager3, one slot is free, the other is allocated.
	 * If redundantTaskManagerNum is 2, four free slots can satisfy the requirement.
	 * @throws Exception
	 */
	@Test
	public void testTaskManagerTimeoutWithTwoRedundantTaskManager() throws Exception {
		registerAndCheckMultiTaskManagers(2);

		assertThat(allocateResourceCalls.get(), is(0));
		assertThat(releaseResourceCalls.get(), is(0));
	}

	/**
	 * Register four taskManagers that all have two slots.
	 * For taskManager0, both slots are free.
	 * For taskManager1, both slots are allocated.
	 * For taskManager2, One slot is allocated, the other is free.
	 * For taskManager3, one slot is free, the other is allocated.
	 * If redundantTaskManagerNum is 3, two more free slots are needed and another taskManager should be allocated.
	 * @throws Exception
	 */
	@Test
	public void testTaskManagerTimeoutWithThreeRedundantTaskManager() throws Exception {
		registerAndCheckMultiTaskManagers(3);

		assertThat(allocateResourceCalls.get(), is(1));
		assertThat(releaseResourceCalls.get(), is(0));
	}

	/**
	 * Tests that idle but not releasable task managers will not be released even if timed out before it can be.
	 */
	@Test
	public void testTaskManagerIsNotReleasedBeforeItCanBe() throws Exception {
		try (SlotManagerImpl slotManager = createAndStartSlotManagerWithTM()) {
			checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, false);
			verifyTmReleased(false);

			checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, true);
			verifyTmReleased(true);
		}
	}

	/**
	 * Tests that idle task managers will not be released after "can be" check in case of concurrent resource allocations.
	 */
	@Test
	public void testTaskManagerIsNotReleasedInCaseOfConcurrentAllocation() throws Exception {
		try (SlotManagerImpl slotManager = createAndStartSlotManagerWithTM()) {
			checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, true, () -> {
				// Allocate and free slot between triggering TM.canBeReleased request and receiving response.
				// There can be potentially newly unreleased partitions, therefore TM can not be released yet.
				AllocationID allocationID = new AllocationID();
				slotManager.registerSlotRequest(new SlotRequest(new JobID(), allocationID, resourceProfile, "foobar"));
				mainThreadExecutor.triggerAll();
				slotManager.freeSlot(slotId, allocationID);
			});
			verifyTmReleased(false);

			checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, true);
			verifyTmReleased(true);
		}
	}

	private void checkTaskManagerTimeout(int redundantTaskManagerNum) throws Exception {
		canBeReleasedFuture.set(CompletableFuture.completedFuture(true));
		try (SlotManager slotManager = SlotManagerBuilder
			.newBuilder()
			.setTaskManagerTimeout(Time.milliseconds(10L))
			.setRedundantTaskManagerNum(redundantTaskManagerNum)
			.buildAndStartWithDirectExec(resourceManagerId, resourceManagerActions)) {

			slotManager.registerTaskManager(taskManagerConnection, slotReport);
			assertThat(releaseFuture.get(), is(equalTo(taskManagerConnection.getInstanceID())));
		}
	}

	/**
	 * Register four taskManagers that all have two slots.
	 * The difference between the taskManagers is whether the slot is allocated.
	 * To maintain redundantTaskManagerNum, SlotManagerImpl may release or allocate taskManagers.
	 * @param redundantTaskManagerNum
	 * @throws Exception
	 */
	private void registerAndCheckMultiTaskManagers(int redundantTaskManagerNum) throws Exception {
		SlotManagerImpl slotManager = createAndStartSlotManager(redundantTaskManagerNum, 2);

		// Both slots are free.
		registerTaskManagerWithTwoSlots(slotManager, true, true);

		// Both slots are allocated.
		registerTaskManagerWithTwoSlots(slotManager, false, false);

		// One slot is allocated, the other is free.
		registerTaskManagerWithTwoSlots(slotManager, false, true);

		// One slot is free, the other is allocated.
		registerTaskManagerWithTwoSlots(slotManager, true, false);

		checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, true);
	}

	private void registerTaskManagerWithTwoSlots(SlotManagerImpl slotManager,
												 boolean slot0Free,
												 boolean slot1Free) {
		canBeReleasedFuture.set(new CompletableFuture<>());

		ResourceID resourceID = ResourceID.generate();
		ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 1);
		JobID jobID = new JobID();

		SlotID slotId0 = new SlotID(resourceID, 0);
		SlotStatus slotStatus0 = slot0Free ? new SlotStatus(slotId0, resourceProfile)
			: new SlotStatus(slotId0, resourceProfile, jobID, new AllocationID());

		SlotID slotId1 = new SlotID(resourceID, 1);
		SlotStatus slotStatus1 = slot1Free ? new SlotStatus(slotId1, resourceProfile) :
			new SlotStatus(slotId1, resourceProfile, jobID, new AllocationID());

		SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus0, slotStatus1));

		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setCanBeReleasedSupplier(canBeReleasedFuture::get)
			.createTestingTaskExecutorGateway();
		TaskExecutorConnection taskManagerConnection =
			new TaskExecutorConnection(resourceID, taskExecutorGateway);

		mainThreadExecutor.execute(() -> slotManager.registerTaskManager(taskManagerConnection, slotReport));
	}

	private SlotManagerImpl createAndStartSlotManagerWithTM() {
		SlotManagerImpl slotManager = createAndStartSlotManager(0, 1);
		mainThreadExecutor.execute(() -> slotManager.registerTaskManager(taskManagerConnection, slotReport));
		return slotManager;
	}

	private SlotManagerImpl createAndStartSlotManager(int redundantTaskManagerNum, int numSlotsPerWorker) {
		SlotManagerImpl slotManager = SlotManagerBuilder
			.newBuilder()
			.setScheduledExecutor(mainThreadExecutor)
			.setTaskManagerTimeout(Time.milliseconds(0L))
			.setRedundantTaskManagerNum(redundantTaskManagerNum)
			.setNumSlotsPerWorker(numSlotsPerWorker)
			.build();
		slotManager.start(resourceManagerId, mainThreadExecutor, resourceManagerActions);
		return slotManager;
	}

	private void checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(
			SlotManagerImpl slotManager,
			boolean canBeReleased) throws Exception {
		checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(slotManager, canBeReleased, () -> {});
	}

	private void checkTaskManagerTimeoutWithCustomCanBeReleasedResponse(
			SlotManagerImpl slotManager,
			boolean canBeReleased,
			RunnableWithException doAfterCheckTriggerBeforeCanBeReleasedResponse) throws Exception {
		canBeReleasedFuture.set(new CompletableFuture<>());
		mainThreadExecutor.execute(slotManager::checkTaskManagerTimeoutsAndRedundancy); // trigger TM.canBeReleased request
		mainThreadExecutor.triggerAll();
		doAfterCheckTriggerBeforeCanBeReleasedResponse.run();
		canBeReleasedFuture.get().complete(canBeReleased); // finish TM.canBeReleased request
		mainThreadExecutor.triggerAll();
	}

	private void verifyTmReleased(boolean isTmReleased) {
		assertThat(releaseFuture.isDone(), is(isTmReleased));
		if (isTmReleased) {
			assertThat(releaseFuture.join(), is(equalTo(taskManagerConnection.getInstanceID())));
		}
	}
}

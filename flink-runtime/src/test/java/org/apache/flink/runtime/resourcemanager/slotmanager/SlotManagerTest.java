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
import org.apache.flink.runtime.clusterframework.types.TaskManagerSlot;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlotManagerTest extends TestLogger {

	/**
	 * Tests that we can register task manager and their slots at the slot manager.
	 */
	@Test
	public void testTaskManagerRegistration() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		ResourceID resourceId = ResourceID.generate();
		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertTrue("The number registered slots does not equal the expected number.",2 == slotManager.getNumberRegisteredSlots());

			assertNotNull(slotManager.getSlot(slotId1));
			assertNotNull(slotManager.getSlot(slotId2));
		}
	}

	/**
	 * Tests that un-registration of task managers will free and remove all registered slots.
	 */
	@Test
	public void testTaskManagerUnregistration() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final JobID jobId = new JobID();

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			any(AllocationID.class),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(new CompletableFuture<>());

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		ResourceID resourceId = ResourceID.generate();
		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final AllocationID allocationId1 = new AllocationID();
		final AllocationID allocationId2 = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile, jobId, allocationId1);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final SlotRequest slotRequest = new SlotRequest(
			new JobID(),
			allocationId2,
			resourceProfile,
			"foobar");

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertTrue("The number registered slots does not equal the expected number.",2 == slotManager.getNumberRegisteredSlots());

			TaskManagerSlot slot1 = slotManager.getSlot(slotId1);
			TaskManagerSlot slot2 = slotManager.getSlot(slotId2);

			assertTrue(slot1.isAllocated());
			assertTrue(slot2.isFree());

			assertTrue(slotManager.registerSlotRequest(slotRequest));

			assertFalse(slot2.isFree());
			assertTrue(slot2.hasPendingSlotRequest());

			PendingSlotRequest pendingSlotRequest = slotManager.getSlotRequest(allocationId2);

			assertTrue("The pending slot request should have been assigned to slot 2", pendingSlotRequest.isAssigned());

			slotManager.unregisterTaskManager(taskManagerConnection.getInstanceID());

			assertTrue(0 == slotManager.getNumberRegisteredSlots());
			assertFalse(pendingSlotRequest.isAssigned());
		}
	}

	/**
	 * Tests that a slot request with no free slots will trigger the resource allocation
	 */
	@Test
	public void testSlotRequestWithoutFreeSlots() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(
			new JobID(),
			new AllocationID(),
			resourceProfile,
			"localhost");

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			slotManager.registerSlotRequest(slotRequest);

			verify(resourceManagerActions).allocateResource(eq(resourceProfile));
		}
	}

	/**
	 * Tests that the slot request fails if we cannot allocate more resources.
	 */
	@Test
	public void testSlotRequestWithResourceAllocationFailure() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(
			new JobID(),
			new AllocationID(),
			resourceProfile,
			"localhost");

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		doThrow(new ResourceManagerException("Test exception")).when(resourceManagerActions).allocateResource(any(ResourceProfile.class));

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			slotManager.registerSlotRequest(slotRequest);

			fail("The slot request should have failed with a ResourceManagerException.");

		} catch (ResourceManagerException e) {
			// expected exception
		}
	}

	/**
	 * Tests that a slot request which can be fulfilled will trigger a slot allocation.
	 */
	@Test
	public void testSlotRequestWithFreeSlot() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(
			jobId,
			allocationId,
			resourceProfile,
			targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			// accept an incoming slot request
			final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
			when(taskExecutorGateway.requestSlot(
				eq(slotId),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

			final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

			final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
			final SlotReport slotReport = new SlotReport(slotStatus);

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			assertTrue("The slot request should be accepted", slotManager.registerSlotRequest(slotRequest));

			verify(taskExecutorGateway).requestSlot(eq(slotId), eq(jobId), eq(allocationId), eq(targetAddress), eq(leaderId), any(Time.class));

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());
		}
	}

	/**
	 * Checks that un-registering a pending slot request will cancel it, removing it from all
	 * assigned task manager slots and then remove it from the slot manager.
	 */
	@Test
	public void testUnregisterPendingSlotRequest() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final SlotID slotId = new SlotID(ResourceID.generate(), 0);
		final AllocationID allocationId = new AllocationID();

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			any(AllocationID.class),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(new CompletableFuture<>());

		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
		final SlotReport slotReport = new SlotReport(slotStatus);

		final SlotRequest slotRequest = new SlotRequest(new JobID(), allocationId, resourceProfile, "foobar");

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			slotManager.registerSlotRequest(slotRequest);

			assertNotNull(slotManager.getSlotRequest(allocationId));

			assertTrue(slot.hasPendingSlotRequest());

			slotManager.unregisterSlotRequest(allocationId);

			assertNull(slotManager.getSlotRequest(allocationId));

			slot = slotManager.getSlot(slotId);
			assertTrue(slot.isFree());
		}
	}

	/**
	 * Tests that pending slot requests are tried to be fulfilled upon new slot registrations.
	 */
	@Test
	public void testFulfillingPendingSlotRequest() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final String targetAddress = "localhost";
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(
			jobId,
			allocationId,
			resourceProfile,
			targetAddress);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		// accept an incoming slot request
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			eq(slotId),
			eq(jobId),
			eq(allocationId),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			assertTrue("The slot request should be accepted", slotManager.registerSlotRequest(slotRequest));

			verify(resourceManagerActions, times(1)).allocateResource(eq(resourceProfile));

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			verify(taskExecutorGateway).requestSlot(eq(slotId), eq(jobId), eq(allocationId), eq(targetAddress), eq(leaderId), any(Time.class));

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());
		}
	}

	/**
	 * Tests that freeing a slot will correctly reset the slot and mark it as a free slot
	 */
	@Test
	public void testFreeSlot() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceID resourceID = ResourceID.generate();
		final JobID jobId = new JobID();
		final SlotID slotId = new SlotID(resourceID, 0);
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);

		ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		// accept an incoming slot request
		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);

		final TaskExecutorConnection taskExecutorConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile, jobId, allocationId);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			slotManager.registerTaskManager(
				taskExecutorConnection,
				slotReport);

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());

			// this should be ignored since the allocation id does not match
			slotManager.freeSlot(slotId, new AllocationID());

			assertTrue(slot.isAllocated());
			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());

			slotManager.freeSlot(slotId, allocationId);

			assertTrue(slot.isFree());
			assertNull(slot.getAllocationId());
		}
	}

	/**
	 * Tests that a second pending slot request is detected as a duplicate if the allocation ids are
	 * the same.
	 */
	@Test
	public void testDuplicatePendingSlotRequest() throws Exception {

		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 2);
		final ResourceProfile resourceProfile2 = new ResourceProfile(2.0, 1);
		final SlotRequest slotRequest1 = new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
		final SlotRequest slotRequest2 = new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			assertTrue(slotManager.registerSlotRequest(slotRequest1));
			assertFalse(slotManager.registerSlotRequest(slotRequest2));
		}

		// check that we have only called the resource allocation only for the first slot request,
		// since the second request is a duplicate
		verify(resourceManagerActions, times(1)).allocateResource(any(ResourceProfile.class));
	}

	/**
	 * Tests that if we have received a slot report with some allocated slots, then we don't accept
	 * slot requests with allocated allocation ids.
	 */
	@Test
	public void testDuplicatePendingSlotRequestAfterSlotReport() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotID slotId = new SlotID(ResourceID.generate(), 0);

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile, jobId, allocationId);
		final SlotReport slotReport = new SlotReport(slotStatus);

		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			assertFalse(slotManager.registerSlotRequest(slotRequest));
		}
	}

	/**
	 * Tests that duplicate slot requests (requests with an already registered allocation id) are
	 * also detected after a pending slot request has been fulfilled but not yet freed.
	 */
	@Test
	public void testDuplicatePendingSlotRequestAfterSuccessfulAllocation() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 2);
		final ResourceProfile resourceProfile2 = new ResourceProfile(2.0, 1);
		final SlotRequest slotRequest1 = new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
		final SlotRequest slotRequest2 = new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			any(AllocationID.class),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotID slotId = new SlotID(ResourceID.generate(), 0);
		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile1);
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);
			assertTrue(slotManager.registerSlotRequest(slotRequest1));

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());

			assertFalse(slotManager.registerSlotRequest(slotRequest2));
		}

		// check that we have only called the resource allocation only for the first slot request,
		// since the second request is a duplicate
		verify(resourceManagerActions, never()).allocateResource(any(ResourceProfile.class));
	}

	/**
	 * Tests that an already registered allocation id can be reused after the initial slot request
	 * has been freed.
	 */
	@Test
	public void testAcceptingDuplicateSlotRequestAfterAllocationRelease() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 2);
		final ResourceProfile resourceProfile2 = new ResourceProfile(2.0, 1);
		final SlotRequest slotRequest1 = new SlotRequest(new JobID(), allocationId, resourceProfile1, "foobar");
		final SlotRequest slotRequest2 = new SlotRequest(new JobID(), allocationId, resourceProfile2, "barfoo");

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			any(AllocationID.class),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotID slotId = new SlotID(ResourceID.generate(), 0);
		final SlotStatus slotStatus = new SlotStatus(slotId, new ResourceProfile(2.0, 2));
		final SlotReport slotReport = new SlotReport(slotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			slotManager.registerTaskManager(taskManagerConnection, slotReport);
			assertTrue(slotManager.registerSlotRequest(slotRequest1));

			TaskManagerSlot slot = slotManager.getSlot(slotId);

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());

			slotManager.freeSlot(slotId, allocationId);

			// check that the slot has been freed
			assertTrue(slot.isFree());
			assertNull(slot.getAllocationId());

			assertTrue(slotManager.registerSlotRequest(slotRequest2));

			assertEquals("The slot has not been allocated to the expected allocation id.", allocationId, slot.getAllocationId());
		}

		// check that we have only called the resource allocation only for the first slot request,
		// since the second request is a duplicate
		verify(resourceManagerActions, never()).allocateResource(any(ResourceProfile.class));
	}

	/**
	 * Tests that the slot manager ignores slot reports of unknown origin (not registered
	 * task managers).
	 */
	@Test
	public void testReceivingUnknownSlotReport() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		final InstanceID unknownInstanceID = new InstanceID();
		final SlotID unknownSlotId = new SlotID(ResourceID.generate(), 0);
		final ResourceProfile unknownResourceProfile = new ResourceProfile(1.0, 1);
		final SlotStatus unknownSlotStatus = new SlotStatus(unknownSlotId, unknownResourceProfile);
		final SlotReport unknownSlotReport = new SlotReport(unknownSlotStatus);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			// check that we don't have any slots registered
			assertTrue(0 == slotManager.getNumberRegisteredSlots());

			// this should not update anything since the instance id is not known to the slot manager
			assertFalse(slotManager.reportSlotStatus(unknownInstanceID, unknownSlotReport));

			assertTrue(0 == slotManager.getNumberRegisteredSlots());
		}
	}

	/**
	 * Tests that slots are updated with respect to the latest incoming slot report. This means that
	 * slots for which a report was received are updated accordingly.
	 */
	@Test
	public void testUpdateSlotReport() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();

		final ResourceID resourceId = ResourceID.generate();
		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);


		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);

		final SlotStatus newSlotStatus2 = new SlotStatus(slotId2, resourceProfile, jobId, allocationId);

		final SlotReport slotReport1 = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));
		final SlotReport slotReport2 = new SlotReport(Arrays.asList(newSlotStatus2, slotStatus1));

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {
			// check that we don't have any slots registered
			assertTrue(0 == slotManager.getNumberRegisteredSlots());

			slotManager.registerTaskManager(taskManagerConnection, slotReport1);

			TaskManagerSlot slot1 = slotManager.getSlot(slotId1);
			TaskManagerSlot slot2 = slotManager.getSlot(slotId2);

			assertTrue(2 == slotManager.getNumberRegisteredSlots());

			assertTrue(slot1.isFree());
			assertTrue(slot2.isFree());

			assertTrue(slotManager.reportSlotStatus(taskManagerConnection.getInstanceID(), slotReport2));

			assertTrue(2 == slotManager.getNumberRegisteredSlots());

			// the slot manager should have removed slotId1
			assertNotNull(slotManager.getSlot(slotId1));
			assertNotNull(slotManager.getSlot(slotId2));

			// slotId2 should have been allocated for allocationId
			assertEquals(allocationId, slotManager.getSlot(slotId2).getAllocationId());
		}
	}

	/**
	 * Tests that idle task managers time out after the configured timeout. A timed out task manager
	 * will be removed from the slot manager and the resource manager will be notified about the
	 * timeout.
	 */
	@Test
	public void testTaskManagerTimeout() throws Exception {
		final long tmTimeout = 500L;

		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final UUID leaderId = UUID.randomUUID();

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotID slotId = new SlotID(ResourceID.generate(), 0);
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotStatus slotStatus = new SlotStatus(slotId, resourceProfile);
		final SlotReport slotReport = new SlotReport(slotStatus);

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (SlotManager slotManager = new SlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			Time.milliseconds(tmTimeout))) {

			slotManager.start(leaderId, mainThreadExecutor, resourceManagerActions);

			mainThreadExecutor.execute(new Runnable() {
				@Override
				public void run() {
					slotManager.registerTaskManager(taskManagerConnection, slotReport);
				}
			});

			verify(resourceManagerActions, timeout(100L * tmTimeout).times(1))
				.releaseResource(eq(taskManagerConnection.getInstanceID()));
		}
	}

	/**
	 * Tests that slot requests time out after the specified request timeout. If a slot request
	 * times out, then the request is cancelled, removed from the slot manager and the resource
	 * manager is notified about the failed allocation.
	 */
	@Test
	public void testSlotRequestTimeout() throws Exception {
		final long allocationTimeout = 50L;

		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final UUID leaderId = UUID.randomUUID();
		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();

		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (SlotManager slotManager = new SlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			Time.milliseconds(allocationTimeout),
			TestingUtils.infiniteTime())) {

			slotManager.start(leaderId, mainThreadExecutor, resourceManagerActions);

			final AtomicReference<Exception> atomicException = new AtomicReference<>(null);

			mainThreadExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						assertTrue(slotManager.registerSlotRequest(slotRequest));
					} catch (Exception e) {
						atomicException.compareAndSet(null, e);
					}
				}
			});

			verify(resourceManagerActions, timeout(100L * allocationTimeout).times(1)).notifyAllocationFailure(
				eq(jobId),
				eq(allocationId),
				any(TimeoutException.class));

			if (atomicException.get() != null) {
				throw atomicException.get();
			}
		}
	}

	/**
	 * Tests that a slot request is retried if it times out on the task manager side
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testTaskManagerSlotRequestTimeoutHandling() throws Exception {
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, "foobar");
		final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();
		final CompletableFuture<Acknowledge> slotRequestFuture2 = new CompletableFuture<>();

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			eq(allocationId),
			anyString(),
			any(UUID.class),
			any(Time.class))).thenReturn(slotRequestFuture1, slotRequestFuture2);

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final ResourceID resourceId = ResourceID.generate();
		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		try (SlotManager slotManager = createSlotManager(leaderId, resourceManagerActions)) {

			slotManager.registerTaskManager(taskManagerConnection, slotReport);

			slotManager.registerSlotRequest(slotRequest);

			ArgumentCaptor<SlotID> slotIdCaptor = ArgumentCaptor.forClass(SlotID.class);

			verify(taskExecutorGateway, times(1)).requestSlot(
				slotIdCaptor.capture(),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class));

			TaskManagerSlot failedSlot = slotManager.getSlot(slotIdCaptor.getValue());

			// let the first attempt fail --> this should trigger a second attempt
			slotRequestFuture1.completeExceptionally(new SlotAllocationException("Test exception."));

			verify(taskExecutorGateway, times(2)).requestSlot(
				slotIdCaptor.capture(),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class));

			// the second attempt succeeds
			slotRequestFuture2.complete(Acknowledge.get());

			TaskManagerSlot slot = slotManager.getSlot(slotIdCaptor.getValue());

			assertTrue(slot.isAllocated());
			assertEquals(allocationId, slot.getAllocationId());

			if (!failedSlot.getSlotId().equals(slot.getSlotId())) {
				assertTrue(failedSlot.isFree());
			}
		}
	}

	/**
	 * Tests that pending slot requests are rejected if a slot report with a different allocation
	 * is received.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testSlotReportWhileActiveSlotRequest() throws Exception {
		final long verifyTimeout = 1000L;
		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);

		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(42.0, 1337);
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, "foobar");
		final CompletableFuture<Acknowledge> slotRequestFuture1 = new CompletableFuture<>();

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			any(JobID.class),
			eq(allocationId),
			anyString(),
			any(UUID.class),
			any(Time.class))).thenReturn(slotRequestFuture1, CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final ResourceID resourceId = ResourceID.generate();
		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport slotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (final SlotManager slotManager = new SlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			slotManager.start(leaderId, mainThreadExecutor, resourceManagerActions);

			CompletableFuture<Void> registrationFuture = CompletableFuture.supplyAsync(
				() -> {
					slotManager.registerTaskManager(taskManagerConnection, slotReport);

					return null;
				},
				mainThreadExecutor)
			.thenAccept(
				(Object value) -> {
					try {
						slotManager.registerSlotRequest(slotRequest);
					} catch (SlotManagerException e) {
						throw new RuntimeException("Could not register slots.", e);
					}
				});

			// check that no exception has been thrown
			registrationFuture.get();

			ArgumentCaptor<SlotID> slotIdCaptor = ArgumentCaptor.forClass(SlotID.class);

			verify(taskExecutorGateway, times(1)).requestSlot(
				slotIdCaptor.capture(),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class));

			final SlotID requestedSlotId = slotIdCaptor.getValue();
			final SlotID freeSlotId = requestedSlotId.equals(slotId1) ? slotId2 : slotId1;

			CompletableFuture<Boolean> freeSlotFuture = CompletableFuture.supplyAsync(
				() -> slotManager.getSlot(freeSlotId).isFree(),
				mainThreadExecutor);

			assertTrue(freeSlotFuture.get());

			final SlotStatus newSlotStatus1 = new SlotStatus(slotIdCaptor.getValue(), resourceProfile, new JobID(), new AllocationID());
			final SlotStatus newSlotStatus2 = new SlotStatus(freeSlotId, resourceProfile);
			final SlotReport newSlotReport = new SlotReport(Arrays.asList(newSlotStatus1, newSlotStatus2));

			CompletableFuture.supplyAsync(
				// this should update the slot with the pending slot request triggering the reassignment of it
				() -> slotManager.reportSlotStatus(taskManagerConnection.getInstanceID(), newSlotReport),
				mainThreadExecutor);

			verify(taskExecutorGateway, timeout(verifyTimeout).times(2)).requestSlot(
				slotIdCaptor.capture(),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class));

			final SlotID requestedSlotId2 = slotIdCaptor.getValue();

			assertEquals(slotId2, requestedSlotId2);

			CompletableFuture<TaskManagerSlot> requestedSlotFuture = CompletableFuture.supplyAsync(
				() -> slotManager.getSlot(requestedSlotId2),
				mainThreadExecutor);

			TaskManagerSlot slot = requestedSlotFuture.get();

			assertTrue(slot.isAllocated());
			assertEquals(allocationId, slot.getAllocationId());
		}
	}

	/**
	 * Tests that formerly used task managers can again timeout after all of their slots have
	 * been freed.
	 */
	@Test
	public void testTimeoutForUnusedTaskManager() throws Exception {
		final long taskManagerTimeout = 50L;
		final long verifyTimeout = taskManagerTimeout * 10L;

		final UUID leaderId = UUID.randomUUID();
		final ResourceManagerActions resourceManagerActions = mock(ResourceManagerActions.class);
		final ScheduledExecutor scheduledExecutor = TestingUtils.defaultScheduledExecutor();

		final ResourceID resourceId = ResourceID.generate();

		final JobID jobId = new JobID();
		final AllocationID allocationId = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 1);
		final SlotRequest slotRequest = new SlotRequest(jobId, allocationId, resourceProfile, "foobar");

		final TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.requestSlot(
			any(SlotID.class),
			eq(jobId),
			eq(allocationId),
			anyString(),
			eq(leaderId),
			any(Time.class))).thenReturn(CompletableFuture.completedFuture(Acknowledge.get()));

		final TaskExecutorConnection taskManagerConnection = new TaskExecutorConnection(taskExecutorGateway);

		final SlotID slotId1 = new SlotID(resourceId, 0);
		final SlotID slotId2 = new SlotID(resourceId, 1);
		final SlotStatus slotStatus1 = new SlotStatus(slotId1, resourceProfile);
		final SlotStatus slotStatus2 = new SlotStatus(slotId2, resourceProfile);
		final SlotReport initialSlotReport = new SlotReport(Arrays.asList(slotStatus1, slotStatus2));

		final Executor mainThreadExecutor = TestingUtils.defaultExecutor();

		try (final SlotManager slotManager = new SlotManager(
			scheduledExecutor,
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			Time.of(taskManagerTimeout, TimeUnit.MILLISECONDS))) {

			slotManager.start(leaderId, mainThreadExecutor, resourceManagerActions);

			CompletableFuture.supplyAsync(
				() -> {
					try {
						return slotManager.registerSlotRequest(slotRequest);
					} catch (SlotManagerException e) {
						throw new FlinkFutureException(e);
					}
				},
				mainThreadExecutor)
			.thenAccept((Object value) -> slotManager.registerTaskManager(taskManagerConnection, initialSlotReport));

			ArgumentCaptor<SlotID> slotIdArgumentCaptor = ArgumentCaptor.forClass(SlotID.class);

			verify(taskExecutorGateway, timeout(verifyTimeout)).requestSlot(
				slotIdArgumentCaptor.capture(),
				eq(jobId),
				eq(allocationId),
				anyString(),
				eq(leaderId),
				any(Time.class));

			CompletableFuture<Boolean> idleFuture = CompletableFuture.supplyAsync(
				() -> slotManager.isTaskManagerIdle(taskManagerConnection.getInstanceID()),
				mainThreadExecutor);

			// check that the TaskManaer is not idle
			assertFalse(idleFuture.get());

			final SlotID slotId = slotIdArgumentCaptor.getValue();

			CompletableFuture<TaskManagerSlot> slotFuture = CompletableFuture.supplyAsync(
				() -> slotManager.getSlot(slotId),
				mainThreadExecutor);

			TaskManagerSlot slot = slotFuture.get();

			assertTrue(slot.isAllocated());
			assertEquals(allocationId, slot.getAllocationId());

			CompletableFuture<Boolean> idleFuture2 = CompletableFuture.runAsync(
				() -> slotManager.freeSlot(slotId, allocationId),
				mainThreadExecutor)
			.thenApply((Object value) -> slotManager.isTaskManagerIdle(taskManagerConnection.getInstanceID()));

			assertTrue(idleFuture2.get());

			verify(resourceManagerActions, timeout(verifyTimeout).times(1)).releaseResource(eq(taskManagerConnection.getInstanceID()));
		}
	}

	private SlotManager createSlotManager(UUID leaderId, ResourceManagerActions resourceManagerActions) {
		SlotManager slotManager = new SlotManager(
			TestingUtils.defaultScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		slotManager.start(leaderId, Executors.directExecutor(), resourceManagerActions);

		return slotManager;
	}
}

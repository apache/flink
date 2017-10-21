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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class SlotProtocolTest extends TestLogger {

	private static final long timeout = 10000L;

	private static final ScheduledExecutorService scheduledExecutorService = 
			new ScheduledThreadPoolExecutor(1);


	private static final ScheduledExecutor scheduledExecutor = 
			new ScheduledExecutorServiceAdapter(scheduledExecutorService);

	@AfterClass
	public static void afterClass() {
		Executors.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, scheduledExecutorService);
	}

	/**
	 * Tests whether
	 * 1) SlotManager accepts a slot request
	 * 2) SlotRequest leads to a container allocation
	 * 3) Slot becomes available and TaskExecutor gets a SlotRequest
	 */
	@Test
	public void testSlotsUnavailableRequest() throws Exception {
		final JobID jobID = new JobID();

		final ResourceManagerId rmLeaderID = ResourceManagerId.generate();

		try (SlotManager slotManager = new SlotManager(
			scheduledExecutor,
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			ResourceActions resourceManagerActions = mock(ResourceActions.class);

			slotManager.start(rmLeaderID, Executors.directExecutor(), resourceManagerActions);

			final AllocationID allocationID = new AllocationID();
			final ResourceProfile resourceProfile = new ResourceProfile(1.0, 100);
			final String targetAddress = "foobar";

			SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile, targetAddress);

			slotManager.registerSlotRequest(slotRequest);

			verify(resourceManagerActions).allocateResource(eq(slotRequest.getResourceProfile()));

			// slot becomes available
			TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
			Mockito.when(
				taskExecutorGateway
					.requestSlot(any(SlotID.class), any(JobID.class), any(AllocationID.class), any(String.class), any(ResourceManagerId.class), any(Time.class)))
				.thenReturn(mock(CompletableFuture.class));

			final ResourceID resourceID = ResourceID.generate();
			final SlotID slotID = new SlotID(resourceID, 0);

			final SlotStatus slotStatus =
				new SlotStatus(slotID, resourceProfile);
			final SlotReport slotReport =
				new SlotReport(Collections.singletonList(slotStatus));
			// register slot at SlotManager
			slotManager.registerTaskManager(new TaskExecutorConnection(taskExecutorGateway), slotReport);

			// 4) Slot becomes available and TaskExecutor gets a SlotRequest
			verify(taskExecutorGateway, timeout(5000L))
				.requestSlot(eq(slotID), eq(jobID), eq(allocationID), any(String.class), any(ResourceManagerId.class), any(Time.class));
		}
	}

	/**
	 * Tests whether
	 * 1) a SlotRequest is routed to the SlotManager
	 * 2) a SlotRequest is confirmed
	 * 3) a SlotRequest leads to an allocation of a registered slot
	 * 4) a SlotRequest is routed to the TaskExecutor
	 */
	@Test
	public void testSlotAvailableRequest() throws Exception {
		final JobID jobID = new JobID();

		final ResourceManagerId rmLeaderID = ResourceManagerId.generate();

		TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		Mockito.when(
			taskExecutorGateway
				.requestSlot(any(SlotID.class), any(JobID.class), any(AllocationID.class), any(String.class), any(ResourceManagerId.class), any(Time.class)))
			.thenReturn(mock(CompletableFuture.class));

		try (SlotManager slotManager = new SlotManager(
			scheduledExecutor,
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime())) {

			ResourceActions resourceManagerActions = mock(ResourceActions.class);

			slotManager.start(rmLeaderID, Executors.directExecutor(), resourceManagerActions);

			final ResourceID resourceID = ResourceID.generate();
			final AllocationID allocationID = new AllocationID();
			final ResourceProfile resourceProfile = new ResourceProfile(1.0, 100);
			final SlotID slotID = new SlotID(resourceID, 0);

			final SlotStatus slotStatus =
				new SlotStatus(slotID, resourceProfile);
			final SlotReport slotReport =
				new SlotReport(Collections.singletonList(slotStatus));
			// register slot at SlotManager
			slotManager.registerTaskManager(
				new TaskExecutorConnection(taskExecutorGateway), slotReport);

			final String targetAddress = "foobar";

			SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile, targetAddress);
			slotManager.registerSlotRequest(slotRequest);

			// a SlotRequest is routed to the TaskExecutor
			verify(taskExecutorGateway, timeout(5000))
				.requestSlot(eq(slotID), eq(jobID), eq(allocationID), any(String.class), any(ResourceManagerId.class), any(Time.class));
		}
	}
}

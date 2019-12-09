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
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the slot allocation protocol.
 */
public class SlotProtocolTest extends TestLogger {

	private static final long timeout = 10000L;

	private static final ScheduledExecutorService scheduledExecutorService =
			new ScheduledThreadPoolExecutor(1);


	private static final ScheduledExecutor scheduledExecutor =
			new ScheduledExecutorServiceAdapter(scheduledExecutorService);

	@AfterClass
	public static void afterClass() {
		ExecutorUtils.gracefulShutdown(timeout, TimeUnit.MILLISECONDS, scheduledExecutorService);
	}

	/**
	 * Tests whether
	 * 1) SlotManager accepts a slot request.
	 * 2) SlotRequest leads to a container allocation.
	 * 3) Slot becomes available and TaskExecutor gets a SlotRequest.
	 */
	@Test
	public void testSlotsUnavailableRequest() throws Exception {
		final JobID jobID = new JobID();

		final ResourceManagerId rmLeaderID = ResourceManagerId.generate();

		try (SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(scheduledExecutor)
			.build()) {

			final CompletableFuture<ResourceProfile> resourceProfileFuture = new CompletableFuture<>();
			ResourceActions resourceManagerActions = new TestingResourceActionsBuilder()
				.setAllocateResourceConsumer(resourceProfileFuture::complete)
				.build();

			slotManager.start(rmLeaderID, Executors.directExecutor(), resourceManagerActions);

			final AllocationID allocationID = new AllocationID();
			final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 100);
			final String targetAddress = "foobar";

			SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile, targetAddress);

			slotManager.registerSlotRequest(slotRequest);

			assertThat(resourceProfileFuture.get(), is(equalTo(slotRequest.getResourceProfile())));

			// slot becomes available
			final CompletableFuture<Tuple3<SlotID, JobID, AllocationID>> requestFuture = new CompletableFuture<>();
			TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setRequestSlotFunction(tuple5 -> {
					requestFuture.complete(Tuple3.of(tuple5.f0, tuple5.f1, tuple5.f2));
					return new CompletableFuture<>();
				})
				.createTestingTaskExecutorGateway();

			final ResourceID resourceID = ResourceID.generate();
			final SlotID slotID = new SlotID(resourceID, 0);

			final SlotStatus slotStatus =
				new SlotStatus(slotID, resourceProfile);
			final SlotReport slotReport =
				new SlotReport(Collections.singletonList(slotStatus));
			// register slot at SlotManager
			slotManager.registerTaskManager(new TaskExecutorConnection(resourceID, taskExecutorGateway), slotReport);

			// 4) Slot becomes available and TaskExecutor gets a SlotRequest
			assertThat(requestFuture.get(), is(equalTo(Tuple3.of(slotID, jobID, allocationID))));
		}
	}

	/**
	 * Tests whether
	 * 1) a SlotRequest is routed to the SlotManager.
	 * 2) a SlotRequest is confirmed.
	 * 3) a SlotRequest leads to an allocation of a registered slot.
	 * 4) a SlotRequest is routed to the TaskExecutor.
	 */
	@Test
	public void testSlotAvailableRequest() throws Exception {
		final JobID jobID = new JobID();

		final ResourceManagerId rmLeaderID = ResourceManagerId.generate();

		final CompletableFuture<Tuple3<SlotID, JobID, AllocationID>> requestFuture = new CompletableFuture<>();
		TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setRequestSlotFunction(tuple5 -> {
				requestFuture.complete(Tuple3.of(tuple5.f0, tuple5.f1, tuple5.f2));
				return new CompletableFuture<>();
			})
			.createTestingTaskExecutorGateway();

		try (SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(scheduledExecutor)
			.build()) {

			ResourceActions resourceManagerActions = new TestingResourceActionsBuilder().build();

			slotManager.start(rmLeaderID, Executors.directExecutor(), resourceManagerActions);

			final ResourceID resourceID = ResourceID.generate();
			final AllocationID allocationID = new AllocationID();
			final ResourceProfile resourceProfile = ResourceProfile.fromResources(1.0, 100);
			final SlotID slotID = new SlotID(resourceID, 0);

			final SlotStatus slotStatus =
				new SlotStatus(slotID, resourceProfile);
			final SlotReport slotReport =
				new SlotReport(Collections.singletonList(slotStatus));
			// register slot at SlotManager
			slotManager.registerTaskManager(
				new TaskExecutorConnection(resourceID, taskExecutorGateway), slotReport);

			final String targetAddress = "foobar";

			SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile, targetAddress);
			slotManager.registerSlotRequest(slotRequest);

			// a SlotRequest is routed to the TaskExecutor
			assertThat(requestFuture.get(), is(equalTo(Tuple3.of(slotID, jobID, allocationID))));
		}
	}
}

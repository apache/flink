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
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.*;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class SlotProtocolTest extends TestLogger {

	private static TestingSerialRpcService testRpcService;

	@BeforeClass
	public static void beforeClass() {
		testRpcService = new TestingSerialRpcService();
	}

	@AfterClass
	public static void afterClass() {
		testRpcService.stopService();
		testRpcService = null;
	}

	@Before
	public void beforeTest(){
		testRpcService.clearGateways();
	}

	/**
	 * Tests whether
	 * 1) SlotRequest is routed to the SlotManager
	 * 2) SlotRequest is confirmed
	 * 3) SlotRequest leads to a container allocation
	 * 4) Slot becomes available and TaskExecutor gets a SlotRequest
	 */
	@Test
	public void testSlotsUnavailableRequest() throws Exception {
		final String rmAddress = "/rm1";
		final String jmAddress = "/jm1";
		final JobID jobID = new JobID();

		testRpcService.registerGateway(jmAddress, mock(JobMasterGateway.class));

		final TestingHighAvailabilityServices testingHaServices = new TestingHighAvailabilityServices();
		final UUID rmLeaderID = UUID.randomUUID();
		final UUID jmLeaderID = UUID.randomUUID();
		TestingLeaderElectionService rmLeaderElectionService =
			configureHA(testingHaServices, jobID, rmAddress, rmLeaderID, jmAddress, jmLeaderID);

		SlotManager slotManager = Mockito.spy(new SimpleSlotManager());
		ResourceManager resourceManager =
			Mockito.spy(new StandaloneResourceManager(testRpcService, testingHaServices, slotManager));
		resourceManager.start();
		rmLeaderElectionService.isLeader(rmLeaderID);

		Future<RegistrationResponse> registrationFuture =
			resourceManager.registerJobMaster(rmLeaderID, jmLeaderID, jmAddress, jobID);
		try {
			registrationFuture.get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			Assert.fail("JobManager registration Future didn't become ready.");
		}

		final AllocationID allocationID = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 100);

		SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile);
		SlotRequestReply slotRequestReply =
			resourceManager.requestSlot(jmLeaderID, rmLeaderID, slotRequest);

		// 1) SlotRequest is routed to the SlotManager
		verify(slotManager).requestSlot(slotRequest);

		// 2) SlotRequest is confirmed
		Assert.assertEquals(
			slotRequestReply.getAllocationID(),
			allocationID);

		// 3) SlotRequest leads to a container allocation
		verify(resourceManager, timeout(5000)).startNewWorker(resourceProfile);

		Assert.assertFalse(slotManager.isAllocated(allocationID));

		// slot becomes available
		final String tmAddress = "/tm1";
		TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		Mockito.when(taskExecutorGateway.requestSlot(any(AllocationID.class), any(UUID.class), any(Time.class)))
			.thenReturn(new FlinkCompletableFuture<SlotRequestReply>());
		testRpcService.registerGateway(tmAddress, taskExecutorGateway);

		final ResourceID resourceID = ResourceID.generate();
		final SlotID slotID = new SlotID(resourceID, 0);

		final SlotStatus slotStatus =
			new SlotStatus(slotID, resourceProfile);
		final SlotReport slotReport =
			new SlotReport(Collections.singletonList(slotStatus), resourceID);
		// register slot at SlotManager
		slotManager.registerTaskExecutor(resourceID, taskExecutorGateway);
		slotManager.updateSlotStatus(slotReport);

		// 4) Slot becomes available and TaskExecutor gets a SlotRequest
		verify(taskExecutorGateway, timeout(5000)).requestSlot(eq(allocationID), any(UUID.class), any(Time.class));
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
		final String rmAddress = "/rm1";
		final String jmAddress = "/jm1";
		final String tmAddress = "/tm1";
		final JobID jobID = new JobID();

		testRpcService.registerGateway(jmAddress, mock(JobMasterGateway.class));

		final TestingHighAvailabilityServices testingHaServices = new TestingHighAvailabilityServices();
		final UUID rmLeaderID = UUID.randomUUID();
		final UUID jmLeaderID = UUID.randomUUID();
		TestingLeaderElectionService rmLeaderElectionService =
			configureHA(testingHaServices, jobID, rmAddress, rmLeaderID, jmAddress, jmLeaderID);

		TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		Mockito.when(taskExecutorGateway.requestSlot(any(AllocationID.class), any(UUID.class), any(Time.class)))
			.thenReturn(new FlinkCompletableFuture<SlotRequestReply>());
		testRpcService.registerGateway(tmAddress, taskExecutorGateway);

		SlotManager slotManager = Mockito.spy(new SimpleSlotManager());
		ResourceManager resourceManager =
			Mockito.spy(new StandaloneResourceManager(testRpcService, testingHaServices, slotManager));
		resourceManager.start();
		rmLeaderElectionService.isLeader(rmLeaderID);

		Future<RegistrationResponse> registrationFuture =
			resourceManager.registerJobMaster(rmLeaderID, jmLeaderID, jmAddress, jobID);
		try {
			registrationFuture.get(5, TimeUnit.SECONDS);
		} catch (Exception e) {
			Assert.fail("JobManager registration Future didn't become ready.");
		}

		final ResourceID resourceID = ResourceID.generate();
		final AllocationID allocationID = new AllocationID();
		final ResourceProfile resourceProfile = new ResourceProfile(1.0, 100);
		final SlotID slotID = new SlotID(resourceID, 0);

		final SlotStatus slotStatus =
			new SlotStatus(slotID, resourceProfile);
		final SlotReport slotReport =
			new SlotReport(Collections.singletonList(slotStatus), resourceID);
		// register slot at SlotManager
		slotManager.registerTaskExecutor(resourceID, taskExecutorGateway);
		slotManager.updateSlotStatus(slotReport);

		SlotRequest slotRequest = new SlotRequest(jobID, allocationID, resourceProfile);
		SlotRequestReply slotRequestReply =
			resourceManager.requestSlot(jmLeaderID, rmLeaderID, slotRequest);

		// 1) a SlotRequest is routed to the SlotManager
		verify(slotManager).requestSlot(slotRequest);

		// 2) a SlotRequest is confirmed
		Assert.assertEquals(
			slotRequestReply.getAllocationID(),
			allocationID);

		// 3) a SlotRequest leads to an allocation of a registered slot
		Assert.assertTrue(slotManager.isAllocated(slotID));
		Assert.assertTrue(slotManager.isAllocated(allocationID));


		// 4) a SlotRequest is routed to the TaskExecutor
		verify(taskExecutorGateway, timeout(5000)).requestSlot(eq(allocationID), any(UUID.class), any(Time.class));
	}

	private static TestingLeaderElectionService configureHA(
			TestingHighAvailabilityServices testingHA, JobID jobID, String rmAddress, UUID rmID, String jmAddress, UUID jmID) {
		final TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		testingHA.setResourceManagerLeaderElectionService(rmLeaderElectionService);
		final TestingLeaderRetrievalService rmLeaderRetrievalService = new TestingLeaderRetrievalService(rmAddress, rmID);
		testingHA.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		final TestingLeaderElectionService jmLeaderElectionService = new TestingLeaderElectionService();
		testingHA.setJobMasterLeaderElectionService(jmLeaderElectionService);
		final TestingLeaderRetrievalService jmLeaderRetrievalService = new TestingLeaderRetrievalService(jmAddress, jmID);
		testingHA.setJobMasterLeaderRetriever(jobID, jmLeaderRetrievalService);

		return rmLeaderElectionService;
	}

}

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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SimpleSlotManager;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.exceptions.LeaderSessionIDException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ResourceManagerJobMasterTest {

	private TestingSerialRpcService rpcService;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingSerialRpcService();
	}

	@After
	public void teardown() throws Exception {
		rpcService.stopService();
	}

	/**
	 * Test receive normal registration from job master and receive duplicate registration from job master
	 */
	@Test
	public void testRegisterJobMaster() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		UUID jmLeaderID = UUID.randomUUID();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(jobMasterAddress, jmLeaderID);
		final ResourceManager resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test response successful
		Future<RegistrationResponse> successfulFuture = resourceManager.registerJobMaster(rmLeaderSessionId, jmLeaderID, jobMasterAddress, jobID);
		RegistrationResponse response = successfulFuture.get(5, TimeUnit.SECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);
	}

	/**
	 * Test receive registration with unmatched leadershipId from job master
	 */
	@Test
	public void testRegisterJobMasterWithUnmatchedLeaderSessionId1() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		UUID jmLeaderID = UUID.randomUUID();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(jobMasterAddress, jmLeaderID);
		final ResourceManager resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		UUID differentLeaderSessionID = UUID.randomUUID();
		Future<RegistrationResponse> unMatchedLeaderFuture = resourceManager.registerJobMaster(differentLeaderSessionID, jmLeaderID, jobMasterAddress, jobID);
		assertTrue(unMatchedLeaderFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);
	}

	/**
	 * Test receive registration with unmatched leadershipId from job master
	 */
	@Test
	public void testRegisterJobMasterWithUnmatchedLeaderSessionId2() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService();
		final ResourceManager resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		UUID differentLeaderSessionID = UUID.randomUUID();
		Future<RegistrationResponse> unMatchedLeaderFuture = resourceManager.registerJobMaster(rmLeaderSessionId, differentLeaderSessionID, jobMasterAddress, jobID);
		assertTrue(unMatchedLeaderFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);
	}

	/**
	 * Test receive registration with invalid address from job master
	 */
	@Test
	public void testRegisterJobMasterFromInvalidAddress() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService();
		final ResourceManager resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test throw exception when receive a registration from job master which takes invalid address
		String invalidAddress = "/jobMasterAddress2";
		Future<RegistrationResponse> invalidAddressFuture = resourceManager.registerJobMaster(rmLeaderSessionId, jmLeaderSessionId, invalidAddress, jobID);
		assertTrue(invalidAddressFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);
	}

	/**
	 * Check and verify return RegistrationResponse.Decline when failed to start a job master Leader retrieval listener
	 */
	@Test
	public void testRegisterJobMasterWithFailureLeaderListener() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService();
		final ResourceManager resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		JobID unknownJobIDToHAServices = new JobID();
		// verify return RegistrationResponse.Decline when failed to start a job master Leader retrieval listener
		Future<RegistrationResponse> declineFuture = resourceManager.registerJobMaster(rmLeaderSessionId, jmLeaderSessionId, jobMasterAddress, unknownJobIDToHAServices);
		RegistrationResponse response = declineFuture.get(5, TimeUnit.SECONDS);
		assertTrue(response instanceof RegistrationResponse.Decline);
	}

	private JobID mockJobMaster(String jobMasterAddress) {
		JobID jobID = new JobID();
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		rpcService.registerGateway(jobMasterAddress, jobMasterGateway);
		return jobID;
	}

	private ResourceManager createAndStartResourceManager(TestingLeaderElectionService resourceManagerLeaderElectionService, JobID jobID, TestingLeaderRetrievalService jobMasterLeaderRetrievalService) {
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		highAvailabilityServices.setJobMasterLeaderRetriever(jobID, jobMasterLeaderRetrievalService);
		ResourceManager resourceManager = new ResourceManager(rpcService, highAvailabilityServices, new SimpleSlotManager());
		resourceManager.start();
		return resourceManager;
	}

	private UUID grantResourceManagerLeadership(TestingLeaderElectionService resourceManagerLeaderElectionService) {
		UUID leaderSessionId = UUID.randomUUID();
		resourceManagerLeaderElectionService.isLeader(leaderSessionId);
		return leaderSessionId;
	}

}

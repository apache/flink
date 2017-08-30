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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class ResourceManagerJobMasterTest extends TestLogger {

	private TestingRpcService rpcService;

	private final Time timeout = Time.seconds(10L);

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();
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
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(jobMasterAddress, jmLeaderID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test response successful
		CompletableFuture<RegistrationResponse> successfulFuture = rmGateway.registerJobManager(
			rmLeaderSessionId,
			jmLeaderID,
			jmResourceId,
			jobMasterAddress,
			jobID,
			timeout);
		RegistrationResponse response = successfulFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof JobMasterRegistrationSuccess);

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
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
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(jobMasterAddress, jmLeaderID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		UUID differentLeaderSessionID = UUID.randomUUID();
		CompletableFuture<RegistrationResponse> unMatchedLeaderFuture = rmGateway.registerJobManager(
			differentLeaderSessionID,
			jmLeaderID,
			jmResourceId,
			jobMasterAddress,
			jobID,
			timeout);
		assertTrue(unMatchedLeaderFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	/**
	 * Test receive registration with unmatched leadershipId from job master
	 */
	@Test
	public void testRegisterJobMasterWithUnmatchedLeaderSessionId2() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		UUID differentLeaderSessionID = UUID.randomUUID();
		CompletableFuture<RegistrationResponse> unMatchedLeaderFuture = rmGateway.registerJobManager(
			rmLeaderSessionId,
			differentLeaderSessionID,
			jmResourceId,
			jobMasterAddress,
			jobID,
			timeout);
		assertTrue(unMatchedLeaderFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	/**
	 * Test receive registration with invalid address from job master
	 */
	@Test
	public void testRegisterJobMasterFromInvalidAddress() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		// test throw exception when receive a registration from job master which takes invalid address
		String invalidAddress = "/jobMasterAddress2";
		CompletableFuture<RegistrationResponse> invalidAddressFuture = rmGateway.registerJobManager(
			rmLeaderSessionId,
			jmLeaderSessionId,
			jmResourceId,
			invalidAddress,
			jobID,
			timeout);
		assertTrue(invalidAddressFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	/**
	 * Check and verify return RegistrationResponse.Decline when failed to start a job master Leader retrieval listener
	 */
	@Test
	public void testRegisterJobMasterWithFailureLeaderListener() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingLeaderRetrievalService jobMasterLeaderRetrievalService = new TestingLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(
			resourceManagerLeaderElectionService,
			jobID,
			jobMasterLeaderRetrievalService,
			testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final UUID rmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final UUID jmLeaderSessionId = grantResourceManagerLeadership(resourceManagerLeaderElectionService);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		JobID unknownJobIDToHAServices = new JobID();
		// verify return RegistrationResponse.Decline when failed to start a job master Leader retrieval listener
		CompletableFuture<RegistrationResponse> declineFuture = rmGateway.registerJobManager(
			rmLeaderSessionId,
			jmLeaderSessionId,
			jmResourceId,
			jobMasterAddress,
			unknownJobIDToHAServices,
			timeout);
		RegistrationResponse response = declineFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		assertTrue(response instanceof RegistrationResponse.Decline);

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	private JobID mockJobMaster(String jobMasterAddress) {
		JobID jobID = new JobID();
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		rpcService.registerGateway(jobMasterAddress, jobMasterGateway);
		return jobID;
	}

	private ResourceManager createAndStartResourceManager(
			TestingLeaderElectionService resourceManagerLeaderElectionService,
			JobID jobID,
			TestingLeaderRetrievalService jobMasterLeaderRetrievalService,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		ResourceID rmResourceId = ResourceID.generate();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		highAvailabilityServices.setJobMasterLeaderRetriever(jobID, jobMasterLeaderRetrievalService);

		HeartbeatServices heartbeatServices = new HeartbeatServices(5L, 5L);

		ResourceManagerConfiguration resourceManagerConfiguration = new ResourceManagerConfiguration(
			Time.seconds(5L),
			Time.seconds(5L));
		MetricRegistry metricRegistry = mock(MetricRegistry.class);
		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));
			
		final SlotManager slotManager = new SlotManager(
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());

		ResourceManager resourceManager = new StandaloneResourceManager(
			rpcService,
			FlinkResourceManager.RESOURCE_MANAGER_NAME,
			rmResourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			fatalErrorHandler);
		resourceManager.start();
		return resourceManager;
	}

	private UUID grantResourceManagerLeadership(TestingLeaderElectionService resourceManagerLeaderElectionService) {
		UUID leaderSessionId = UUID.randomUUID();
		resourceManagerLeaderElectionService.isLeader(leaderSessionId);
		return leaderSessionId;
	}

}

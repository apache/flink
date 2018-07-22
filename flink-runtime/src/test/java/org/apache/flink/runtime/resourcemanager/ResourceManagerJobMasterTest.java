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
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.JobMasterRegistrationSuccess;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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
		RpcUtils.terminateRpcService(rpcService, timeout);
	}

	/**
	 * Test receive normal registration from job master and receive duplicate registration from job master
	 */
	@Test
	public void testRegisterJobMaster() throws Exception {
		String jobMasterAddress = "/jobMasterAddress1";
		JobID jobID = mockJobMaster(jobMasterAddress);
		JobMasterId jobMasterId = JobMasterId.generate();
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);
		SettableLeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(jobMasterAddress, jobMasterId.toUUID());
		TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		// wait until the leader election has been completed
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// test response successful
		CompletableFuture<RegistrationResponse> successfulFuture = rmGateway.registerJobManager(
			jobMasterId,
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
		JobMasterId jobMasterId = JobMasterId.generate();
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);
		SettableLeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(jobMasterAddress, jobMasterId.toUUID());
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(mock(LeaderElectionService.class), jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway wronglyFencedGateway = rpcService.connect(resourceManager.getAddress(), ResourceManagerId.generate(), ResourceManagerGateway.class)
			.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		CompletableFuture<RegistrationResponse> unMatchedLeaderFuture = wronglyFencedGateway.registerJobManager(
			jobMasterId,
			jmResourceId,
			jobMasterAddress,
			jobID,
			timeout);

		try {
			unMatchedLeaderFuture.get(5L, TimeUnit.SECONDS);
			fail("Should fail because we are using the wrong fencing token.");
		} catch (ExecutionException e) {
			assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
		}

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
		SettableLeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		// wait until the leader election has been completed
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// test throw exception when receive a registration from job master which takes unmatched leaderSessionId
		JobMasterId differentJobMasterId = JobMasterId.generate();
		CompletableFuture<RegistrationResponse> unMatchedLeaderFuture = rmGateway.registerJobManager(
			differentJobMasterId,
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
		SettableLeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(resourceManagerLeaderElectionService, jobID, jobMasterLeaderRetrievalService, testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		// wait until the leader election has been completed
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// test throw exception when receive a registration from job master which takes invalid address
		String invalidAddress = "/jobMasterAddress2";
		CompletableFuture<RegistrationResponse> invalidAddressFuture = rmGateway.registerJobManager(
			new JobMasterId(HighAvailabilityServices.DEFAULT_LEADER_ID),
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
		SettableLeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(
			"localhost",
			HighAvailabilityServices.DEFAULT_LEADER_ID);
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final ResourceManager<?> resourceManager = createAndStartResourceManager(
			resourceManagerLeaderElectionService,
			jobID,
			jobMasterLeaderRetrievalService,
			testingFatalErrorHandler);
		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		final ResourceID jmResourceId = new ResourceID(jobMasterAddress);

		JobID unknownJobIDToHAServices = new JobID();

		// wait until the leader election has been completed
		resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// this should fail because we try to register a job leader listener for an unknown job id
		CompletableFuture<RegistrationResponse> registrationFuture = rmGateway.registerJobManager(
			new JobMasterId(HighAvailabilityServices.DEFAULT_LEADER_ID),
			jmResourceId,
			jobMasterAddress,
			unknownJobIDToHAServices,
			timeout);

		try {
			registrationFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
		} catch (ExecutionException e) {
			assertTrue(ExceptionUtils.stripExecutionException(e) instanceof ResourceManagerException);
		}
	}

	private JobID mockJobMaster(String jobMasterAddress) {
		JobID jobID = new JobID();
		JobMasterGateway jobMasterGateway = mock(JobMasterGateway.class);
		rpcService.registerGateway(jobMasterAddress, jobMasterGateway);
		return jobID;
	}

	private ResourceManager createAndStartResourceManager(
			LeaderElectionService resourceManagerLeaderElectionService,
			JobID jobID,
			LeaderRetrievalService jobMasterLeaderRetrievalService,
			FatalErrorHandler fatalErrorHandler) throws Exception {
		ResourceID rmResourceId = ResourceID.generate();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		highAvailabilityServices.setJobMasterLeaderRetriever(jobID, jobMasterLeaderRetrievalService);

		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

		MetricRegistryImpl metricRegistry = mock(MetricRegistryImpl.class);
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
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			fatalErrorHandler);
		resourceManager.start();
		return resourceManager;
	}
}

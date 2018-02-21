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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link StandaloneResourceManager}.
 */
public class StandaloneResourceManagerTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingHighAvailabilityServices haServices;

	private TestingLeaderElectionService resourceManagerLeaderElectionService;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
		haServices = new TestingHighAvailabilityServices();

		resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		haServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
	}

	@After
	public void tearDown() throws Exception {
		testingFatalErrorHandler.rethrowError();
	}

	@AfterClass
	public static void tearDownClass() {
		rpcService.stopService();
	}

	/**
	 * Tests that allocating a slot from the {@link StandaloneResourceManager}, which
	 * triggers a new worker to be started, fails immediately.
	 */
	@Test
	public void testSlotAllocationWithNewResourceFails() throws Exception {
		final Configuration configuration = new Configuration();

		final SlotManager slotManager = new SlotManager(
			rpcService.getScheduledExecutor(),
			timeout,
			timeout,
			timeout);

		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(haServices, rpcService.getScheduledExecutor(), timeout);

		final StandaloneResourceManager standaloneResourceManager = new StandaloneResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME + UUID.randomUUID(),
			ResourceID.generate(),
			ResourceManagerConfiguration.fromConfiguration(configuration),
			haServices,
			new HeartbeatServices(1000L, 1000L),
			slotManager,
			NoOpMetricRegistry.INSTANCE,
			jobLeaderIdService,
			new ClusterInformation("localhsot", 42),
			testingFatalErrorHandler);

		standaloneResourceManager.start();

		try {
			resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

			final ResourceManagerGateway resourceManagerGateway = standaloneResourceManager.getSelfGateway(ResourceManagerGateway.class);

			final JobMasterId jobMasterId = JobMasterId.generate();
			final JobID jobId = new JobID();

			final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGateway("foobar", jobMasterId);
			final String leaderAddress = jobMasterGateway.getAddress();
			rpcService.registerGateway(leaderAddress, jobMasterGateway);

			haServices.setJobMasterLeaderRetriever(jobId, new TestingLeaderRetrievalService(leaderAddress, jobMasterId.toUUID()));

			standaloneResourceManager.registerJobManager(
				jobMasterId,
				ResourceID.generate(),
				leaderAddress,
				jobId,
				timeout).get();

			final SlotRequest slotRequest = new SlotRequest(jobId, new AllocationID(), ResourceProfile.UNKNOWN, leaderAddress);

			final CompletableFuture<Acknowledge> slotAllocationFuture = resourceManagerGateway.requestSlot(jobMasterId, slotRequest, timeout);

			try {
				slotAllocationFuture.get();
				fail("Should have failed immediately");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.findThrowable(ee, ResourceManagerException.class).isPresent(), Matchers.is(true));
			}

		} finally {
			RpcUtils.terminateRpcEndpoint(standaloneResourceManager, timeout);
		}
	}
}

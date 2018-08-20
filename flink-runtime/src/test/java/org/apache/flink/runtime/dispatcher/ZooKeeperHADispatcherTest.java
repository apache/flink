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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.ZooKeeperSubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertThat;

/**
 * Test cases for the interaction between ZooKeeper HA and the {@link Dispatcher}.
 */
public class ZooKeeperHADispatcherTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	@ClassRule
	public static final ZooKeeperResource ZOO_KEEPER_RESOURCE = new ZooKeeperResource();

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static Configuration configuration;

	private static TestingRpcService rpcService;

	private static BlobServer blobServer;

	@Rule
	public TestName name = new TestName();

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() throws IOException {
		configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, ZOO_KEEPER_RESOURCE.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
		rpcService = new TestingRpcService();
		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Before
	public void setup() {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);
			rpcService = null;
		}

		if (blobServer != null) {
			blobServer.close();
			blobServer = null;
		}
	}

	/**
	 * Tests that the {@link Dispatcher} releases a locked {@link SubmittedJobGraph} if it
	 * lost the leadership.
	 */
	@Test
	public void testSubmittedJobGraphRelease() throws Exception {
		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final CuratorFramework otherClient = ZooKeeperUtils.startCuratorFramework(configuration);

		final TestingHighAvailabilityServices testingHighAvailabilityServices = new TestingHighAvailabilityServices();
		testingHighAvailabilityServices.setSubmittedJobGraphStore(ZooKeeperUtils.createSubmittedJobGraphs(client, configuration));

		final ZooKeeperSubmittedJobGraphStore otherSubmittedJobGraphStore = ZooKeeperUtils.createSubmittedJobGraphs(
			otherClient,
			configuration);

		otherSubmittedJobGraphStore.start(NoOpSubmittedJobGraphListener.INSTANCE);

		final TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		testingHighAvailabilityServices.setDispatcherLeaderElectionService(leaderElectionService);

		final TestingDispatcher dispatcher = createDispatcher(testingHighAvailabilityServices);

		dispatcher.start();

		try {
			final DispatcherId expectedLeaderId = DispatcherId.generate();
			leaderElectionService.isLeader(expectedLeaderId.toUUID()).get();

			final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			final JobGraph nonEmptyJobGraph = DispatcherHATest.createNonEmptyJobGraph();
			final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(nonEmptyJobGraph, TIMEOUT);
			submissionFuture.get();

			Collection<JobID> jobIds = otherSubmittedJobGraphStore.getJobIds();

			final JobID jobId = nonEmptyJobGraph.getJobID();
			assertThat(jobIds, Matchers.contains(jobId));

			leaderElectionService.notLeader();

			// wait for the job to properly terminate
			final CompletableFuture<Void> jobTerminationFuture = dispatcher.getJobTerminationFuture(jobId, TIMEOUT);
			jobTerminationFuture.get();

			// recover the job
			final SubmittedJobGraph submittedJobGraph = otherSubmittedJobGraphStore.recoverJobGraph(jobId);

			assertThat(submittedJobGraph, Matchers.is(Matchers.notNullValue()));

			// check that the other submitted job graph store can remove the job graph after the original leader
			// has lost its leadership
			otherSubmittedJobGraphStore.removeJobGraph(jobId);

			jobIds = otherSubmittedJobGraphStore.getJobIds();

			assertThat(jobIds, Matchers.not(Matchers.contains(jobId)));
		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
			client.close();
			otherClient.close();
		}
	}

	@Nonnull
	private TestingDispatcher createDispatcher(TestingHighAvailabilityServices testingHighAvailabilityServices) throws Exception {
		return new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
			configuration,
			testingHighAvailabilityServices,
			new TestingResourceManagerGateway(),
			blobServer,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			new TestingJobManagerRunnerFactory(new CompletableFuture<>(), new CompletableFuture<>()),
			testingFatalErrorHandler);
	}
}

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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.ZooKeeperSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cases for the interaction between ZooKeeper HA and the {@link Dispatcher}.
 */
public class ZooKeeperHADispatcherTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	@Rule
	public final ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

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
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getAbsolutePath());
		rpcService = new TestingRpcService();
		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Before
	public void setup() throws Exception {
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
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

		try (final TestingHighAvailabilityServices testingHighAvailabilityServices = new TestingHighAvailabilityServices()) {
			testingHighAvailabilityServices.setSubmittedJobGraphStore(ZooKeeperUtils.createSubmittedJobGraphs(client, configuration));

			final ZooKeeperSubmittedJobGraphStore otherSubmittedJobGraphStore = ZooKeeperUtils.createSubmittedJobGraphs(
				otherClient,
				configuration);

			otherSubmittedJobGraphStore.start(NoOpSubmittedJobGraphListener.INSTANCE);

			final TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
			testingHighAvailabilityServices.setDispatcherLeaderElectionService(leaderElectionService);

			final TestingDispatcher dispatcher = createDispatcher(
				testingHighAvailabilityServices,
				new TestingJobManagerRunnerFactory(new CompletableFuture<>(), new CompletableFuture<>(), CompletableFuture.completedFuture(null)));

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

				assertThat(submittedJobGraph, is(notNullValue()));

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
	}

	/**
	 * Tests that a standby Dispatcher does not interfere with the clean up of a completed
	 * job.
	 */
	@Test
	public void testStandbyDispatcherJobExecution() throws Exception {
		try (final TestingHighAvailabilityServices haServices1 = new TestingHighAvailabilityServices();
			final TestingHighAvailabilityServices haServices2 = new TestingHighAvailabilityServices();
			final CuratorFramework curatorFramework = ZooKeeperUtils.startCuratorFramework(configuration)) {

			final ZooKeeperSubmittedJobGraphStore submittedJobGraphStore1 = ZooKeeperUtils.createSubmittedJobGraphs(curatorFramework, configuration);
			haServices1.setSubmittedJobGraphStore(submittedJobGraphStore1);
			final TestingLeaderElectionService leaderElectionService1 = new TestingLeaderElectionService();
			haServices1.setDispatcherLeaderElectionService(leaderElectionService1);

			final ZooKeeperSubmittedJobGraphStore submittedJobGraphStore2 = ZooKeeperUtils.createSubmittedJobGraphs(curatorFramework, configuration);
			haServices2.setSubmittedJobGraphStore(submittedJobGraphStore2);
			final TestingLeaderElectionService leaderElectionService2 = new TestingLeaderElectionService();
			haServices2.setDispatcherLeaderElectionService(leaderElectionService2);

			final CompletableFuture<JobGraph> jobGraphFuture = new CompletableFuture<>();
			final CompletableFuture<ArchivedExecutionGraph> resultFuture = new CompletableFuture<>();
			final TestingDispatcher dispatcher1 = createDispatcher(
				haServices1,
				new TestingJobManagerRunnerFactory(jobGraphFuture, resultFuture, CompletableFuture.completedFuture(null)));

			final TestingDispatcher dispatcher2 = createDispatcher(
				haServices2,
				new TestingJobManagerRunnerFactory(new CompletableFuture<>(), new CompletableFuture<>(), CompletableFuture.completedFuture(null)));

			try {
				dispatcher1.start();
				dispatcher2.start();

				leaderElectionService1.isLeader(UUID.randomUUID()).get();
				final DispatcherGateway dispatcherGateway1 = dispatcher1.getSelfGateway(DispatcherGateway.class);

				final JobGraph jobGraph = DispatcherHATest.createNonEmptyJobGraph();

				dispatcherGateway1.submitJob(jobGraph, TIMEOUT).get();

				final CompletableFuture<JobResult> jobResultFuture = dispatcherGateway1.requestJobResult(jobGraph.getJobID(), TIMEOUT);

				jobGraphFuture.get();

				// complete the job
				resultFuture.complete(new ArchivedExecutionGraphBuilder().setJobID(jobGraph.getJobID()).setState(JobStatus.FINISHED).build());

				final JobResult jobResult = jobResultFuture.get();

				assertThat(jobResult.isSuccess(), is(true));

				// wait for the completion of the job
				dispatcher1.getJobTerminationFuture(jobGraph.getJobID(), TIMEOUT).get();

				// change leadership
				leaderElectionService1.notLeader();
				leaderElectionService2.isLeader(UUID.randomUUID()).get();

				// Dispatcher 2 should not recover any jobs
				final DispatcherGateway dispatcherGateway2 = dispatcher2.getSelfGateway(DispatcherGateway.class);
				assertThat(dispatcherGateway2.listJobs(TIMEOUT).get(), is(empty()));
			} finally {
				RpcUtils.terminateRpcEndpoint(dispatcher1, TIMEOUT);
				RpcUtils.terminateRpcEndpoint(dispatcher2, TIMEOUT);
			}
		}
	}

	/**
	 * Tests that a standby {@link Dispatcher} can recover all submitted jobs.
	 */
	@Test
	public void testStandbyDispatcherJobRecovery() throws Exception {
		try (CuratorFramework curatorFramework = ZooKeeperUtils.startCuratorFramework(configuration)) {

			HighAvailabilityServices haServices = null;
			Dispatcher dispatcher1 = null;
			Dispatcher dispatcher2 = null;

			try {
				haServices = new ZooKeeperHaServices(curatorFramework, rpcService.getExecutor(), configuration, new VoidBlobStore());

				final CompletableFuture<JobGraph> jobGraphFuture1 = new CompletableFuture<>();
				dispatcher1 = createDispatcher(
					haServices,
					new TestingJobManagerRunnerFactory(jobGraphFuture1, new CompletableFuture<>(), CompletableFuture.completedFuture(null)));
				final CompletableFuture<JobGraph> jobGraphFuture2 = new CompletableFuture<>();
				dispatcher2 = createDispatcher(
					haServices,
					new TestingJobManagerRunnerFactory(jobGraphFuture2, new CompletableFuture<>(), CompletableFuture.completedFuture(null)));

				dispatcher1.start();
				dispatcher2.start();

				final LeaderConnectionInfo leaderConnectionInfo = LeaderRetrievalUtils.retrieveLeaderConnectionInfo(haServices.getDispatcherLeaderRetriever(), TIMEOUT);

				final DispatcherGateway dispatcherGateway = rpcService.connect(leaderConnectionInfo.getAddress(), DispatcherId.fromUuid(leaderConnectionInfo.getLeaderSessionID()), DispatcherGateway.class).get();

				final JobGraph nonEmptyJobGraph = DispatcherHATest.createNonEmptyJobGraph();
				dispatcherGateway.submitJob(nonEmptyJobGraph, TIMEOUT).get();

				if (dispatcher1.getAddress().equals(leaderConnectionInfo.getAddress())) {
					dispatcher1.closeAsync();
					assertThat(jobGraphFuture2.get().getJobID(), is(equalTo(nonEmptyJobGraph.getJobID())));
				} else {
					dispatcher2.closeAsync();
					assertThat(jobGraphFuture1.get().getJobID(), is(equalTo(nonEmptyJobGraph.getJobID())));
				}
			} finally {
				if (dispatcher1 != null) {
					RpcUtils.terminateRpcEndpoint(dispatcher1, TIMEOUT);
				}

				if (dispatcher2 != null) {
					RpcUtils.terminateRpcEndpoint(dispatcher2, TIMEOUT);
				}

				if (haServices != null) {
					haServices.close();
				}
			}
		}
	}

	@Nonnull
	private TestingDispatcher createDispatcher(HighAvailabilityServices highAvailabilityServices, JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
		TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		return new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName() + UUID.randomUUID(),
			configuration,
			highAvailabilityServices,
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			blobServer,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			jobManagerRunnerFactory,
			testingFatalErrorHandler);
	}
}

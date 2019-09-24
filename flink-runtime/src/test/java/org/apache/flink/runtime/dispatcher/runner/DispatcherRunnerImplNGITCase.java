/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.minicluster.SessionDispatcherWithUUIDFactory;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.BlobServerResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for the {@link DispatcherRunnerImplNG}.
 */
public class DispatcherRunnerImplNGITCase extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final JobID TEST_JOB_ID = new JobID();

	@ClassRule
	public static TestingRpcServiceResource rpcServiceResource = new TestingRpcServiceResource();

	@ClassRule
	public static BlobServerResource blobServerResource = new BlobServerResource();

	private JobGraph jobGraph;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private TestingFatalErrorHandler fatalErrorHandler;

	private JobGraphStore jobGraphStore;

	private PartialDispatcherServices partialDispatcherServices;

	@Before
	public void setup() {
		jobGraph = createJobGraph();
		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		fatalErrorHandler = new TestingFatalErrorHandler();
		jobGraphStore = TestingJobGraphStore.newBuilder().build();

		partialDispatcherServices = new PartialDispatcherServices(
			new Configuration(),
			new TestingHighAvailabilityServicesBuilder().build(),
			CompletableFuture::new,
			blobServerResource.getBlobServer(),
			new TestingHeartbeatServices(),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			new MemoryArchivedExecutionGraphStore(),
			fatalErrorHandler,
			VoidHistoryServerArchivist.INSTANCE,
			null);
	}

	@After
	public void teardown() throws Exception {
		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
		}
	}

	@Test
	public void leaderChange_afterJobSubmission_recoversSubmittedJob() throws Exception {
		try (final DispatcherRunnerImplNG dispatcherRunner = createDispatcherRunner()) {
			final UUID firstLeaderSessionId = UUID.randomUUID();

			dispatcherLeaderElectionService.isLeader(firstLeaderSessionId);

			final DispatcherGateway firstDispatcherGateway = dispatcherRunner.getDispatcherGateway().get();

			firstDispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

			dispatcherLeaderElectionService.notLeader();

			final UUID secondLeaderSessionId = UUID.randomUUID();
			dispatcherLeaderElectionService.isLeader(secondLeaderSessionId).get();

			final DispatcherGateway secondDispatcherGateway = dispatcherRunner.getDispatcherGateway().get();

			final Collection<JobID> jobIds = secondDispatcherGateway.listJobs(TIMEOUT).get();

			assertThat(jobIds, hasSize(1));
			assertThat(jobIds, contains(jobGraph.getJobID()));
		}
	}

	private static JobGraph createJobGraph() {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph testJob = new JobGraph(TEST_JOB_ID, "testJob", testVertex);
		testJob.setAllowQueuedScheduling(true);

		return testJob;
	}

	private DispatcherRunnerImplNG createDispatcherRunner() throws Exception {
		final DispatcherRunnerImplNGFactory runnerFactory = DispatcherRunnerImplNGFactory.createSessionRunner(SessionDispatcherWithUUIDFactory.INSTANCE);

		return runnerFactory.createDispatcherRunner(
			dispatcherLeaderElectionService,
			fatalErrorHandler,
			() -> jobGraphStore,
			TestingUtils.defaultExecutor(),
			rpcServiceResource.getTestingRpcService(),
			partialDispatcherServices);
	}
}

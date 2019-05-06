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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherFactory;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherServices;
import org.apache.flink.runtime.dispatcher.JobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SingleJobJobGraphStore;
import org.apache.flink.runtime.dispatcher.StandaloneDispatcher;
import org.apache.flink.runtime.dispatcher.TestingJobManagerRunnerFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the {@link DispatcherRunnerImpl}.
 */
public class DispatcherRunnerImplTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(DispatcherRunnerImplTest.class);

	private static final Time TESTING_TIMEOUT = Time.seconds(10L);

	@ClassRule
	public static TestingRpcServiceResource testingRpcServiceResource = new TestingRpcServiceResource();

	private TestingFatalErrorHandler fatalErrorHandler;

	@Before
	public void setup() {
		fatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void teardown() throws Exception {
		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
		}
	}

	/**
	 * See FLINK-11843. This is a probabilistic test which needs to be executed several times to fail.
	 */
	@Test
	@Ignore
	public void testJobRecoveryUnderLeaderChange() throws Exception {
		final TestingRpcService rpcService = testingRpcServiceResource.getTestingRpcService();
		final TestingLeaderElectionService dispatcherLeaderElectionService = new TestingLeaderElectionService();
		final Configuration configuration = new Configuration();

		final JobGraph jobGraph = new JobGraph();
		final SingleJobJobGraphStore submittedJobGraphStore = new SingleJobJobGraphStore(jobGraph);

		try (final BlobServer blobServer = new BlobServer(configuration, new VoidBlobStore());
			final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServicesBuilder()
			.setJobGraphStore(submittedJobGraphStore)
			.setDispatcherLeaderElectionService(dispatcherLeaderElectionService)
			.build()) {

			final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(
				configuration,
				highAvailabilityServices,
				CompletableFuture::new,
				blobServer,
				new TestingHeartbeatServices(),
				UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup,
				new MemoryArchivedExecutionGraphStore(),
				fatalErrorHandler,
				VoidHistoryServerArchivist.INSTANCE,
				null);

			final TestingJobManagerRunnerFactory jobManagerRunnerFactory = new TestingJobManagerRunnerFactory(1);
			try (final DispatcherRunnerImpl dispatcherRunner = new DispatcherRunnerImpl(
				new TestingDispatcherFactory(jobManagerRunnerFactory),
				rpcService,
				partialDispatcherServices)) {
				// initial run
				grantLeadership(dispatcherLeaderElectionService, dispatcherRunner);
				final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();

				dispatcherLeaderElectionService.notLeader();

				LOG.info("Re-grant leadership first time.");
				dispatcherLeaderElectionService.isLeader(UUID.randomUUID());

				// give the Dispatcher some time to recover jobs
				Thread.sleep(1L);

				dispatcherLeaderElectionService.notLeader();

				LOG.info("Re-grant leadership second time.");
				final UUID leaderSessionId = UUID.randomUUID();
				final CompletableFuture<UUID> leaderFuture = dispatcherLeaderElectionService.isLeader(leaderSessionId);

				LOG.info("Complete the termination of the first job manager runner.");
				testingJobManagerRunner.completeTerminationFuture();

				assertThat(leaderFuture.get(TESTING_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS), is(equalTo(leaderSessionId)));
			}
		}
	}

	private DispatcherGateway grantLeadership(
			TestingLeaderElectionService dispatcherLeaderElectionService,
			DispatcherRunner dispatcherRunner) throws InterruptedException, java.util.concurrent.ExecutionException {
		final UUID leaderSessionId = UUID.randomUUID();
		dispatcherLeaderElectionService.isLeader(leaderSessionId).get();

		return dispatcherRunner.getDispatcherGateway().get();
	}

	private static class TestingDispatcherFactory implements DispatcherFactory {
		private final JobManagerRunnerFactory jobManagerRunnerFactory;

		private TestingDispatcherFactory(JobManagerRunnerFactory jobManagerRunnerFactory) {
			this.jobManagerRunnerFactory = jobManagerRunnerFactory;
		}

		@Override
		public Dispatcher createDispatcher(@Nonnull RpcService rpcService, @Nonnull PartialDispatcherServices partialDispatcherServices) throws Exception {
			return new StandaloneDispatcher(rpcService, getEndpointId(), DispatcherServices.from(partialDispatcherServices, jobManagerRunnerFactory));
		}
	}
}

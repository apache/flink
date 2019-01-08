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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests the HA behaviour of the {@link Dispatcher}.
 */
public class DispatcherHATest extends TestLogger {

	private static final DispatcherId NULL_FENCING_TOKEN = DispatcherId.fromUuid(new UUID(0L, 0L));

	private static final Time timeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
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
	public static void teardownClass() throws ExecutionException, InterruptedException {
		if (rpcService != null) {
			rpcService.stopService().get();
			rpcService = null;
		}
	}

	/**
	 * Tests that interleaved granting and revoking of the leadership won't interfere
	 * with the job recovery and the resulting internal state of the Dispatcher.
	 */
	@Test
	public void testGrantingRevokingLeadership() throws Exception {

		final Configuration configuration = new Configuration();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		final JobGraph nonEmptyJobGraph = createNonEmptyJobGraph();
		final SubmittedJobGraph submittedJobGraph = new SubmittedJobGraph(nonEmptyJobGraph, null);

		final OneShotLatch enterGetJobIdsLatch = new OneShotLatch();
		final OneShotLatch proceedGetJobIdsLatch = new OneShotLatch();
		highAvailabilityServices.setSubmittedJobGraphStore(new BlockingSubmittedJobGraphStore(submittedJobGraph, enterGetJobIdsLatch, proceedGetJobIdsLatch));
		final TestingLeaderElectionService dispatcherLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);

		final BlockingQueue<DispatcherId> fencingTokens = new ArrayBlockingQueue<>(2);

		final HATestingDispatcher dispatcher = new HATestingDispatcher(
			rpcService,
			UUID.randomUUID().toString(),
			configuration,
			highAvailabilityServices,
			new TestingResourceManagerGateway(),
			new BlobServer(configuration, new VoidBlobStore()),
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			new TestingJobManagerRunnerFactory(new CompletableFuture<>(), new CompletableFuture<>()),
			testingFatalErrorHandler,
			fencingTokens);

		dispatcher.start();

		try {
			final UUID leaderId = UUID.randomUUID();
			dispatcherLeaderElectionService.isLeader(leaderId);

			dispatcherLeaderElectionService.notLeader();

			final DispatcherId firstFencingToken = fencingTokens.take();

			assertThat(firstFencingToken, equalTo(NULL_FENCING_TOKEN));

			enterGetJobIdsLatch.await();
			proceedGetJobIdsLatch.trigger();

			assertThat(dispatcher.getNumberJobs(timeout).get(), is(0));

		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, timeout);
		}
	}

	@Nonnull
	private JobGraph createNonEmptyJobGraph() {
		final JobVertex noOpVertex = new JobVertex("NoOp vertex");
		return new JobGraph(noOpVertex);
	}

	private static class HATestingDispatcher extends TestingDispatcher {

		@Nonnull
		private final BlockingQueue<DispatcherId> fencingTokens;

		HATestingDispatcher(RpcService rpcService, String endpointId, Configuration configuration, HighAvailabilityServices highAvailabilityServices, ResourceManagerGateway resourceManagerGateway, BlobServer blobServer, HeartbeatServices heartbeatServices, JobManagerMetricGroup jobManagerMetricGroup, @Nullable String metricQueryServicePath, ArchivedExecutionGraphStore archivedExecutionGraphStore, JobManagerRunnerFactory jobManagerRunnerFactory, FatalErrorHandler fatalErrorHandler, @Nonnull BlockingQueue<DispatcherId> fencingTokens) throws Exception {
			super(rpcService, endpointId, configuration, highAvailabilityServices, resourceManagerGateway, blobServer, heartbeatServices, jobManagerMetricGroup, metricQueryServicePath, archivedExecutionGraphStore, jobManagerRunnerFactory, fatalErrorHandler);
			this.fencingTokens = fencingTokens;
		}

		@VisibleForTesting
		CompletableFuture<Integer> getNumberJobs(Time timeout) {
			return callAsyncWithoutFencing(
				() -> listJobs(timeout).get().size(),
				timeout);
		}

		@Override
		protected void setFencingToken(@Nullable DispatcherId newFencingToken) {
			super.setFencingToken(newFencingToken);

			if (newFencingToken == null) {
				fencingTokens.offer(NULL_FENCING_TOKEN);
			} else {
				fencingTokens.offer(newFencingToken);
			}
		}
	}

	private static class BlockingSubmittedJobGraphStore implements SubmittedJobGraphStore {

		@Nonnull
		private final SubmittedJobGraph submittedJobGraph;

		@Nonnull
		private final OneShotLatch enterGetJobIdsLatch;

		@Nonnull
		private final OneShotLatch proceedGetJobIdsLatch;

		private boolean isStarted = false;

		private BlockingSubmittedJobGraphStore(@Nonnull SubmittedJobGraph submittedJobGraph, @Nonnull OneShotLatch enterGetJobIdsLatch, @Nonnull OneShotLatch proceedGetJobIdsLatch) {
			this.submittedJobGraph = submittedJobGraph;
			this.enterGetJobIdsLatch = enterGetJobIdsLatch;
			this.proceedGetJobIdsLatch = proceedGetJobIdsLatch;
		}

		@Override
		public void start(SubmittedJobGraphListener jobGraphListener) throws Exception {
			isStarted = true;
		}

		@Override
		public void stop() throws Exception {
			isStarted = false;
		}

		@Nullable
		@Override
		public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
			Preconditions.checkArgument(jobId.equals(submittedJobGraph.getJobId()));

			return submittedJobGraph;
		}

		@Override
		public void putJobGraph(SubmittedJobGraph jobGraph) throws Exception {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public void removeJobGraph(JobID jobId) throws Exception {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public Collection<JobID> getJobIds() throws Exception {
			enterGetJobIdsLatch.trigger();
			proceedGetJobIdsLatch.await();
			return Collections.singleton(submittedJobGraph.getJobId());
		}
	}
}

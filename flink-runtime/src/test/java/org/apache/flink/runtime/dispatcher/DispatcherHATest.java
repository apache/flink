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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

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
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		final JobGraph nonEmptyJobGraph = createNonEmptyJobGraph();
		final SubmittedJobGraph submittedJobGraph = new SubmittedJobGraph(nonEmptyJobGraph);

		final OneShotLatch enterGetJobIdsLatch = new OneShotLatch();
		final OneShotLatch proceedGetJobIdsLatch = new OneShotLatch();
		highAvailabilityServices.setSubmittedJobGraphStore(new BlockingSubmittedJobGraphStore(submittedJobGraph, enterGetJobIdsLatch, proceedGetJobIdsLatch));
		final TestingLeaderElectionService dispatcherLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);

		final BlockingQueue<DispatcherId> fencingTokens = new ArrayBlockingQueue<>(2);

		final HATestingDispatcher dispatcher = createDispatcherWithObservableFencingTokens(highAvailabilityServices, fencingTokens);

		dispatcher.start();

		try {
			// wait until the election service has been started
			dispatcherLeaderElectionService.getStartFuture().get();

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

	/**
	 * Tests that all JobManagerRunner are terminated if the leadership of the
	 * Dispatcher is revoked.
	 */
	@Test
	public void testRevokeLeadershipTerminatesJobManagerRunners() throws Exception {

		final TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServicesBuilder()
			.setDispatcherLeaderElectionService(leaderElectionService)
			.build();

		final ArrayBlockingQueue<DispatcherId> fencingTokens = new ArrayBlockingQueue<>(2);
		final HATestingDispatcher dispatcher = createDispatcherWithObservableFencingTokens(
			highAvailabilityServices,
			fencingTokens);

		dispatcher.start();

		try {
			// grant leadership and submit a single job
			final DispatcherId expectedDispatcherId = DispatcherId.generate();

			leaderElectionService.isLeader(expectedDispatcherId.toUUID()).get();

			assertThat(fencingTokens.take(), is(equalTo(expectedDispatcherId)));

			final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(createNonEmptyJobGraph(), timeout);

			submissionFuture.get();

			assertThat(dispatcher.getNumberJobs(timeout).get(), is(1));

			// revoke the leadership --> this should stop all running JobManagerRunners
			leaderElectionService.notLeader();

			assertThat(fencingTokens.take(), is(equalTo(NULL_FENCING_TOKEN)));

			assertThat(dispatcher.getNumberJobs(timeout).get(), is(0));
		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, timeout);
		}
	}

	/**
	 * Tests that a Dispatcher does not remove the JobGraph from the submitted job graph store
	 * when losing leadership and recovers it when regaining leadership.
	 */
	@Test
	public void testJobRecoveryWhenChangingLeadership() throws Exception {
		final InMemorySubmittedJobGraphStore submittedJobGraphStore = new InMemorySubmittedJobGraphStore();

		final CompletableFuture<JobID> recoveredJobFuture = new CompletableFuture<>();
		submittedJobGraphStore.setRecoverJobGraphFunction((jobID, jobIDSubmittedJobGraphMap) -> {
			recoveredJobFuture.complete(jobID);
			return jobIDSubmittedJobGraphMap.get(jobID);
		});

		final TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();

		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServicesBuilder()
			.setSubmittedJobGraphStore(submittedJobGraphStore)
			.setDispatcherLeaderElectionService(leaderElectionService)
			.build();

		final ArrayBlockingQueue<DispatcherId> fencingTokens = new ArrayBlockingQueue<>(2);
		final HATestingDispatcher dispatcher = createDispatcherWithObservableFencingTokens(
			highAvailabilityServices,
			fencingTokens);

		dispatcher.start();

		try {
			// grant leadership and submit a single job
			final DispatcherId expectedDispatcherId = DispatcherId.generate();
			leaderElectionService.isLeader(expectedDispatcherId.toUUID()).get();

			assertThat(fencingTokens.take(), is(equalTo(expectedDispatcherId)));

			final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

			final JobGraph jobGraph = createNonEmptyJobGraph();
			final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

			submissionFuture.get();

			final JobID jobId = jobGraph.getJobID();
			assertThat(submittedJobGraphStore.contains(jobId), is(true));

			// revoke the leadership --> this should stop all running JobManagerRunners
			leaderElectionService.notLeader();

			assertThat(fencingTokens.take(), is(equalTo(NULL_FENCING_TOKEN)));

			assertThat(submittedJobGraphStore.contains(jobId), is(true));

			assertThat(recoveredJobFuture.isDone(), is(false));

			// re-grant leadership
			leaderElectionService.isLeader(DispatcherId.generate().toUUID());

			assertThat(recoveredJobFuture.get(), is(equalTo(jobId)));
		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, timeout);
		}
	}

	/**
	 * Tests that a fatal error is reported if the job recovery fails.
	 */
	@Test
	public void testFailingRecoveryIsAFatalError() throws Exception {
		final String exceptionMessage = "Job recovery test failure.";
		final Supplier<Exception> exceptionSupplier = () -> new FlinkException(exceptionMessage);
		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServicesBuilder()
			.setSubmittedJobGraphStore(new FailingSubmittedJobGraphStore(exceptionSupplier))
			.build();

		final HATestingDispatcher dispatcher = createDispatcher(haServices);
		dispatcher.start();

		final Throwable failure = testingFatalErrorHandler.getErrorFuture().get();

		assertThat(ExceptionUtils.findThrowableWithMessage(failure, exceptionMessage).isPresent(), is(true));

		testingFatalErrorHandler.clearError();
	}

	@Nonnull
	private HATestingDispatcher createDispatcherWithObservableFencingTokens(HighAvailabilityServices highAvailabilityServices, Queue<DispatcherId> fencingTokens) throws Exception {
		return createDispatcher(highAvailabilityServices, fencingTokens, createTestingJobManagerRunnerFactory());
	}

	@Nonnull
	private TestingJobManagerRunnerFactory createTestingJobManagerRunnerFactory() {
		return new TestingJobManagerRunnerFactory(new CompletableFuture<>(), new CompletableFuture<>(), CompletableFuture.completedFuture(null));
	}

	private HATestingDispatcher createDispatcher(HighAvailabilityServices haServices) throws Exception {
		return createDispatcher(
			haServices,
			new ArrayDeque<>(1),
			createTestingJobManagerRunnerFactory());
	}

	@Nonnull
	private HATestingDispatcher createDispatcher(
		HighAvailabilityServices highAvailabilityServices,
		@Nonnull Queue<DispatcherId> fencingTokens,
		JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
		final Configuration configuration = new Configuration();

		TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		return new HATestingDispatcher(
			rpcService,
			UUID.randomUUID().toString(),
			configuration,
			highAvailabilityServices,
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			new BlobServer(configuration, new VoidBlobStore()),
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			jobManagerRunnerFactory,
			testingFatalErrorHandler,
			fencingTokens);
	}

	@Nonnull
	static JobGraph createNonEmptyJobGraph() {
		final JobVertex noOpVertex = new JobVertex("NoOp vertex");
		noOpVertex.setInvokableClass(NoOpInvokable.class);
		final JobGraph jobGraph = new JobGraph(noOpVertex);
		jobGraph.setAllowQueuedScheduling(true);

		return jobGraph;
	}

	private static class HATestingDispatcher extends TestingDispatcher {

		@Nonnull
		private final Queue<DispatcherId> fencingTokens;

		HATestingDispatcher(
				RpcService rpcService,
				String endpointId,
				Configuration configuration,
				HighAvailabilityServices highAvailabilityServices,
				GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				JobManagerMetricGroup jobManagerMetricGroup,
				@Nullable String metricQueryServiceAddress,
				ArchivedExecutionGraphStore archivedExecutionGraphStore,
				JobManagerRunnerFactory jobManagerRunnerFactory,
				FatalErrorHandler fatalErrorHandler,
				@Nonnull Queue<DispatcherId> fencingTokens) throws Exception {
			super(
				rpcService,
				endpointId,
				configuration,
				highAvailabilityServices,
				resourceManagerGatewayRetriever,
				blobServer,
				heartbeatServices,
				jobManagerMetricGroup,
				metricQueryServiceAddress,
				archivedExecutionGraphStore,
				jobManagerRunnerFactory,
				fatalErrorHandler);
			this.fencingTokens = fencingTokens;
		}

		@Override
		protected void setFencingToken(@Nullable DispatcherId newFencingToken) {
			super.setFencingToken(newFencingToken);

			final DispatcherId fencingToken;
			if (newFencingToken == null) {
				fencingToken = NULL_FENCING_TOKEN;
			} else {
				fencingToken = newFencingToken;
			}

			fencingTokens.offer(fencingToken);
		}
	}

	private static class BlockingSubmittedJobGraphStore implements SubmittedJobGraphStore {

		@Nonnull
		private final SubmittedJobGraph submittedJobGraph;

		@Nonnull
		private final OneShotLatch enterGetJobIdsLatch;

		@Nonnull
		private final OneShotLatch proceedGetJobIdsLatch;

		private BlockingSubmittedJobGraphStore(@Nonnull SubmittedJobGraph submittedJobGraph, @Nonnull OneShotLatch enterGetJobIdsLatch, @Nonnull OneShotLatch proceedGetJobIdsLatch) {
			this.submittedJobGraph = submittedJobGraph;
			this.enterGetJobIdsLatch = enterGetJobIdsLatch;
			this.proceedGetJobIdsLatch = proceedGetJobIdsLatch;
		}

		@Override
		public void start(SubmittedJobGraphListener jobGraphListener) {
		}

		@Override
		public void stop() {
		}

		@Nullable
		@Override
		public SubmittedJobGraph recoverJobGraph(JobID jobId) {
			Preconditions.checkArgument(jobId.equals(submittedJobGraph.getJobId()));

			return submittedJobGraph;
		}

		@Override
		public void putJobGraph(SubmittedJobGraph jobGraph) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public void removeJobGraph(JobID jobId) {
			throw new UnsupportedOperationException("Should not be called.");
		}

		@Override
		public void releaseJobGraph(JobID jobId) {}

		@Override
		public Collection<JobID> getJobIds() throws Exception {
			enterGetJobIdsLatch.trigger();
			proceedGetJobIdsLatch.await();
			return Collections.singleton(submittedJobGraph.getJobId());
		}
	}

	private static class FailingSubmittedJobGraphStore implements SubmittedJobGraphStore {
		private final JobID jobId = new JobID();

		private final Supplier<Exception> exceptionSupplier;

		private FailingSubmittedJobGraphStore(Supplier<Exception> exceptionSupplier) {
			this.exceptionSupplier = exceptionSupplier;
		}

		@Override
		public void start(SubmittedJobGraphListener jobGraphListener) {

		}

		@Override
		public void stop() {

		}

		@Nullable
		@Override
		public SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
			throw exceptionSupplier.get();
		}

		@Override
		public void putJobGraph(SubmittedJobGraph jobGraph) {

		}

		@Override
		public void removeJobGraph(JobID jobId) {

		}

		@Override
		public void releaseJobGraph(JobID jobId) {

		}

		@Override
		public Collection<JobID> getJobIds() {
			return Collections.singleton(jobId);
		}
	}
}

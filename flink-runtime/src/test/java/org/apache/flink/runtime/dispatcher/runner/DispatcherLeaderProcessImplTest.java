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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.BiFunctionWithException;

import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link DispatcherLeaderProcessImpl}.
 */
public class DispatcherLeaderProcessImplTest extends TestLogger {

	private static final JobGraph JOB_GRAPH = new JobGraph("JobGraph");

	private static ExecutorService ioExecutor;

	private final UUID leaderSessionId = UUID.randomUUID();

	private TestingFatalErrorHandler fatalErrorHandler;

	private JobGraphStore jobGraphStore;

	private TestingDispatcherServiceFactory dispatcherServiceFactory;

	@BeforeClass
	public static void setupClass() {
		ioExecutor = Executors.newSingleThreadExecutor();
	}

	@Before
	public void setup() {
		fatalErrorHandler = new TestingFatalErrorHandler();
		jobGraphStore = TestingJobGraphStore.newBuilder().build();
		dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder().build();
	}

	@After
	public void teardown() throws Exception {
		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
			fatalErrorHandler = null;
		}
	}

	@AfterClass
	public static void teardownClass() {
		if (ioExecutor != null) {
			ExecutorUtils.gracefulShutdown(5L, TimeUnit.SECONDS, ioExecutor);
		}
	}

	@Test
	public void start_afterClose_doesNotHaveAnEffect() throws Exception {
		final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess();

		dispatcherLeaderProcess.close();
		dispatcherLeaderProcess.start();

		assertThat(dispatcherLeaderProcess.getState(), is(DispatcherLeaderProcessImpl.State.STOPPED));
	}

	@Test
	public void start_triggersJobGraphRecoveryAndDispatcherServiceCreation() throws Exception {
		jobGraphStore = TestingJobGraphStore.newBuilder()
			.setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
			.build();

		final CompletableFuture<Collection<JobGraph>> recoveredJobGraphsFuture = new CompletableFuture<>();
		dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder()
			.setCreateFunction(
				(recoveredJobGraphs, jobGraphStore) -> {
					recoveredJobGraphsFuture.complete(recoveredJobGraphs);
					return TestingDispatcherService.newBuilder().build();
				}
			)
			.build();

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();
			assertThat(dispatcherLeaderProcess.getState(), is(DispatcherLeaderProcessImpl.State.RUNNING));

			final Collection<JobGraph> recoveredJobGraphs = recoveredJobGraphsFuture.get();

			assertThat(recoveredJobGraphs, hasSize(1));
			assertThat(recoveredJobGraphs, containsInAnyOrder(JOB_GRAPH));
		}
	}

	@Test
	public void closeAsync_stopsJobGraphStoreAndDispatcher() throws Exception {
		final CompletableFuture<Void> jobGraphStopFuture = new CompletableFuture<>();
		jobGraphStore = TestingJobGraphStore.newBuilder()
			.setStopRunnable(() -> jobGraphStopFuture.complete(null))
			.build();

		final CompletableFuture<Void> dispatcherServiceTerminationFuture = new CompletableFuture<>();
		final OneShotLatch dispatcherServiceShutdownLatch = new OneShotLatch();
		dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder()
			.setCreateFunction((ignoredA, ignoredB) -> TestingDispatcherService.newBuilder()
				.setTerminationFutureSupplier(() -> {
					dispatcherServiceShutdownLatch.trigger();
					return dispatcherServiceTerminationFuture;
				})
				.build())
			.build();

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();

			// wait for the creation of the DispatcherService
			dispatcherLeaderProcess.getDispatcherGateway().get();

			final CompletableFuture<Void> terminationFuture = dispatcherLeaderProcess.closeAsync();

			assertThat(jobGraphStopFuture.isDone(), is(false));
			assertThat(terminationFuture.isDone(), is(false));

			dispatcherServiceShutdownLatch.await();
			dispatcherServiceTerminationFuture.complete(null);

			// verify that we shut down the JobGraphStore
			jobGraphStopFuture.get();

			// verify that we completed the dispatcher leader process shut down
			terminationFuture.get();
		}
	}

	@Test
	public void confirmLeaderSessionFuture_completesAfterDispatcherServiceHasBeenStarted() throws Exception {
		final OneShotLatch createDispatcherServiceLatch = new OneShotLatch();
		final String dispatcherAddress = "myAddress";
		final TestingDispatcherGateway dispatcherGateway = new TestingDispatcherGateway.Builder()
			.setAddress(dispatcherAddress)
			.build();

		dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder()
			.setCreateFunction(
				BiFunctionWithException.unchecked((ignoredA, ignoredB) -> {
					createDispatcherServiceLatch.await();
					return TestingDispatcherService.newBuilder()
						.setDispatcherGateway(dispatcherGateway)
						.build();
				}))
			.build();

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			final CompletableFuture<String> confirmLeaderSessionFuture = dispatcherLeaderProcess.getConfirmLeaderSessionFuture();

			dispatcherLeaderProcess.start();

			assertThat(confirmLeaderSessionFuture.isDone(), is(false));

			createDispatcherServiceLatch.trigger();

			assertThat(confirmLeaderSessionFuture.get(), is(dispatcherAddress));
		}
	}

	@Test
	public void closeAsync_duringJobRecovery_preventsDispatcherServiceCreation() throws Exception {
		final OneShotLatch jobRecoveryStarted = new OneShotLatch();
		final OneShotLatch completeJobRecovery = new OneShotLatch();
		final OneShotLatch createDispatcherService = new OneShotLatch();

		this.jobGraphStore = TestingJobGraphStore.newBuilder()
			.setJobIdsFunction(storedJobs -> {
				jobRecoveryStarted.trigger();
				completeJobRecovery.await();
				return storedJobs;
			})
			.build();

		this.dispatcherServiceFactory = TestingDispatcherServiceFactory.newBuilder()
			.setCreateFunction(
				(ignoredA, ignoredB) -> {
					createDispatcherService.trigger();
					return TestingDispatcherService.newBuilder().build();
				})
			.build();

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();

			jobRecoveryStarted.await();

			dispatcherLeaderProcess.closeAsync();

			completeJobRecovery.trigger();

			try {
				createDispatcherService.await(10L, TimeUnit.MILLISECONDS);
				fail("No dispatcher service should be created after the process has been stopped.");
			} catch (TimeoutException expected) {}
		}
	}

	@Test
	public void onRemovedJobGraph_cancelsRunningJob() throws Exception {
		jobGraphStore = TestingJobGraphStore.newBuilder()
			.setInitialJobGraphs(Collections.singleton(JOB_GRAPH))
			.build();

		final CompletableFuture<JobID> cancelJobFuture = new CompletableFuture<>();
		final TestingDispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
			.setCancelJobFunction(
				jobToCancel -> {
					cancelJobFuture.complete(jobToCancel);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
			.build();

		dispatcherServiceFactory = createDispatcherServiceFactoryFor(testingDispatcherGateway);

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();

			// wait for the dispatcher process to be created
			dispatcherLeaderProcess.getDispatcherGateway().get();

			// now remove the Job from the JobGraphStore and notify the dispatcher service
			jobGraphStore.removeJobGraph(JOB_GRAPH.getJobID());
			dispatcherLeaderProcess.onRemovedJobGraph(JOB_GRAPH.getJobID());

			assertThat(cancelJobFuture.get(), is(JOB_GRAPH.getJobID()));
		}
	}

	@Test
	public void onAddedJobGraph_submitsRecoveredJob() throws Exception {
		final CompletableFuture<JobGraph> submittedJobFuture = new CompletableFuture<>();
		final TestingDispatcherGateway testingDispatcherGateway = new TestingDispatcherGateway.Builder()
			.setSubmitFunction(
				submittedJob -> {
					submittedJobFuture.complete(submittedJob);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
			.build();

		dispatcherServiceFactory = createDispatcherServiceFactoryFor(testingDispatcherGateway);

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();

			// wait first for the dispatcher service to be created
			dispatcherLeaderProcess.getDispatcherGateway().get();

			jobGraphStore.putJobGraph(JOB_GRAPH);
			dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

			final JobGraph submittedJobGraph = submittedJobFuture.get();

			assertThat(submittedJobGraph.getJobID(), is(JOB_GRAPH.getJobID()));
		}
	}

	@Test
	public void onAddedJobGraph_failingRecovery_propagatesTheFailure() throws Exception {
		final FlinkException expectedFailure = new FlinkException("Expected failure");
		jobGraphStore = TestingJobGraphStore.newBuilder()
			.setRecoverJobGraphFunction(
				(ignoredA, ignoredB) -> {
					throw expectedFailure;
				})
			.build();

		try (final DispatcherLeaderProcessImpl dispatcherLeaderProcess = createDispatcherLeaderProcess()) {
			dispatcherLeaderProcess.start();

			// wait first for the dispatcher service to be created
			dispatcherLeaderProcess.getDispatcherGateway().get();

			jobGraphStore.putJobGraph(JOB_GRAPH);
			dispatcherLeaderProcess.onAddedJobGraph(JOB_GRAPH.getJobID());

			final CompletableFuture<Throwable> errorFuture = fatalErrorHandler.getErrorFuture();
			final Throwable throwable = errorFuture.get();
			Assert.assertThat(ExceptionUtils.findThrowable(throwable, expectedFailure::equals).isPresent(), Is.is(true));

			assertThat(dispatcherLeaderProcess.getState(), is(DispatcherLeaderProcessImpl.State.STOPPED));

			fatalErrorHandler.clearError();
		}
	}

	@Test
	@Ignore
	public void onAddedJobGraph_falsePositive_willBeIgnored() {
		fail("Needs to be implemented once the proper deduplication mechanism is in place.");
	}

	private TestingDispatcherServiceFactory createDispatcherServiceFactoryFor(TestingDispatcherGateway testingDispatcherGateway) {
		return TestingDispatcherServiceFactory.newBuilder()
			.setCreateFunction(
				(ignoredA, ignoredB) -> TestingDispatcherService.newBuilder()
					.setDispatcherGateway(testingDispatcherGateway)
					.build())
			.build();
	}

	private DispatcherLeaderProcessImpl createDispatcherLeaderProcess() {
		return DispatcherLeaderProcessImpl.create(
			leaderSessionId,
			dispatcherServiceFactory,
			jobGraphStore,
			ioExecutor,
			fatalErrorHandler);
	}
}

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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.runtime.util.TestingUserCodeClassLoader;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link JobManagerRunnerImpl}.
 */
public class JobManagerRunnerImplTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static JobGraph jobGraph;

	private static ArchivedExecutionGraph archivedExecutionGraph;

	private static JobMasterServiceFactory defaultJobMasterServiceFactory;

	private TestingHighAvailabilityServices haServices;

	private TestingLeaderElectionService leaderElectionService;

	private TestingFatalErrorHandler fatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		defaultJobMasterServiceFactory = new TestingJobMasterServiceFactory();

		final JobVertex jobVertex = new JobVertex("Test vertex");
		jobVertex.setInvokableClass(NoOpInvokable.class);
		jobGraph = new JobGraph(jobVertex);

		archivedExecutionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobGraph.getJobID())
			.setState(JobStatus.FINISHED)
			.build();
	}

	@Before
	public void setup() {
		leaderElectionService = new TestingLeaderElectionService();
		haServices = new TestingHighAvailabilityServices();
		haServices.setJobMasterLeaderElectionService(jobGraph.getJobID(), leaderElectionService);
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		fatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void tearDown() throws Exception {
		fatalErrorHandler.rethrowError();
	}

	@Test
	public void testJobCompletion() throws Exception {
		final JobManagerRunnerImpl jobManagerRunner = createJobManagerRunner();

		try {
			jobManagerRunner.start();

			final CompletableFuture<ArchivedExecutionGraph> resultFuture = jobManagerRunner.getResultFuture();

			assertThat(resultFuture.isDone(), is(false));

			jobManagerRunner.jobReachedGloballyTerminalState(archivedExecutionGraph);

			assertThat(resultFuture.get(), is(archivedExecutionGraph));
		} finally {
			jobManagerRunner.close();
		}
	}

	@Test
	public void testJobFinishedByOther() throws Exception {
		final JobManagerRunnerImpl jobManagerRunner = createJobManagerRunner();

		try {
			jobManagerRunner.start();

			final CompletableFuture<ArchivedExecutionGraph> resultFuture = jobManagerRunner.getResultFuture();

			assertThat(resultFuture.isDone(), is(false));

			jobManagerRunner.jobFinishedByOther();

			try {
				resultFuture.get();
				fail("Should have failed.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(JobNotFinishedException.class));
			}
		} finally {
			jobManagerRunner.close();
		}
	}

	@Test
	public void testShutDown() throws Exception {
		final JobManagerRunner jobManagerRunner = createJobManagerRunner();

		try {
			jobManagerRunner.start();

			final CompletableFuture<ArchivedExecutionGraph> resultFuture = jobManagerRunner.getResultFuture();

			assertThat(resultFuture.isDone(), is(false));

			jobManagerRunner.closeAsync();

			try {
				resultFuture.get();
				fail("Should have failed.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.stripExecutionException(ee), instanceOf(JobNotFinishedException.class));
			}
		} finally {
			jobManagerRunner.close();
		}
	}

	@Test
	public void testLibraryCacheManagerRegistration() throws Exception {
		final OneShotLatch registerClassLoaderLatch = new OneShotLatch();
		final OneShotLatch closeClassLoaderLeaseLatch = new OneShotLatch();
		final TestingUserCodeClassLoader userCodeClassLoader = TestingUserCodeClassLoader.newBuilder().build();
		final TestingClassLoaderLease classLoaderLease = TestingClassLoaderLease.newBuilder()
			.setGetOrResolveClassLoaderFunction((permanentBlobKeys, urls) -> {
				registerClassLoaderLatch.trigger();
				return userCodeClassLoader;
			})
			.setCloseRunnable(closeClassLoaderLeaseLatch::trigger)
			.build();
		final JobManagerRunner jobManagerRunner = createJobManagerRunner(classLoaderLease);

		try {
			jobManagerRunner.start();

			registerClassLoaderLatch.await();

			jobManagerRunner.close();

			closeClassLoaderLeaseLatch.await();
		} finally {
			jobManagerRunner.close();
		}
	}

	/**
	 * Tests that the {@link JobManagerRunnerImpl} always waits for the previous leadership operation
	 * (granting or revoking leadership) to finish before starting a new leadership operation.
	 */
	@Test
	public void testConcurrentLeadershipOperationsBlockingSuspend() throws Exception {
		final CompletableFuture<Acknowledge> suspendedFuture = new CompletableFuture<>();

		TestingJobMasterServiceFactory jobMasterServiceFactory = new TestingJobMasterServiceFactory(
			() -> new TestingJobMasterService(
				"localhost",
				e -> suspendedFuture));
		JobManagerRunner jobManagerRunner = createJobManagerRunner(jobMasterServiceFactory);

		jobManagerRunner.start();

		leaderElectionService.isLeader(UUID.randomUUID()).get();

		leaderElectionService.notLeader();

		final CompletableFuture<UUID> leaderFuture = leaderElectionService.isLeader(UUID.randomUUID());

		// the new leadership should wait first for the suspension to happen
		assertThat(leaderFuture.isDone(), is(false));

		try {
			leaderFuture.get(1L, TimeUnit.MILLISECONDS);
			fail("Granted leadership even though the JobMaster has not been suspended.");
		} catch (TimeoutException expected) {
			// expected
		}

		suspendedFuture.complete(Acknowledge.get());

		leaderFuture.get();
	}

	/**
	 * Tests that the {@link JobManagerRunnerImpl} always waits for the previous leadership operation
	 * (granting or revoking leadership) to finish before starting a new leadership operation.
	 */
	@Test
	public void testConcurrentLeadershipOperationsBlockingGainLeadership() throws Exception {
		final CompletableFuture<Exception> suspendFuture = new CompletableFuture<>();
		final CompletableFuture<Acknowledge> startFuture = new CompletableFuture<>();

		TestingJobMasterServiceFactory jobMasterServiceFactory = new TestingJobMasterServiceFactory(
			() -> new TestingJobMasterService(
				"localhost",
				e -> {
					suspendFuture.complete(e);
					return CompletableFuture.completedFuture(Acknowledge.get());
				},
				ignored -> startFuture));
		JobManagerRunner jobManagerRunner = createJobManagerRunner(jobMasterServiceFactory);

		jobManagerRunner.start();

		leaderElectionService.isLeader(UUID.randomUUID());

		leaderElectionService.notLeader();

		// suspending should wait for the start to happen first
		assertThat(suspendFuture.isDone(), is(false));

		try {
			suspendFuture.get(1L, TimeUnit.MILLISECONDS);
			fail("Suspended leadership even though the JobMaster has not been started.");
		} catch (TimeoutException expected) {
			// expected
		}

		startFuture.complete(Acknowledge.get());

		suspendFuture.get();
	}

	@Nonnull
	private JobManagerRunner createJobManagerRunner(LibraryCacheManager.ClassLoaderLease classLoaderLease) throws Exception {
		return createJobManagerRunner(defaultJobMasterServiceFactory, classLoaderLease);
	}

	@Nonnull
	private JobManagerRunnerImpl createJobManagerRunner() throws Exception {
		return createJobManagerRunner(defaultJobMasterServiceFactory, TestingClassLoaderLease.newBuilder().build());
	}

	@Nonnull
	private JobManagerRunner createJobManagerRunner(JobMasterServiceFactory jobMasterServiceFactory) throws Exception {
		return createJobManagerRunner(jobMasterServiceFactory, TestingClassLoaderLease.newBuilder().build());
	}

	@Nonnull
	private JobManagerRunnerImpl createJobManagerRunner(JobMasterServiceFactory jobMasterServiceFactory, LibraryCacheManager.ClassLoaderLease classLoaderLease) throws Exception{
		return new JobManagerRunnerImpl(
			jobGraph,
			jobMasterServiceFactory,
			haServices,
			classLoaderLease,
			TestingUtils.defaultExecutor(),
			fatalErrorHandler,
			System.currentTimeMillis());
	}
}

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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.FailingPermanentBlobService;
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link JobManagerRunner}.
 */
public class JobManagerRunnerTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static JobGraph jobGraph;

	private static ArchivedExecutionGraph archivedExecutionGraph;

	private static LibraryCacheManager libraryCacheManager;

	private static JobMasterServiceFactory jobMasterFactory;

	private TestingHighAvailabilityServices haServices;

	private TestingFatalErrorHandler fatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		libraryCacheManager = new BlobLibraryCacheManager(
			FailingPermanentBlobService.INSTANCE,
			FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
			new String[]{});

		jobMasterFactory = TestingJobMasterFactory.INSTANCE;

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
		haServices = new TestingHighAvailabilityServices();
		haServices.setJobMasterLeaderElectionService(jobGraph.getJobID(), new TestingLeaderElectionService());
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		fatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void tearDown() throws Exception {
		fatalErrorHandler.rethrowError();
	}

	@AfterClass
	public static void tearDownClass() {
		if (libraryCacheManager != null) {
			libraryCacheManager.shutdown();
		}
	}

	@Test
	public void testJobCompletion() throws Exception {
		final JobManagerRunner jobManagerRunner = createJobManagerRunner();

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
		final JobManagerRunner jobManagerRunner = createJobManagerRunner();

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
		final BlobLibraryCacheManager libraryCacheManager = new BlobLibraryCacheManager(
			VoidPermanentBlobService.INSTANCE,
			FlinkUserCodeClassLoaders.ResolveOrder.CHILD_FIRST,
			new String[]{});
		final JobManagerRunner jobManagerRunner = createJobManagerRunner(libraryCacheManager);

		try {
			jobManagerRunner.start();

			final JobID jobID = jobGraph.getJobID();
			assertThat(libraryCacheManager.hasClassLoader(jobID), is(true));

			jobManagerRunner.close();

			assertThat(libraryCacheManager.hasClassLoader(jobID), is(false));
		} finally {
			jobManagerRunner.close();
		}
	}

	@Nonnull
	private JobManagerRunner createJobManagerRunner(LibraryCacheManager libraryCacheManager) throws Exception {
		return new JobManagerRunner(
			jobGraph,
			jobMasterFactory,
			haServices,
			libraryCacheManager,
			TestingUtils.defaultExecutor(),
			fatalErrorHandler);
	}

	@Nonnull
	private JobManagerRunner createJobManagerRunner() throws Exception {
		return createJobManagerRunner(libraryCacheManager);
	}
}

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

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.TestingRpcService;
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
 * Tests for the {@link JobManagerRunner}
 */
public class JobManagerRunnerTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static Configuration configuration;

	private static TestingRpcService rpcService;

	private static BlobServer blobServer;

	private static HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

	private static JobManagerSharedServices jobManagerSharedServices;

	private static JobGraph jobGraph;

	private static ArchivedExecutionGraph archivedExecutionGraph;

	private TestingHighAvailabilityServices haServices;

	private TestingFatalErrorHandler fatalErrorHandler;

	@BeforeClass
	public static void setupClass() throws Exception {
		configuration = new Configuration();
		rpcService = new TestingRpcService();

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(
			configuration,
			new VoidBlobStore());

		jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(configuration, blobServer);

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
	public static void tearDownClass() throws Exception {
		if (jobManagerSharedServices != null) {
			jobManagerSharedServices.shutdown();
		}

		if (blobServer != null) {
			blobServer.close();
		}

		if (rpcService != null) {
			rpcService.stopService();
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

	@Nonnull
	private JobManagerRunner createJobManagerRunner() throws Exception {
		return new JobManagerRunner(
			ResourceID.generate(),
			jobGraph,
			configuration,
			rpcService,
			haServices,
			heartbeatServices,
			blobServer,
			jobManagerSharedServices,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			fatalErrorHandler);
	}
}

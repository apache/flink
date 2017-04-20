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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobManagerRunner.class)
public class JobManagerRunnerMockTest {

	private JobManagerRunner runner;

	private JobMaster jobManager;

	private JobMasterGateway jobManagerGateway;

	private LeaderElectionService leaderElectionService;

	private SubmittedJobGraphStore submittedJobGraphStore;

	private TestingOnCompletionActions jobCompletion;

	private BlobStore blobStore;

	private RunningJobsRegistry runningJobsRegistry;

	@Before
	public void setUp() throws Exception {
		RpcService mockRpc = mock(RpcService.class);
		when(mockRpc.getAddress()).thenReturn("localhost");

		jobManager = mock(JobMaster.class);
		jobManagerGateway = mock(JobMasterGateway.class);
		when(jobManager.getSelf()).thenReturn(jobManagerGateway);
		when(jobManager.getRpcService()).thenReturn(mockRpc);

		PowerMockito.whenNew(JobMaster.class).withAnyArguments().thenReturn(jobManager);

		jobCompletion = new TestingOnCompletionActions();

		leaderElectionService = mock(LeaderElectionService.class);
		when(leaderElectionService.hasLeadership()).thenReturn(true);

		SubmittedJobGraphStore submittedJobGraphStore = mock(SubmittedJobGraphStore.class);

		blobStore = mock(BlobStore.class);
		
		HighAvailabilityServices haServices = mock(HighAvailabilityServices.class);
		when(haServices.getJobManagerLeaderElectionService(any(JobID.class))).thenReturn(leaderElectionService);
		when(haServices.getSubmittedJobGraphStore()).thenReturn(submittedJobGraphStore);
		when(haServices.createBlobStore()).thenReturn(blobStore);
		when(haServices.getRunningJobsRegistry()).thenReturn(runningJobsRegistry);

		HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);

		runner = PowerMockito.spy(new JobManagerRunner(
			ResourceID.generate(),
			new JobGraph("test", new JobVertex("vertex")),
			mock(Configuration.class),
			mockRpc,
			haServices,
			heartbeatServices,
			JobManagerServices.fromConfiguration(new Configuration(), haServices),
			new MetricRegistry(MetricRegistryConfiguration.defaultMetricRegistryConfiguration()),
			jobCompletion,
			jobCompletion));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Ignore
	@Test
	public void testStartAndShutdown() throws Exception {
		runner.start();
		verify(leaderElectionService).start(runner);

		assertTrue(!jobCompletion.isJobFinished());
		assertTrue(!jobCompletion.isJobFailed());

		verify(jobManager).start(any(UUID.class));
		
		runner.shutdown();
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
	}

	@Ignore
	@Test
	public void testShutdownBeforeGrantLeadership() throws Exception {
		runner.start();
		verify(jobManager).start();
		verify(leaderElectionService).start(runner);

		runner.shutdown();
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();

		assertTrue(!jobCompletion.isJobFinished());
		assertTrue(!jobCompletion.isJobFailed());

		runner.grantLeadership(UUID.randomUUID());
		assertTrue(!jobCompletion.isJobFinished());
		assertTrue(!jobCompletion.isJobFailed());

	}

	@Ignore
	@Test
	public void testJobFinished() throws Exception {
		runner.start();

		UUID leaderSessionID = UUID.randomUUID();
		runner.grantLeadership(leaderSessionID);
		verify(jobManager).start(leaderSessionID);
		assertTrue(!jobCompletion.isJobFinished());

		// runner been told by JobManager that job is finished
		runner.jobFinished(mock(JobExecutionResult.class));

		assertTrue(jobCompletion.isJobFinished());
		assertFalse(jobCompletion.isJobFinishedByOther());
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
		assertTrue(runner.isShutdown());
	}

	@Ignore
	@Test
	public void testJobFailed() throws Exception {
		runner.start();

		UUID leaderSessionID = UUID.randomUUID();
		runner.grantLeadership(leaderSessionID);
		verify(jobManager).start(leaderSessionID);
		assertTrue(!jobCompletion.isJobFinished());

		// runner been told by JobManager that job is failed
		runner.jobFailed(new Exception("failed manually"));

		assertTrue(jobCompletion.isJobFailed());
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
		assertTrue(runner.isShutdown());
	}

	@Ignore
	@Test
	public void testLeadershipRevoked() throws Exception {
		runner.start();

		UUID leaderSessionID = UUID.randomUUID();
		runner.grantLeadership(leaderSessionID);
		verify(jobManager).start(leaderSessionID);
		assertTrue(!jobCompletion.isJobFinished());

		runner.revokeLeadership();
		verify(jobManager).suspendExecution(any(Throwable.class));
		assertFalse(runner.isShutdown());
	}

	@Ignore
	@Test
	public void testRegainLeadership() throws Exception {
		runner.start();

		UUID leaderSessionID = UUID.randomUUID();
		runner.grantLeadership(leaderSessionID);
		verify(jobManager).start(leaderSessionID);
		assertTrue(!jobCompletion.isJobFinished());

		runner.revokeLeadership();
		verify(jobManager).suspendExecution(any(Throwable.class));
		assertFalse(runner.isShutdown());

		UUID leaderSessionID2 = UUID.randomUUID();
		runner.grantLeadership(leaderSessionID2);
		verify(jobManager).start(leaderSessionID2);
	}

	private static class TestingOnCompletionActions implements OnCompletionActions, FatalErrorHandler {

		private volatile JobExecutionResult result;

		private volatile Throwable failedCause;

		private volatile boolean finishedByOther;

		@Override
		public void jobFinished(JobExecutionResult result) {
			checkArgument(!isJobFinished(), "job finished already");
			checkArgument(!isJobFailed(), "job failed already");

			this.result = result;
		}

		@Override
		public void jobFailed(Throwable cause) {
			checkArgument(!isJobFinished(), "job finished already");
			checkArgument(!isJobFailed(), "job failed already");

			this.failedCause = cause;
		}

		@Override
		public void jobFinishedByOther() {
			checkArgument(!isJobFinished(), "job finished already");
			checkArgument(!isJobFailed(), "job failed already");

			this.finishedByOther = true;
		}

		@Override
		public void onFatalError(Throwable exception) {
			jobFailed(exception);
		}

		boolean isJobFinished() {
			return result != null || finishedByOther;
		}

		boolean isJobFinishedByOther() {
			return finishedByOther;
		}

		boolean isJobFailed() {
			return failedCause != null;
		}
	}
}

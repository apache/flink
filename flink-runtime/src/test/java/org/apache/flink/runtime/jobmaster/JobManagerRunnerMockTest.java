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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.RpcService;
import org.junit.After;
import org.junit.Before;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JobManagerRunner.class)
public class JobManagerRunnerMockTest {

	private JobManagerRunner runner;

	private JobMaster jobManager;

	private JobMasterGateway jobManagerGateway;

	private LeaderElectionService leaderElectionService;

	private TestingOnCompletionActions jobCompletion;

	@Before
	public void setUp() throws Exception {
		jobManager = mock(JobMaster.class);
		jobManagerGateway = mock(JobMasterGateway.class);
		when(jobManager.getSelf()).thenReturn(jobManagerGateway);

		PowerMockito.whenNew(JobMaster.class).withAnyArguments().thenReturn(jobManager);

		jobCompletion = new TestingOnCompletionActions();

		leaderElectionService = mock(LeaderElectionService.class);
		when(leaderElectionService.hasLeadership()).thenReturn(true);

		HighAvailabilityServices haServices = mock(HighAvailabilityServices.class);
		when(haServices.getJobMasterLeaderElectionService(any(JobID.class))).thenReturn(leaderElectionService);

		runner = PowerMockito.spy(new JobManagerRunner(
			new JobGraph("test"),
			mock(Configuration.class),
			mock(RpcService.class),
			haServices,
			mock(JobManagerServices.class),
			jobCompletion));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testStartAndShutdown() throws Exception {
		runner.start();
		verify(jobManager).init();
		verify(jobManager).start();
		verify(leaderElectionService).start(runner);

		assertTrue(!jobCompletion.isJobFinished());
		assertTrue(!jobCompletion.isJobFailed());

		runner.shutdown();
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
	}

	@Test
	public void testShutdownBeforeGrantLeadership() throws Exception {
		runner.start();
		verify(jobManager).init();
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

	@Test
	public void testJobFinishedByOtherBeforeGrantLeadership() throws Exception {
		runner.start();

		when(runner.isJobFinishedByOthers()).thenReturn(true);
		runner.grantLeadership(UUID.randomUUID());

		// runner should shutdown automatic and informed the job completion
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();

		assertTrue(jobCompletion.isJobFinished());
		assertTrue(jobCompletion.isJobFinishedByOther());
	}

	@Test
	public void testJobFinished() throws Exception {
		runner.start();

		runner.grantLeadership(UUID.randomUUID());
		verify(jobManagerGateway).startJob();
		assertTrue(!jobCompletion.isJobFinished());

		// runner been told by JobManager that job is finished
		runner.jobFinished(mock(JobExecutionResult.class));

		assertTrue(jobCompletion.isJobFinished());
		assertFalse(jobCompletion.isJobFinishedByOther());
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
		assertTrue(runner.isShutdown());
	}

	@Test
	public void testJobFailed() throws Exception {
		runner.start();

		runner.grantLeadership(UUID.randomUUID());
		verify(jobManagerGateway).startJob();
		assertTrue(!jobCompletion.isJobFinished());

		// runner been told by JobManager that job is failed
		runner.jobFailed(new Exception("failed manually"));

		assertTrue(jobCompletion.isJobFailed());
		verify(leaderElectionService).stop();
		verify(jobManager).shutDown();
		assertTrue(runner.isShutdown());
	}

	@Test
	public void testLeadershipRevoked() throws Exception {
		runner.start();

		runner.grantLeadership(UUID.randomUUID());
		verify(jobManagerGateway).startJob();
		assertTrue(!jobCompletion.isJobFinished());

		runner.revokeLeadership();
		verify(jobManagerGateway).suspendJob(any(Throwable.class));
		assertFalse(runner.isShutdown());
	}

	@Test
	public void testRegainLeadership() throws Exception {
		runner.start();

		runner.grantLeadership(UUID.randomUUID());
		verify(jobManagerGateway).startJob();
		assertTrue(!jobCompletion.isJobFinished());

		runner.revokeLeadership();
		verify(jobManagerGateway).suspendJob(any(Throwable.class));
		assertFalse(runner.isShutdown());

		runner.grantLeadership(UUID.randomUUID());
		verify(jobManagerGateway, times(2)).startJob();
	}

	private static class TestingOnCompletionActions implements OnCompletionActions {

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

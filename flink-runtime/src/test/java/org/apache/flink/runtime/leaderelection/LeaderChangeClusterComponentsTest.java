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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.highavailability.nonha.embedded.TestingEmbeddedHaServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests which verify the cluster behaviour in case of leader changes.
 */
public class LeaderChangeClusterComponentsTest extends TestLogger {

	private static final int SLOTS_PER_TM = 2;
	private static final int NUM_TMS = 2;
	public static final int PARALLELISM = SLOTS_PER_TM * NUM_TMS;

	private static TestingMiniCluster miniCluster;

	private static TestingEmbeddedHaServices highAvailabilityServices;

	private JobGraph jobGraph;

	private JobID jobId;

	@BeforeClass
	public static void setupClass() throws Exception  {
		highAvailabilityServices = new TestingEmbeddedHaServices(TestingUtils.defaultExecutor());

		miniCluster = new TestingMiniCluster(
			new MiniClusterConfiguration.Builder()
				.setNumSlotsPerTaskManager(SLOTS_PER_TM)
				.setNumSlotsPerTaskManager(NUM_TMS)
				.build(),
			() -> highAvailabilityServices);

		miniCluster.start();
	}

	@Before
	public void setup() throws Exception  {
		jobGraph = createJobGraph(PARALLELISM);
		jobId = jobGraph.getJobID();
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (miniCluster != null) {
			miniCluster.close();
		}
	}

	@Test
	public void testReelectionOfDispatcher() throws Exception {
		final CompletableFuture<JobSubmissionResult> submissionFuture = miniCluster.submitJob(jobGraph);

		submissionFuture.get();

		CompletableFuture<JobResult> jobResultFuture = miniCluster.requestJobResult(jobId);

		highAvailabilityServices.revokeDispatcherLeadership().get();

		try {
			jobResultFuture.get();
			fail("Expected JobNotFinishedException");
		} catch (ExecutionException ee) {
			assertThat(ExceptionUtils.findThrowable(ee, JobNotFinishedException.class).isPresent(), is(true));
		}

		highAvailabilityServices.grantDispatcherLeadership();

		BlockingOperator.isBlocking = false;

		final CompletableFuture<JobSubmissionResult> submissionFuture2 = miniCluster.submitJob(jobGraph);

		submissionFuture2.get();

		final CompletableFuture<JobResult> jobResultFuture2 = miniCluster.requestJobResult(jobId);

		JobResult jobResult = jobResultFuture2.get();

		assertThat(jobResult.isSuccess(), is(true));
	}

	@Test
	public void testReelectionOfJobMaster() throws Exception {
		final CompletableFuture<JobSubmissionResult> submissionFuture = miniCluster.submitJob(jobGraph);

		submissionFuture.get();

		CompletableFuture<JobResult> jobResultFuture = miniCluster.requestJobResult(jobId);

		highAvailabilityServices.revokeJobMasterLeadership(jobId).get();

		assertThat(jobResultFuture.isDone(), is(false));
		BlockingOperator.isBlocking = false;

		highAvailabilityServices.grantJobMasterLeadership(jobId);

		JobResult jobResult = jobResultFuture.get();

		assertThat(jobResult.isSuccess(), is(true));
	}

	private JobGraph createJobGraph(int parallelism) {
		BlockingOperator.isBlocking = true;
		final JobVertex vertex = new JobVertex("blocking operator");
		vertex.setParallelism(parallelism);
		vertex.setInvokableClass(BlockingOperator.class);

		return new JobGraph("Blocking test job", vertex);
	}

	/**
	 * Blocking invokable which is controlled by a static field.
	 */
	public static class BlockingOperator extends AbstractInvokable {
		static boolean isBlocking = true;

		public BlockingOperator(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			if (isBlocking) {
				synchronized (this) {
					while (true) {
						wait();
					}
				}
			}
		}
	}
}

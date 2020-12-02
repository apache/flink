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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingAbstractInvokables;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for the {@link TaskExecutor}.
 */
public class TaskExecutorITCase extends TestLogger {

	private static final Duration TESTING_TIMEOUT = Duration.ofMinutes(2L);
	private static final int NUM_TMS = 2;
	private static final int SLOTS_PER_TM = 2;
	private static final int PARALLELISM = NUM_TMS * SLOTS_PER_TM;

	private MiniCluster miniCluster;

	@Before
	public void setup() throws Exception  {
		miniCluster = new MiniCluster(
			new MiniClusterConfiguration.Builder()
				.setNumTaskManagers(NUM_TMS)
				.setNumSlotsPerTaskManager(SLOTS_PER_TM)
				.build());

		miniCluster.start();
	}

	@After
	public void teardown() throws Exception {
		if (miniCluster != null) {
			miniCluster.close();
		}
	}

	/**
	 * Tests that a job can be re-executed after the job has failed due
	 * to a TaskExecutor termination.
	 */
	@Test
	public void testJobReExecutionAfterTaskExecutorTermination() throws Exception {
		final JobGraph jobGraph = createJobGraph(PARALLELISM);

		final CompletableFuture<JobResult> jobResultFuture = submitJobAndWaitUntilRunning(jobGraph);

		// kill one TaskExecutor which should fail the job execution
		miniCluster.terminateTaskManager(0);

		final JobResult jobResult = jobResultFuture.get();

		assertThat(jobResult.isSuccess(), is(false));

		miniCluster.startTaskManager();

		final JobGraph newJobGraph = createJobGraph(PARALLELISM);
		BlockingOperator.unblock();
		miniCluster.submitJob(newJobGraph).get();

		miniCluster.requestJobResult(newJobGraph.getJobID()).get();
	}

	/**
	 * Tests that the job can recover from a failing {@link TaskExecutor}.
	 */
	@Test
	public void testJobRecoveryWithFailingTaskExecutor() throws Exception {
		final JobGraph jobGraph = createJobGraphWithRestartStrategy(PARALLELISM);

		final CompletableFuture<JobResult> jobResultFuture = submitJobAndWaitUntilRunning(jobGraph);

		// start an additional TaskExecutor
		miniCluster.startTaskManager();

		miniCluster.terminateTaskManager(0).get(); // this should fail the job

		BlockingOperator.unblock();

		assertThat(jobResultFuture.get().isSuccess(), is(true));
	}

	private CompletableFuture<JobResult> submitJobAndWaitUntilRunning(JobGraph jobGraph) throws Exception {
		miniCluster.submitJob(jobGraph).get();

		final CompletableFuture<JobResult> jobResultFuture = miniCluster.requestJobResult(jobGraph.getJobID());

		assertThat(jobResultFuture.isDone(), is(false));

		CommonTestUtils.waitUntilCondition(
			jobIsRunning(() -> miniCluster.getExecutionGraph(jobGraph.getJobID())),
			Deadline.fromNow(TESTING_TIMEOUT),
			50L);

		return jobResultFuture;
	}

	private SupplierWithException<Boolean, Exception> jobIsRunning(Supplier<CompletableFuture<? extends AccessExecutionGraph>> executionGraphFutureSupplier) {
		final Predicate<AccessExecution> runningOrFinished = ExecutionGraphTestUtils.isInExecutionState(ExecutionState.RUNNING).or(ExecutionGraphTestUtils.isInExecutionState(ExecutionState.FINISHED));
		final Predicate<AccessExecutionGraph> allExecutionsRunning = ExecutionGraphTestUtils.allExecutionsPredicate(runningOrFinished);

		return () -> {
			final AccessExecutionGraph executionGraph = executionGraphFutureSupplier.get().join();
			return allExecutionsRunning.test(executionGraph) && executionGraph.getState() == JobStatus.RUNNING;
		};
	}

	private JobGraph createJobGraphWithRestartStrategy(int parallelism) throws IOException {
		final JobGraph jobGraph = createJobGraph(parallelism);
		final ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 0L));
		jobGraph.setExecutionConfig(executionConfig);

		return jobGraph;
	}

	private JobGraph createJobGraph(int parallelism) {
		final JobVertex sender = new JobVertex("Sender");
		sender.setParallelism(parallelism);
		sender.setInvokableClass(TestingAbstractInvokables.Sender.class);

		final JobVertex receiver = new JobVertex("Blocking receiver");
		receiver.setParallelism(parallelism);
		receiver.setInvokableClass(BlockingOperator.class);
		BlockingOperator.reset();

		receiver.connectNewDataSetAsInput(sender, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
		sender.setSlotSharingGroup(slotSharingGroup);
		receiver.setSlotSharingGroup(slotSharingGroup);

		return new JobGraph("Blocking test job with slot sharing", sender, receiver);
	}

	/**
	 * Blocking invokable which is controlled by a static field.
	 */
	public static class BlockingOperator extends TestingAbstractInvokables.Receiver {
		private static CountDownLatch countDownLatch = new CountDownLatch(1);

		public BlockingOperator(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			countDownLatch.await();
			super.invoke();
		}

		public static void unblock() {
			countDownLatch.countDown();
		}

		public static void reset() {
			countDownLatch = new CountDownLatch(1);
		}
	}
}

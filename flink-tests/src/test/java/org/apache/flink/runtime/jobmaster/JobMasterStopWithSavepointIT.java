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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * ITCases testing the stop with savepoint functionality.
 * This includes checking both SUSPEND and TERMINATE.
 */
public class JobMasterStopWithSavepointIT extends AbstractTestBase {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final long CHECKPOINT_INTERVAL = 10;
	private static final int PARALLELISM = 2;

	private static OneShotLatch finishingLatch;

	private static CountDownLatch invokeLatch;

	private static CountDownLatch numberOfRestarts;
	private static AtomicLong syncSavepointId = new AtomicLong();
	private static CountDownLatch checkpointsToWaitFor;


	private Path savepointDirectory;
	private MiniClusterClient clusterClient;

	private JobGraph jobGraph;

	@Test(timeout = 5000)
	public void suspendWithSavepointWithoutComplicationsShouldSucceedAndLeadJobToFinished() throws Exception {
		stopWithSavepointNormalExecutionHelper(false);
	}

	@Test(timeout = 5000)
	public void terminateWithSavepointWithoutComplicationsShouldSucceedAndLeadJobToFinished() throws Exception {
		stopWithSavepointNormalExecutionHelper(true);
	}

	private void stopWithSavepointNormalExecutionHelper(final boolean terminate) throws Exception {
		setUpJobGraph(NoOpBlockingStreamTask.class, RestartStrategies.noRestart());

		final CompletableFuture<String> savepointLocationFuture = stopWithSavepoint(terminate);

		assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

		finishingLatch.trigger();

		final String savepointLocation = savepointLocationFuture.get();
		assertThat(getJobStatus(), equalTo(JobStatus.FINISHED));

		final List<Path> savepoints;
		try (Stream<Path> savepointFiles = Files.list(savepointDirectory)) {
			savepoints = savepointFiles.map(Path::getFileName).collect(Collectors.toList());
		}
		assertThat(savepoints, hasItem(Paths.get(savepointLocation).getFileName()));
	}

	@Test(timeout = 5000)
	public void throwingExceptionOnCallbackWithNoRestartsShouldFailTheSuspend() throws Exception {
		throwingExceptionOnCallbackWithoutRestartsHelper(false);
	}

	@Test(timeout = 5000)
	public void throwingExceptionOnCallbackWithNoRestartsShouldFailTheTerminate() throws Exception {
		throwingExceptionOnCallbackWithoutRestartsHelper(true);
	}

	private void throwingExceptionOnCallbackWithoutRestartsHelper(final boolean terminate) throws Exception {
		setUpJobGraph(ExceptionOnCallbackStreamTask.class, RestartStrategies.noRestart());

		assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

		try {
			stopWithSavepoint(terminate).get();
			fail();
		} catch (Exception e) {
			// expected
		}

		// verifying that we actually received a synchronous checkpoint
		assertTrue(syncSavepointId.get() > 0);
		assertThat(getJobStatus(), equalTo(JobStatus.FAILED));
	}

	@Test(timeout = 5000)
	public void throwingExceptionOnCallbackWithRestartsShouldSimplyRestartInSuspend() throws Exception {
		throwingExceptionOnCallbackWithRestartsHelper(false);
	}

	@Test(timeout = 5000)
	public void throwingExceptionOnCallbackWithRestartsShouldSimplyRestartInTerminate() throws Exception {
		throwingExceptionOnCallbackWithRestartsHelper(true);
	}

	private void throwingExceptionOnCallbackWithRestartsHelper(final boolean terminate) throws Exception {
		final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10));
		final int numberOfCheckpointsToExpect = 10;

		numberOfRestarts = new CountDownLatch(2);
		checkpointsToWaitFor = new CountDownLatch(numberOfCheckpointsToExpect);

		setUpJobGraph(ExceptionOnCallbackStreamTask.class, RestartStrategies.fixedDelayRestart(15, Time.milliseconds(10)));
		assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));
		try {
			stopWithSavepoint(terminate).get(50, TimeUnit.MILLISECONDS);
			fail();
		} catch (Exception e) {
			// expected
		}

		// wait until we restart at least 2 times and until we see at least 10 checkpoints.
		numberOfRestarts.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
		checkpointsToWaitFor.await(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

		// verifying that we actually received a synchronous checkpoint
		assertTrue(syncSavepointId.get() > 0);

		assertThat(getJobStatus(), equalTo(JobStatus.RUNNING));

		// make sure that we saw the synchronous savepoint and
		// that after that we saw more checkpoints due to restarts.
		final long syncSavepoint = syncSavepointId.get();
		assertTrue(syncSavepoint > 0 && syncSavepoint < numberOfCheckpointsToExpect);

		clusterClient.cancel(jobGraph.getJobID());
		assertThat(getJobStatus(), either(equalTo(JobStatus.CANCELLING)).or(equalTo(JobStatus.CANCELED)));
	}

	private CompletableFuture<String> stopWithSavepoint(boolean terminate) {
		return miniClusterResource.getMiniCluster().stopWithSavepoint(
				jobGraph.getJobID(),
				savepointDirectory.toAbsolutePath().toString(),
				terminate);
	}

	private JobStatus getJobStatus() throws InterruptedException, ExecutionException {
		return clusterClient.getJobStatus(jobGraph.getJobID()).get();
	}

	private void setUpJobGraph(
			final Class<? extends AbstractInvokable> invokable,
			final RestartStrategies.RestartStrategyConfiguration restartStrategy) throws Exception {

		finishingLatch = new OneShotLatch();

		invokeLatch = new CountDownLatch(PARALLELISM);

		numberOfRestarts = new CountDownLatch(2);
		checkpointsToWaitFor = new CountDownLatch(10);

		syncSavepointId.set(-1);

		savepointDirectory = temporaryFolder.newFolder().toPath();

		Assume.assumeTrue(
				"ClusterClient is not an instance of MiniClusterClient",
				miniClusterResource.getClusterClient() instanceof MiniClusterClient);

		clusterClient = (MiniClusterClient) miniClusterResource.getClusterClient();
		clusterClient.setDetached(true);

		jobGraph = new JobGraph();

		final ExecutionConfig config = new ExecutionConfig();
		config.setRestartStrategy(restartStrategy);
		jobGraph.setExecutionConfig(config);

		final JobVertex vertex = new JobVertex("testVertex");
		vertex.setInvokableClass(invokable);
		vertex.setParallelism(PARALLELISM);
		jobGraph.addVertex(vertex);

		jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
				Collections.singletonList(vertex.getID()),
				Collections.singletonList(vertex.getID()),
				Collections.singletonList(vertex.getID()),
				new CheckpointCoordinatorConfiguration(
						CHECKPOINT_INTERVAL,
						60_000,
						10,
						1,
						CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
						true,
						false,
						0),
				null));

		clusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());
		invokeLatch.await(60, TimeUnit.SECONDS);
		waitForJob();
	}

	private void waitForJob() throws Exception {
		for (int i = 0; i < 60; i++) {
			try {
				final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
				assertThat(jobStatus.isGloballyTerminalState(), equalTo(false));
				if (jobStatus == JobStatus.RUNNING) {
					return;
				}
			} catch (ExecutionException ignored) {
				// JobManagerRunner is not yet registered in Dispatcher
			}
			Thread.sleep(1000);
		}
		throw new AssertionError("Job did not become running within timeout.");
	}

	/**
	 * A {@link StreamTask} which throws an exception in the {@code notifyCheckpointComplete()} for subtask 0.
	 */
	public static class ExceptionOnCallbackStreamTask extends NoOpStreamTask {

		private long synchronousSavepointId = Long.MIN_VALUE;

		private final transient OneShotLatch finishLatch;

		public ExceptionOnCallbackStreamTask(final Environment environment) {
			super(environment);
			this.finishLatch = new OneShotLatch();
		}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
			if (taskIndex == 0) {
				numberOfRestarts.countDown();
			}
			invokeLatch.countDown();
			finishLatch.await();
			context.allActionsCompleted();
		}

		@Override
		protected void cancelTask() throws Exception {
			super.cancelTask();
			finishLatch.trigger();
		}

		@Override
		public boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) throws Exception {
			final long checkpointId = checkpointMetaData.getCheckpointId();
			final CheckpointType checkpointType = checkpointOptions.getCheckpointType();

			if (checkpointType == CheckpointType.SYNC_SAVEPOINT) {
				synchronousSavepointId = checkpointId;
				syncSavepointId.compareAndSet(-1, synchronousSavepointId);
			}

			final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
			if (taskIndex == 0) {
				checkpointsToWaitFor.countDown();
			}
			return super.triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			final long taskIndex = getEnvironment().getTaskInfo().getIndexOfThisSubtask();
			if (checkpointId == synchronousSavepointId && taskIndex == 0) {
				throw new RuntimeException("Expected Exception");
			}

			super.notifyCheckpointComplete(checkpointId);
		}
	}

	/**
	 * A {@link StreamTask} that simply waits to be terminated normally.
	 */
	public static class NoOpBlockingStreamTask extends NoOpStreamTask {

		private final transient OneShotLatch finishLatch;

		public NoOpBlockingStreamTask(final Environment environment) {
			super(environment);
			this.finishLatch = new OneShotLatch();
		}

		@Override
		protected void processInput(ActionContext context) throws Exception {
			invokeLatch.countDown();
			finishLatch.await();
			context.allActionsCompleted();
		}

		@Override
		public void finishTask() throws Exception {
			finishingLatch.await();
			finishLatch.trigger();
		}
	}
}

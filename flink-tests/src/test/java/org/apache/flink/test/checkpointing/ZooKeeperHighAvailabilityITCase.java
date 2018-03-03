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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.Preconditions;

import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore}.
 */
public class ZooKeeperHighAvailabilityITCase extends TestBaseUtils {

	private static final FiniteDuration TEST_TIMEOUT = new FiniteDuration(5, TimeUnit.MINUTES);

	private static final int NUM_JMS = 1;
	private static final int NUM_TMS = 1;
	private static final int NUM_SLOTS_PER_TM = 1;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static File haStorageDir;

	private static TestingServer zkServer;

	private static LocalFlinkMiniCluster cluster = null;

	private static OneShotLatch waitForCheckpointLatch = new OneShotLatch();
	private static OneShotLatch failInCheckpointLatch = new OneShotLatch();

	@BeforeClass
	public static void setup() throws Exception {
		zkServer = new TestingServer();

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_JOB_MANAGER, NUM_JMS);
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);

		haStorageDir = TEMPORARY_FOLDER.newFolder();

		config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haStorageDir.toString());
		config.setString(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		cluster = TestBaseUtils.startCluster(config, false);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);

		zkServer.stop();
		zkServer.close();
	}

	/**
	 * Verify that we don't start a job from scratch if we cannot restore any of the
	 * CompletedCheckpoints.
	 *
	 * <p>Synchronization for the different steps and things we want to observe happens via
	 * latches in the test method and the methods of {@link CheckpointBlockingFunction}.
	 *
	 * <p>The test follows these steps:
	 * <ol>
	 *     <li>Start job and block on a latch until we have done some checkpoints
	 *     <li>Block in the special function
	 *     <li>Move away the contents of the ZooKeeper HA directory to make restoring from
	 *       checkpoints impossible
	 *     <li>Unblock the special function, which now induces a failure
	 *     <li>Make sure that the job does not recover successfully
	 *     <li>Move back the HA directory
	 *     <li>Make sure that the job recovers, we use a latch to ensure that the operator
	 *       restored successfully
	 * </ol>
	 */
	@Test(timeout = 120_000L)
	public void testRestoreBehaviourWithFaultyStateHandles() throws Exception {
		CheckpointBlockingFunction.allowedInitializeCallsWithoutRestore.set(1);
		CheckpointBlockingFunction.successfulRestores.set(0);
		CheckpointBlockingFunction.illegalRestores.set(0);
		CheckpointBlockingFunction.afterMessWithZooKeeper.set(false);
		CheckpointBlockingFunction.failedAlready.set(false);

		waitForCheckpointLatch = new OneShotLatch();
		failInCheckpointLatch = new OneShotLatch();

		final Deadline deadline = TEST_TIMEOUT.fromNow();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0));
		env.enableCheckpointing(10); // Flink doesn't allow lower than 10 ms

		File checkpointLocation = TEMPORARY_FOLDER.newFolder();
		env.setStateBackend(new FsStateBackend(checkpointLocation.toURI()));

		DataStreamSource<String> source = env.addSource(new UnboundedSource());

		source
			.keyBy((str) -> str)
			.map(new CheckpointBlockingFunction());

		JobGraph jobGraph = env.getStreamGraph().getJobGraph();
		JobID jobID = Preconditions.checkNotNull(jobGraph.getJobID());

		// Retrieve the job manager
		ActorGateway jobManager = Await.result(cluster.leaderGateway().future(), deadline.timeLeft());
		cluster.submitJobDetached(jobGraph);

		// wait until we did some checkpoints
		waitForCheckpointLatch.await();

		// mess with the HA directory so that the job cannot restore
		File movedCheckpointLocation = TEMPORARY_FOLDER.newFolder();
		int numCheckpoints = 0;
		File[] files = haStorageDir.listFiles();
		assertNotNull(files);
		for (File file : files) {
			if (file.getName().startsWith("completedCheckpoint")) {
				assertTrue(file.renameTo(new File(movedCheckpointLocation, file.getName())));
				numCheckpoints++;
			}
		}
		assertTrue(numCheckpoints > 0);

		failInCheckpointLatch.trigger();

		// Ensure that we see at least one cycle where the job tries to restart and fails.
		CompletableFuture<JobStatus> jobStatusFuture = FutureUtils.retrySuccesfulWithDelay(
			() -> getJobStatus(jobManager, jobID, TEST_TIMEOUT),
			Time.milliseconds(1),
			deadline,
			(jobStatus) -> jobStatus == JobStatus.RESTARTING,
			TestingUtils.defaultScheduledExecutor());
		assertEquals(JobStatus.RESTARTING, jobStatusFuture.get());

		jobStatusFuture = FutureUtils.retrySuccesfulWithDelay(
			() -> getJobStatus(jobManager, jobID, TEST_TIMEOUT),
			Time.milliseconds(1),
			deadline,
			(jobStatus) -> jobStatus == JobStatus.FAILING,
			TestingUtils.defaultScheduledExecutor());
		assertEquals(JobStatus.FAILING, jobStatusFuture.get());

		// move back the HA directory so that the job can restore
		CheckpointBlockingFunction.afterMessWithZooKeeper.set(true);

		files = movedCheckpointLocation.listFiles();
		assertNotNull(files);
		for (File file : files) {
			if (file.getName().startsWith("completedCheckpoint")) {
				assertTrue(file.renameTo(new File(haStorageDir, file.getName())));
			}
		}

		// now the job should be able to go to RUNNING again and then eventually to FINISHED,
		// which it only does if it could successfully restore
		jobStatusFuture = FutureUtils.retrySuccesfulWithDelay(
			() -> getJobStatus(jobManager, jobID, TEST_TIMEOUT),
			Time.milliseconds(50),
			deadline,
			(jobStatus) -> jobStatus == JobStatus.FINISHED,
			TestingUtils.defaultScheduledExecutor());
		assertEquals(JobStatus.FINISHED, jobStatusFuture.get());

		assertThat("We saw illegal restores.", CheckpointBlockingFunction.illegalRestores.get(), is(0));
	}

	/**
	 * Requests the {@link JobStatus} of the job with the given {@link JobID}.
	 */
	private CompletableFuture<JobStatus> getJobStatus(
		ActorGateway jobManager,
		JobID jobId,
		FiniteDuration timeout) {

		Future<Object> response = jobManager.ask(JobManagerMessages.getRequestJobStatus(jobId), timeout);

		CompletableFuture<Object> javaFuture = FutureUtils.toJava(response);

		return javaFuture.thenApply((responseMessage) -> {
			if (responseMessage instanceof JobManagerMessages.CurrentJobStatus) {
				return ((JobManagerMessages.CurrentJobStatus) responseMessage).status();
			} else if (responseMessage instanceof JobManagerMessages.JobNotFound) {
				throw new CompletionException(
					new IllegalStateException("Could not find job with JobId " + jobId));
			} else {
				throw new CompletionException(
					new IllegalStateException("Unknown JobManager response of type " + responseMessage.getClass()));
			}
		});
	}

	private static class UnboundedSource implements SourceFunction<String> {
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			while (running && !CheckpointBlockingFunction.afterMessWithZooKeeper.get()) {
				ctx.collect("hello");
				// don't overdo it ... ;-)
				Thread.sleep(50);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class CheckpointBlockingFunction
			extends RichMapFunction<String, String>
			implements CheckpointedFunction {

		// verify that we only call initializeState()
		// once with isRestored() == false. All other invocations must have isRestored() == true. This
		// verifies that we don't restart a job from scratch in case the CompletedCheckpoints can't
		// be read.
		static AtomicInteger allowedInitializeCallsWithoutRestore = new AtomicInteger(1);

		// we count when we see restores that are not allowed. We only
		// allow restores once we messed with the HA directory and moved it back again
		static AtomicInteger illegalRestores = new AtomicInteger(0);
		static AtomicInteger successfulRestores = new AtomicInteger(0);

		// whether we are after the phase where we messed with the ZooKeeper HA directory, i.e.
		// whether it's now ok for a restore to happen
		static AtomicBoolean afterMessWithZooKeeper = new AtomicBoolean(false);

		static AtomicBoolean failedAlready = new AtomicBoolean(false);

		// also have some state to write to the checkpoint
		private final ValueStateDescriptor<String> stateDescriptor =
			new ValueStateDescriptor<>("state", StringSerializer.INSTANCE);

		@Override
		public String map(String value) throws Exception {
			getRuntimeContext().getState(stateDescriptor).update("42");
			return value;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			if (context.getCheckpointId() > 5) {
				waitForCheckpointLatch.trigger();
				failInCheckpointLatch.await();
				if (!failedAlready.getAndSet(true)) {
					throw new RuntimeException("Failing on purpose.");
				}
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) {
			if (!context.isRestored()) {
				int updatedValue = allowedInitializeCallsWithoutRestore.decrementAndGet();
				if (updatedValue < 0) {
					illegalRestores.getAndIncrement();
					throw new RuntimeException("We are not allowed any more restores.");
				}
			} else {
				if (!afterMessWithZooKeeper.get()) {
					illegalRestores.getAndIncrement();
				} else if (successfulRestores.getAndIncrement() > 0) {
					// already saw the one allowed successful restore
					illegalRestores.getAndIncrement();
				}
			}
		}
	}
}

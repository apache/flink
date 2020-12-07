/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.state.ManualWindowSpeedITCase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.junit.Assert.assertNotNull;

/**
 * IT case for resuming from checkpoints manually via their external pointer, rather than automatic
 * failover through the checkpoint coordinator. This test checks that this works properly with the
 * common state backends and checkpoint stores, in combination with asynchronous and incremental
 * snapshots.
 *
 * <p>This tests considers full and incremental checkpoints and was introduced to guard against problems like FLINK-6964.
 */
public class ResumeCheckpointManuallyITCase extends TestLogger {

	private static final int PARALLELISM = 2;
	private static final int NUM_TASK_MANAGERS = 2;
	private static final int SLOTS_PER_TASK_MANAGER = 2;

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testExternalizedIncrementalRocksDBCheckpointsStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createRocksDBStateBackend(checkpointDir, true),
			false);
	}

	@Test
	public void testExternalizedFullRocksDBCheckpointsStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createRocksDBStateBackend(checkpointDir, false),
			false);
	}

	@Test
	public void testExternalizedIncrementalRocksDBCheckpointsWithLocalRecoveryStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createRocksDBStateBackend(checkpointDir, true),
			true);
	}

	@Test
	public void testExternalizedFullRocksDBCheckpointsWithLocalRecoveryStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createRocksDBStateBackend(checkpointDir, false),
			true);
	}

	@Test
	public void testExternalizedFSCheckpointsStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createFsStateBackend(checkpointDir),
			false);
	}

	@Test
	public void testExternalizedFSCheckpointsWithLocalRecoveryStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			createFsStateBackend(checkpointDir),
			true);
	}

	@Test
	public void testExternalizedIncrementalRocksDBCheckpointsZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createRocksDBStateBackend(checkpointDir, true),
				false);
		} finally {
			zkServer.stop();
		}
	}

	@Test
	public void testExternalizedFullRocksDBCheckpointsZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createRocksDBStateBackend(checkpointDir, false),
				false);
		} finally {
			zkServer.stop();
		}
	}

	@Test
	public void testExternalizedIncrementalRocksDBCheckpointsWithLocalRecoveryZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createRocksDBStateBackend(checkpointDir, true),
				true);
		} finally {
			zkServer.stop();
		}
	}

	@Test
	public void testExternalizedFullRocksDBCheckpointsWithLocalRecoveryZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createRocksDBStateBackend(checkpointDir, false),
				true);
		} finally {
			zkServer.stop();
		}
	}

	@Test
	public void testExternalizedFSCheckpointsZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createFsStateBackend(checkpointDir),
				false);
		} finally {
			zkServer.stop();
		}
	}

	@Test
	public void testExternalizedFSCheckpointsWithLocalRecoveryZookeeper() throws Exception {
		TestingServer zkServer = new TestingServer();
		zkServer.start();
		try {
			final File checkpointDir = temporaryFolder.newFolder();
			testExternalizedCheckpoints(
				checkpointDir,
				zkServer.getConnectString(),
				createFsStateBackend(checkpointDir),
				true);
		} finally {
			zkServer.stop();
		}
	}

	private FsStateBackend createFsStateBackend(File checkpointDir) throws IOException {
		return new FsStateBackend(checkpointDir.toURI().toString(), true);
	}

	private RocksDBStateBackend createRocksDBStateBackend(
		File checkpointDir,
		boolean incrementalCheckpointing) throws IOException {

		return new RocksDBStateBackend(checkpointDir.toURI().toString(), incrementalCheckpointing);
	}

	private void testExternalizedCheckpoints(
		File checkpointDir,
		String zooKeeperQuorum,
		StateBackend backend,
		boolean localRecovery) throws Exception {

		final Configuration config = new Configuration();

		final File savepointDir = temporaryFolder.newFolder();

		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
		config.setBoolean(CheckpointingOptions.LOCAL_RECOVERY, localRecovery);

		// ZooKeeper recovery mode?
		if (zooKeeperQuorum != null) {
			final File haDir = temporaryFolder.newFolder();
			config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperQuorum);
			config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haDir.toURI().toString());
		}

		MiniClusterWithClientResource cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(config)
				.setNumberTaskManagers(NUM_TASK_MANAGERS)
				.setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
				.build());

		cluster.before();

		ClusterClient<?> client = cluster.getClusterClient();

		try {
			// main test sequence:  start job -> eCP -> restore job -> eCP -> restore job
			String firstExternalCheckpoint = runJobAndGetExternalizedCheckpoint(backend, checkpointDir, null, client);
			assertNotNull(firstExternalCheckpoint);

			String secondExternalCheckpoint = runJobAndGetExternalizedCheckpoint(backend, checkpointDir, firstExternalCheckpoint, client);
			assertNotNull(secondExternalCheckpoint);

			String thirdExternalCheckpoint = runJobAndGetExternalizedCheckpoint(backend, checkpointDir, secondExternalCheckpoint, client);
			assertNotNull(thirdExternalCheckpoint);
		} finally {
			cluster.after();
		}
	}

	private static String runJobAndGetExternalizedCheckpoint(StateBackend backend, File checkpointDir, @Nullable String externalCheckpoint, ClusterClient<?> client) throws Exception {
		JobGraph initialJobGraph = getJobGraph(backend, externalCheckpoint);
		NotifyingInfiniteTupleSource.countDownLatch = new CountDownLatch(PARALLELISM);

		client.submitJob(initialJobGraph).get();

		// wait until all sources have been started
		NotifyingInfiniteTupleSource.countDownLatch.await();

		waitUntilExternalizedCheckpointCreated(checkpointDir, initialJobGraph.getJobID());
		client.cancel(initialJobGraph.getJobID()).get();
		waitUntilCanceled(initialJobGraph.getJobID(), client);

		return getExternalizedCheckpointCheckpointPath(checkpointDir, initialJobGraph.getJobID());
	}

	private static String getExternalizedCheckpointCheckpointPath(File checkpointDir, JobID jobId) throws IOException {
		Optional<Path> checkpoint = findExternalizedCheckpoint(checkpointDir, jobId);
		if (!checkpoint.isPresent()) {
			throw new AssertionError("No complete checkpoint could be found.");
		} else {
			return checkpoint.get().toString();
		}
	}

	private static void waitUntilExternalizedCheckpointCreated(File checkpointDir, JobID jobId) throws InterruptedException, IOException {
		while (true) {
			Thread.sleep(50);
			Optional<Path> externalizedCheckpoint = findExternalizedCheckpoint(checkpointDir, jobId);
			if (externalizedCheckpoint.isPresent()) {
				break;
			}
		}
	}

	private static Optional<Path> findExternalizedCheckpoint(File checkpointDir, JobID jobId) throws IOException {
		try (Stream<Path> checkpoints = Files.list(checkpointDir.toPath().resolve(jobId.toString()))) {
			return checkpoints
				.filter(path -> path.getFileName().toString().startsWith("chk-"))
				.filter(path -> {
					try (Stream<Path> checkpointFiles = Files.list(path)) {
						return checkpointFiles.anyMatch(child -> child.getFileName().toString().contains("meta"));
					} catch (IOException ignored) {
						return false;
					}
				})
				.findAny();
		}
	}

	private static void waitUntilCanceled(JobID jobId, ClusterClient<?> client) throws ExecutionException, InterruptedException {
		while (client.getJobStatus(jobId).get() != JobStatus.CANCELED) {
			Thread.sleep(50);
		}
	}

	private static JobGraph getJobGraph(StateBackend backend, @Nullable String externalCheckpoint) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.enableCheckpointing(500);
		env.setStateBackend(backend);
		env.setParallelism(PARALLELISM);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

		env
				.addSource(new NotifyingInfiniteTupleSource(10_000))
				.assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.seconds(3)))
				.reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
				.filter(value -> value.f0.startsWith("Tuple 0"));

		StreamGraph streamGraph = env.getStreamGraph("Test");

		JobGraph jobGraph = streamGraph.getJobGraph();

		// recover from previous iteration?
		if (externalCheckpoint != null) {
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(externalCheckpoint));
		}

		return jobGraph;
	}

	/**
	 * Infinite source which notifies when all of its sub tasks have been started via the count down latch.
	 */
	public static class NotifyingInfiniteTupleSource extends ManualWindowSpeedITCase.InfiniteTupleSource {

		private static final long serialVersionUID = 8120981235081181746L;

		private static CountDownLatch countDownLatch;

		public NotifyingInfiniteTupleSource(int numKeys) {
			super(numKeys);
		}

		@Override
		public void run(SourceContext<Tuple2<String, Integer>> out) throws Exception {
			if (countDownLatch != null) {
				countDownLatch.countDown();
			}

			super.run(out);
		}
	}

	/**
	 * This {@link WatermarkStrategy} assigns the current system time as the event-time timestamp.
	 * In a real use case you should use proper timestamps and an appropriate {@link
	 * WatermarkStrategy}.
	 */
	private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

		private IngestionTimeWatermarkStrategy() {
		}

		public static <T> IngestionTimeWatermarkStrategy<T> create() {
			return new IngestionTimeWatermarkStrategy<>();
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
			return new AscendingTimestampsWatermarks<>();
		}

		@Override
		public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
			return (event, timestamp) -> System.currentTimeMillis();
		}
	}
}

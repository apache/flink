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

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.test.state.ManualWindowSpeedITCase;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CountDownLatch;

/**
 * IT case for externalized checkpoints with {@link org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore}
 * and {@link org.apache.flink.runtime.checkpoint.ZooKeeperCompletedCheckpointStore}.
 *
 * <p>This tests considers full and incremental checkpoints and was introduced to guard against problems like FLINK-6964.
 */
public class ExternalizedCheckpointITCase extends TestLogger {

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
			new RocksDBStateBackend(checkpointDir.toURI().toString(), true));
	}

	@Test
	public void testExternalizedFullRocksDBCheckpointsStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			new RocksDBStateBackend(checkpointDir.toURI().toString(), false));
	}

	@Test
	public void testExternalizedFSCheckpointsStandalone() throws Exception {
		final File checkpointDir = temporaryFolder.newFolder();
		testExternalizedCheckpoints(
			checkpointDir,
			null,
			new FsStateBackend(checkpointDir.toURI().toString(), true));

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
				new RocksDBStateBackend(checkpointDir.toURI().toString(), true));
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
				new RocksDBStateBackend(checkpointDir.toURI().toString(), false));
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
				new FsStateBackend(checkpointDir.toURI().toString(), true));
		} finally {
			zkServer.stop();
		}
	}

	private void testExternalizedCheckpoints(
		File checkpointDir,
		String zooKeeperQuorum,
		AbstractStateBackend backend) throws Exception {

		final Configuration config = new Configuration();

		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, SLOTS_PER_TASK_MANAGER);

		final File savepointDir = temporaryFolder.newFolder();

		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
		config.setString(CoreOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
		config.setString(CoreOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());

		// ZooKeeper recovery mode?
		if (zooKeeperQuorum != null) {
			final File haDir = temporaryFolder.newFolder();
			config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperQuorum);
			config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haDir.toURI().toString());
		}

		TestingCluster cluster = new TestingCluster(config);
		cluster.start();

		String externalCheckpoint = null;

		try {

			// main test sequence:  start job -> eCP -> restore job -> eCP -> restore job -> eCP
			for (int i = 0; i < 3; ++i) {
				final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

				env.setStateBackend(backend);
				env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

				env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
				env.setParallelism(PARALLELISM);

				// initialize count down latch
				NotifyingInfiniteTupleSource.countDownLatch = new CountDownLatch(PARALLELISM);

				env.addSource(new NotifyingInfiniteTupleSource(10_000))
					.keyBy(0)
					.timeWindow(Time.seconds(3))
					.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Tuple2<String, Integer> reduce(
							Tuple2<String, Integer> value1,
							Tuple2<String, Integer> value2) throws Exception {
							return Tuple2.of(value1.f0, value1.f1 + value2.f1);
						}
					})
					.filter(new FilterFunction<Tuple2<String, Integer>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public boolean filter(Tuple2<String, Integer> value) throws Exception {
							return value.f0.startsWith("Tuple 0");
						}
					});

				StreamGraph streamGraph = env.getStreamGraph();
				streamGraph.setJobName("Test");

				JobGraph jobGraph = streamGraph.getJobGraph();

				// recover from previous iteration?
				if (externalCheckpoint != null) {
					jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(externalCheckpoint));
				}

				config.addAll(jobGraph.getJobConfiguration());
				JobSubmissionResult submissionResult = cluster.submitJobDetached(jobGraph);

				// wait until all sources have been started
				NotifyingInfiniteTupleSource.countDownLatch.await();

				externalCheckpoint =
					cluster.requestCheckpoint(submissionResult.getJobID(), CheckpointOptions.forFullCheckpoint());

				cluster.cancelJob(submissionResult.getJobID());
			}
		} finally {
			cluster.stop();
			cluster.awaitTermination();
		}
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
}

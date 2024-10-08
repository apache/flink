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

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.FsStateChangelogStorageFactory;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.state.ManualWindowSpeedITCase;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.test.util.TestUtils.waitUntilJobCanceled;
import static org.junit.Assert.assertNotNull;

/**
 * IT case for resuming from checkpoints manually via their external pointer, rather than automatic
 * failover through the checkpoint coordinator. This test checks that this works properly with the
 * common state backends and checkpoint stores, in combination with asynchronous and incremental
 * snapshots.
 *
 * <p>This tests considers full and incremental checkpoints and was introduced to guard against
 * problems like FLINK-6964.
 */
@RunWith(Parameterized.class)
public class ResumeCheckpointManuallyITCase extends TestLogger {

    private static final int PARALLELISM = 2;
    private static final int NUM_TASK_MANAGERS = 2;
    private static final int SLOTS_PER_TASK_MANAGER = 2;

    @Parameterized.Parameter public RecoveryClaimMode recoveryClaimMode;

    @Parameterized.Parameters(name = "RecoveryClaimMode = {0}")
    public static Object[] parameters() {
        return RecoveryClaimMode.values();
    }

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testExternalizedIncrementalRocksDBCheckpointsStandalone() throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                createRocksDBStateBackend(checkpointDir, true),
                false,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedFullRocksDBCheckpointsStandalone() throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                createRocksDBStateBackend(checkpointDir, false),
                false,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedIncrementalRocksDBCheckpointsWithLocalRecoveryStandalone()
            throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                createRocksDBStateBackend(checkpointDir, true),
                true,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedFullRocksDBCheckpointsWithLocalRecoveryStandalone()
            throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                createRocksDBStateBackend(checkpointDir, false),
                true,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedFSCheckpointsStandalone() throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir, null, createFsStateBackend(checkpointDir), false, recoveryClaimMode);
    }

    @Test
    public void testExternalizedFSCheckpointsWithLocalRecoveryStandalone() throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        testExternalizedCheckpoints(
                checkpointDir, null, createFsStateBackend(checkpointDir), true, recoveryClaimMode);
    }

    @Test
    public void testExternalizedIncrementalRocksDBCheckpointsZookeeper() throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createRocksDBStateBackend(checkpointDir, true),
                    false,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedFullRocksDBCheckpointsZookeeper() throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createRocksDBStateBackend(checkpointDir, false),
                    false,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedIncrementalRocksDBCheckpointsWithLocalRecoveryZookeeper()
            throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createRocksDBStateBackend(checkpointDir, true),
                    true,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedFullRocksDBCheckpointsWithLocalRecoveryZookeeper()
            throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createRocksDBStateBackend(checkpointDir, false),
                    true,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedFSCheckpointsZookeeper() throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createFsStateBackend(checkpointDir),
                    false,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedFSCheckpointsWithLocalRecoveryZookeeper() throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    createFsStateBackend(checkpointDir),
                    true,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedSwitchRocksDBCheckpointsStandalone() throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        Configuration previousStateBackendConfig = createRocksDBStateBackend(checkpointDir, false);
        Configuration newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                previousStateBackendConfig,
                newStateBackendConfig,
                previousStateBackendConfig,
                false,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedSwitchRocksDBCheckpointsWithLocalRecoveryStandalone()
            throws Exception {
        final File checkpointDir = temporaryFolder.newFolder();
        Configuration previousStateBackendConfig = createRocksDBStateBackend(checkpointDir, false);
        Configuration newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
        testExternalizedCheckpoints(
                checkpointDir,
                null,
                previousStateBackendConfig,
                newStateBackendConfig,
                previousStateBackendConfig,
                true,
                recoveryClaimMode);
    }

    @Test
    public void testExternalizedSwitchRocksDBCheckpointsZookeeper() throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            Configuration previousStateBackendConfig =
                    createRocksDBStateBackend(checkpointDir, false);
            Configuration newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    previousStateBackendConfig,
                    newStateBackendConfig,
                    previousStateBackendConfig,
                    false,
                    recoveryClaimMode);
        }
    }

    @Test
    public void testExternalizedSwitchRocksDBCheckpointsWithLocalRecoveryZookeeper()
            throws Exception {
        try (TestingServer zkServer = ZooKeeperTestUtils.createAndStartZookeeperTestingServer()) {
            final File checkpointDir = temporaryFolder.newFolder();
            Configuration previousStateBackendConfig =
                    createRocksDBStateBackend(checkpointDir, false);
            Configuration newStateBackendConfig = createRocksDBStateBackend(checkpointDir, true);
            testExternalizedCheckpoints(
                    checkpointDir,
                    zkServer.getConnectString(),
                    previousStateBackendConfig,
                    newStateBackendConfig,
                    previousStateBackendConfig,
                    true,
                    recoveryClaimMode);
        }
    }

    private Configuration createFsStateBackend(File checkpointDir) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        return configuration;
    }

    private Configuration createRocksDBStateBackend(
            File checkpointDir, boolean incrementalCheckpointing) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, incrementalCheckpointing);
        return configuration;
    }

    private static void testExternalizedCheckpoints(
            File checkpointDir,
            String zooKeeperQuorum,
            Configuration configuration,
            boolean localRecovery,
            RecoveryClaimMode recoveryClaimMode)
            throws Exception {
        testExternalizedCheckpoints(
                checkpointDir,
                zooKeeperQuorum,
                configuration,
                configuration,
                configuration,
                localRecovery,
                recoveryClaimMode);
    }

    private static void testExternalizedCheckpoints(
            File checkpointDir,
            String zooKeeperQuorum,
            Configuration config1,
            Configuration config2,
            Configuration config3,
            boolean localRecovery,
            RecoveryClaimMode recoveryClaimMode)
            throws Exception {

        final Configuration config = new Configuration();

        final File savepointDir = temporaryFolder.newFolder();

        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
        config.set(StateRecoveryOptions.LOCAL_RECOVERY, localRecovery);

        // Configure DFS DSTL for this test as it might produce too much GC pressure if
        // ChangelogStateBackend is used.
        // Doing it on cluster level unconditionally as randomization currently happens on the job
        // level (environment); while this factory can only be set on the cluster level.
        FsStateChangelogStorageFactory.configure(
                config, temporaryFolder.newFolder(), Duration.ofMinutes(1), 10);

        // ZooKeeper recovery mode?
        if (zooKeeperQuorum != null) {
            final File haDir = temporaryFolder.newFolder();
            config.set(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
            config.set(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperQuorum);
            config.set(HighAvailabilityOptions.HA_STORAGE_PATH, haDir.toURI().toString());
        }

        MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                                .build());

        cluster.before();

        try {
            // main test sequence:  start job -> eCP -> restore job -> eCP -> restore job
            String firstExternalCheckpoint =
                    runJobAndGetExternalizedCheckpoint(config1, null, cluster, recoveryClaimMode);
            assertNotNull(firstExternalCheckpoint);

            String secondExternalCheckpoint =
                    runJobAndGetExternalizedCheckpoint(
                            config2, firstExternalCheckpoint, cluster, recoveryClaimMode);
            assertNotNull(secondExternalCheckpoint);

            String thirdExternalCheckpoint =
                    runJobAndGetExternalizedCheckpoint(
                            config3,
                            // in CLAIM mode, the previous run is only guaranteed to preserve the
                            // latest checkpoint; in NO_CLAIM/LEGACY, even the initial checkpoints
                            // must remain valid
                            recoveryClaimMode == RecoveryClaimMode.CLAIM
                                    ? secondExternalCheckpoint
                                    : firstExternalCheckpoint,
                            cluster,
                            recoveryClaimMode);
            assertNotNull(thirdExternalCheckpoint);
        } finally {
            cluster.after();
        }
    }

    private static String runJobAndGetExternalizedCheckpoint(
            Configuration configuration,
            @Nullable String externalCheckpoint,
            MiniClusterWithClientResource cluster,
            RecoveryClaimMode recoveryClaimMode)
            throws Exception {
        // complete at least two checkpoints so that the initial checkpoint can be subsumed
        return runJobAndGetExternalizedCheckpoint(
                externalCheckpoint, cluster, recoveryClaimMode, configuration, 2, true);
    }

    static String runJobAndGetExternalizedCheckpoint(
            @Nullable String externalCheckpoint,
            MiniClusterWithClientResource cluster,
            RecoveryClaimMode recoveryClaimMode,
            Configuration jobConfig,
            int consecutiveCheckpoints,
            boolean retainCheckpoints)
            throws Exception {
        JobGraph initialJobGraph =
                getJobGraph(externalCheckpoint, recoveryClaimMode, jobConfig, retainCheckpoints);
        NotifyingInfiniteTupleSource.countDownLatch = new CountDownLatch(PARALLELISM);
        cluster.getClusterClient().submitJob(initialJobGraph).get();

        // wait until all sources have been started
        NotifyingInfiniteTupleSource.countDownLatch.await();

        waitForCheckpoint(
                initialJobGraph.getJobID(), cluster.getMiniCluster(), consecutiveCheckpoints);
        cluster.getClusterClient().cancel(initialJobGraph.getJobID()).get();
        waitUntilJobCanceled(initialJobGraph.getJobID(), cluster.getClusterClient());

        return getLatestCompletedCheckpointPath(
                        initialJobGraph.getJobID(), cluster.getMiniCluster())
                .<IllegalStateException>orElseThrow(
                        () -> {
                            throw new IllegalStateException("Checkpoint not generated");
                        });
    }

    private static JobGraph getJobGraph(
            @Nullable String externalCheckpoint,
            RecoveryClaimMode recoveryClaimMode,
            Configuration jobConfig,
            boolean retainCheckpoints) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(jobConfig);

        env.enableCheckpointing(500);
        env.setParallelism(PARALLELISM);
        env.getCheckpointConfig()
                .setExternalizedCheckpointRetention(
                        retainCheckpoints
                                ? ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
                                : ExternalizedCheckpointRetention.DELETE_ON_CANCELLATION);
        RestartStrategyUtils.configureNoRestartStrategy(env);

        env.addSource(new NotifyingInfiniteTupleSource(10_000))
                .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create())
                .keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(3)))
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .filter(value -> value.f0.startsWith("Tuple 0"));

        StreamGraph streamGraph = env.getStreamGraph();

        JobGraph jobGraph = streamGraph.getJobGraph();

        // recover from previous iteration?
        if (externalCheckpoint != null) {
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(externalCheckpoint, false, recoveryClaimMode));
        }

        return jobGraph;
    }

    /**
     * Infinite source which notifies when all of its sub tasks have been started via the count down
     * latch.
     */
    public static class NotifyingInfiniteTupleSource
            extends ManualWindowSpeedITCase.InfiniteTupleSource {

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

        private IngestionTimeWatermarkStrategy() {}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }
}

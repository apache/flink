/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV6Serializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV7Serializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.FIXED_DELAY;
import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.filesOtherThanMetadata;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.readMetadataVersion;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.ENUMERATOR_STATE;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.ENUMERATOR_STATE_SIZE;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.waitForRestoredEnumeratorState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the state of a source coordinator is restored after a JobManager failover with
 * ZooKeeper high availability, where the second JobManager recovers the job and its completed
 * checkpoints from the high availability store, with and without the coordinator state in its own
 * file.
 */
class CoordinatorStateInFileHaRecoveryITCase {

    @RegisterExtension
    private static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @BeforeEach
    void resetSource() {
        EnumeratorStateTestSource.restoredEnumeratorState = new CompletableFuture<>();
        EnumeratorStateTestSource.FAIL_ENUMERATOR_AFTER_CHECKPOINT.set(false);
    }

    @ParameterizedTest(name = "coordinator state in file: {0}")
    @ValueSource(booleans = {true, false})
    void testSourceCoordinatorStateIsRestoredAfterJobManagerFailover(
            boolean coordinatorStateInFile, @TempDir Path tmp) throws Exception {
        final Configuration configuration = createConfiguration(coordinatorStateInFile, tmp);
        // the job takes its restart strategy from the cluster that runs it
        final JobGraph jobGraph = createJobGraph(configuration);
        final JobID jobId = jobGraph.getJobID();

        final Configuration firstConfiguration = new Configuration(configuration);
        // keep the job running when its TaskManager disconnects while the first cluster shuts down
        firstConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        firstConfiguration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        firstConfiguration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));

        final MiniCluster miniCluster =
                new MiniCluster(createMiniClusterConfiguration(firstConfiguration));
        try {
            miniCluster.start();
            miniCluster.submitJob(jobGraph).get();
            // a completed checkpoint after the first one, so the HA store has replaced its entry
            waitForCheckpoint(jobId, miniCluster, 2);
        } finally {
            miniCluster.closeAsyncWithoutCleaningHighAvailabilityData().get();
        }

        final Path checkpoint = latestCompletedCheckpoint(tmp.resolve("checkpoints"), jobId);
        final List<Path> haEntries = completedCheckpointHaEntries(tmp.resolve("ha"));
        assertThat(haEntries).isNotEmpty();
        if (coordinatorStateInFile) {
            assertThat(readMetadataVersion(checkpoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(checkpoint)).hasSize(1);
            // the high availability store does not keep a second copy of the coordinator state
            assertThat(haEntries).allMatch(entry -> size(entry) < ENUMERATOR_STATE_SIZE);
        } else {
            assertThat(readMetadataVersion(checkpoint)).isEqualTo(MetadataV6Serializer.VERSION);
            assertThat(filesOtherThanMetadata(checkpoint)).isEmpty();
            assertThat(haEntries).allMatch(entry -> size(entry) > ENUMERATOR_STATE_SIZE);
        }

        final Configuration recoveringConfiguration = new Configuration(configuration);
        // fail the job if the recovering JobManager cannot restore it, rather than restart it
        recoveringConfiguration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());

        // only count restores by the recovering JobManager
        EnumeratorStateTestSource.restoredEnumeratorState = new CompletableFuture<>();
        try (MiniCluster recoveringMiniCluster =
                new MiniCluster(createMiniClusterConfiguration(recoveringConfiguration))) {
            recoveringMiniCluster.start();
            assertThat(waitForRestoredEnumeratorState(recoveringMiniCluster, jobId))
                    .isEqualTo(ENUMERATOR_STATE);
        }
    }

    private static MiniClusterConfiguration createMiniClusterConfiguration(
            Configuration configuration) {
        return new MiniClusterConfiguration.Builder()
                .setConfiguration(configuration)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(1)
                .build();
    }

    private static Configuration createConfiguration(boolean coordinatorStateInFile, Path tmp) {
        final Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tmp.resolve("checkpoints").toUri().toString());
        configuration.set(
                CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, coordinatorStateInFile);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOOKEEPER_EXTENSION.getCustomExtension().getConnectString());
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, tmp.resolve("ha").toString());
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());
        return configuration;
    }

    private static JobGraph createJobGraph(Configuration configuration) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.fromSource(
                        new EnumeratorStateTestSource(),
                        WatermarkStrategy.noWatermarks(),
                        "enumerator-state-source")
                .sinkTo(new DiscardingSink<>());
        return env.getStreamGraph().getJobGraph();
    }

    private static Path latestCompletedCheckpoint(Path checkpointsDirectory, JobID jobId)
            throws IOException {
        try (Stream<Path> directories =
                Files.list(checkpointsDirectory.resolve(jobId.toString()))) {
            return directories
                    .filter(directory -> directory.getFileName().toString().startsWith("chk-"))
                    .filter(directory -> Files.exists(directory.resolve(METADATA_FILE_NAME)))
                    .max(
                            Comparator.comparingLong(
                                    directory ->
                                            Long.parseLong(
                                                    directory
                                                            .getFileName()
                                                            .toString()
                                                            .substring("chk-".length()))))
                    .orElseThrow(() -> new AssertionError("No completed checkpoint found"));
        }
    }

    private static List<Path> completedCheckpointHaEntries(Path haStorage) throws IOException {
        try (Stream<Path> files = Files.walk(haStorage)) {
            return files.filter(Files::isRegularFile)
                    .filter(
                            file ->
                                    file.getFileName()
                                            .toString()
                                            .startsWith(
                                                    ZooKeeperUtils.HA_STORAGE_COMPLETED_CHECKPOINT))
                    .collect(Collectors.toList());
        }
    }

    private static long size(Path file) {
        try {
            return Files.size(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

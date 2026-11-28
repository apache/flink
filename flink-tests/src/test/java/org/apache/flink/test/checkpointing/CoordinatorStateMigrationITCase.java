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

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV6Serializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV7Serializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.filesOtherThanMetadata;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.readMetadataVersion;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.ENUMERATOR_STATE;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.waitForRestoredEnumeratorState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests restoring the state of a FLIP-27 source coordinator from savepoints of earlier Flink
 * versions, with the option to store coordinator state in its own file enabled and disabled. The
 * savepoints keep the state inline in the checkpoint metadata, and from 2.5 on, each version also
 * has a savepoint with the state in its own file. The existing savepoint migration tests use the
 * legacy {@code SourceFunction}, which has no coordinator.
 */
@ExtendWith(ParameterizedTestExtension.class)
class CoordinatorStateMigrationITCase implements MigrationTest {

    /** The first release with FLIP-619, from which a savepoint can have the state in a file. */
    private static final FlinkVersion FIRST_VERSION_WITH_STATE_IN_FILE = FlinkVersion.v2_5;

    @Parameters(name = "Flink {0}, coordinator state in file: {1}, savepoint with it in file: {2}")
    private static Collection<Object[]> parameters() {
        final List<Object[]> parameters = new ArrayList<>();
        final FlinkVersion mostRecent = MigrationTest.getMostRecentlyPublishedVersion();
        for (FlinkVersion version : FlinkVersion.rangeOf(FlinkVersion.v1_20, mostRecent)) {
            parameters.add(new Object[] {version, true, false});
            parameters.add(new Object[] {version, false, false});
        }
        // these versions also have a savepoint with the coordinator state in its own file
        for (FlinkVersion version :
                FlinkVersion.rangeOf(FIRST_VERSION_WITH_STATE_IN_FILE, mostRecent)) {
            parameters.add(new Object[] {version, true, true});
            parameters.add(new Object[] {version, false, true});
        }
        return parameters;
    }

    @Parameter private FlinkVersion flinkVersion;

    @Parameter(1)
    private boolean coordinatorStateInFile;

    @Parameter(2)
    private boolean savepointWithStateInFile;

    @TempDir private Path tmp;

    @BeforeEach
    void resetSource() {
        EnumeratorStateTestSource.restoredEnumeratorState = new CompletableFuture<>();
        EnumeratorStateTestSource.FAIL_ENUMERATOR_AFTER_CHECKPOINT.set(false);
    }

    @SnapshotsGenerator
    public void generateSnapshots(FlinkVersion targetVersion) throws Exception {
        generateSnapshot(targetVersion, false);
        if (!FIRST_VERSION_WITH_STATE_IN_FILE.isNewerVersionThan(targetVersion)) {
            generateSnapshot(targetVersion, true);
        }
    }

    private static void generateSnapshot(FlinkVersion targetVersion, boolean stateInFile)
            throws Exception {
        final Path target =
                Paths.get("src/test/resources", getSnapshotPath(targetVersion, stateInFile));
        if (Files.exists(target)) {
            // keep a savepoint that its own version wrote, such as the 2.4 one from release-2.4
            return;
        }
        final Path directory = Files.createTempDirectory("coordinator-state-migration");
        try {
            final Configuration configuration = createConfiguration(directory);
            configuration.set(CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, stateInFile);
            final Path savepoint = runAndStopWithSavepoint(configuration, null);
            Files.createDirectories(target);
            try (Stream<Path> files = Files.list(savepoint)) {
                for (Path file : files.collect(Collectors.toList())) {
                    Files.copy(file, target.resolve(file.getFileName().toString()));
                }
            }
        } finally {
            FileUtils.deleteDirectory(directory.toFile());
        }
    }

    @TestTemplate
    void testRestoreSavepointOfEarlierVersion() throws Exception {
        final URL resource =
                CoordinatorStateMigrationITCase.class
                        .getClassLoader()
                        .getResource(getSnapshotPath(flinkVersion, savepointWithStateInFile));
        assertThat(resource).as("savepoint of Flink %s", flinkVersion).isNotNull();
        final Path savepoint = Paths.get(resource.toURI());
        if (savepointWithStateInFile) {
            assertThat(readMetadataVersion(savepoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(savepoint)).hasSize(1);
        } else {
            assertThat(readMetadataVersion(savepoint)).isLessThan(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(savepoint)).isEmpty();
        }

        final Configuration configuration = createConfiguration(tmp);
        configuration.set(
                CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, coordinatorStateInFile);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());

        final Path nextSavepoint = runAndStopWithSavepoint(configuration, savepoint);

        if (coordinatorStateInFile) {
            assertThat(readMetadataVersion(nextSavepoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(nextSavepoint)).hasSize(1);
        } else {
            assertThat(readMetadataVersion(nextSavepoint)).isEqualTo(MetadataV6Serializer.VERSION);
            assertThat(filesOtherThanMetadata(nextSavepoint)).isEmpty();
        }
    }

    private static String getSnapshotPath(FlinkVersion version, boolean stateInFile) {
        return "coordinator-state-migration-itcase-flink"
                + version
                + (stateInFile ? "-in-file" : "")
                + "-savepoint";
    }

    private static Configuration createConfiguration(Path directory) {
        final Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                directory.resolve("checkpoints").toUri().toString());
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                directory.resolve("savepoints").toUri().toString());
        configuration.set(RestOptions.BIND_PORT, "0");
        return configuration;
    }

    /**
     * Runs the job until it has completed a checkpoint, and stops it with a savepoint. If the job
     * is restored from a savepoint, checks the state that its enumerator is restored with.
     */
    private static Path runAndStopWithSavepoint(
            Configuration configuration, @Nullable Path restoreFrom) throws Exception {
        try (MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(1)
                                .build())) {
            miniCluster.start();
            final JobGraph jobGraph = createJobGraph(configuration);
            if (restoreFrom != null) {
                jobGraph.setSavepointRestoreSettings(
                        SavepointRestoreSettings.forPath(restoreFrom.toUri().toString(), false));
            }
            final JobID jobId = jobGraph.getJobID();
            miniCluster.submitJob(jobGraph).get();
            if (restoreFrom != null) {
                assertThat(waitForRestoredEnumeratorState(miniCluster, jobId))
                        .isEqualTo(ENUMERATOR_STATE);
            }
            // the source must be running before a savepoint can be taken
            waitForCheckpoint(jobId, miniCluster, 1);
            final String savepoint =
                    miniCluster
                            .stopWithSavepoint(
                                    jobId,
                                    configuration.get(CheckpointingOptions.SAVEPOINT_DIRECTORY),
                                    false,
                                    SavepointFormatType.CANONICAL)
                            .get();
            return Paths.get(URI.create(savepoint));
        }
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
                .uid("enumerator-state-source")
                .sinkTo(new DiscardingSink<>())
                .uid("discarding-sink");
        return env.getStreamGraph().getJobGraph();
    }
}

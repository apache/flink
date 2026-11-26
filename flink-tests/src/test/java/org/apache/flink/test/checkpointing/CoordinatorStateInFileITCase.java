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
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV6Serializer;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV7Serializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.FIXED_DELAY;
import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.filesOtherThanMetadata;
import static org.apache.flink.test.checkpointing.CoordinatorStateTestUtils.readMetadataVersion;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.ENUMERATOR_STATE;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.hasCompletedCheckpointAfterRestore;
import static org.apache.flink.test.checkpointing.EnumeratorStateTestSource.waitForRestoredEnumeratorState;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests storing source coordinator state in its own file and restoring it from a relocated
 * savepoint, from a retained checkpoint in claim and no-claim mode, after a global failover with
 * either scheduler and a rescale of the adaptive scheduler, and after the option is turned off or
 * on, and that a region failover does not read it. Also tests the inline threshold, disposing a
 * savepoint, and a missing coordinator state file, without and with restarts.
 */
class CoordinatorStateInFileITCase {

    @TempDir private Path tmp;

    @RegisterExtension
    final LoggerAuditingExtension coordinatorLog =
            new LoggerAuditingExtension(RecreateOnResetOperatorCoordinator.class, Level.INFO);

    @BeforeEach
    void resetSource() {
        EnumeratorStateTestSource.restoredEnumeratorState = new CompletableFuture<>();
        EnumeratorStateTestSource.FAIL_ENUMERATOR_AFTER_CHECKPOINT.set(false);
        EnumeratorStateTestSource.FAIL_READER_AFTER_CHECKPOINT.set(false);
    }

    @Test
    void testRestoreFromRelocatedSavepoint() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            final Path relocated = Files.move(savepoint, tmp.resolve("relocated"));

            assertThat(readMetadataVersion(relocated)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(relocated)).hasSize(1);

            final JobGraph jobGraph = createJobGraph(configuration, relocated);
            miniCluster.submitJob(jobGraph).get();
            assertThat(waitForRestoredEnumeratorState(miniCluster, jobGraph.getJobID()))
                    .isEqualTo(ENUMERATOR_STATE);
        }
    }

    @ParameterizedTest(name = "scheduler = {0}")
    @EnumSource(
            value = JobManagerOptions.SchedulerType.class,
            names = {"Default", "Adaptive"})
    void testRestoreAfterGlobalFailover(JobManagerOptions.SchedulerType scheduler)
            throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        // keep the restored checkpoint, so that its files can be checked
        configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1000);
        EnumeratorStateTestSource.FAIL_ENUMERATOR_AFTER_CHECKPOINT.set(true);

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final JobGraph jobGraph = createJobGraph(configuration, null);
            final JobID jobId = jobGraph.getJobID();
            miniCluster.submitJob(jobGraph).get();

            assertThat(waitForRestoredEnumeratorState(miniCluster, jobId))
                    .isEqualTo(ENUMERATOR_STATE);

            // the checkpoint statistics that the job reports can lag behind the restore
            final AtomicReference<RestoredCheckpointStats> restored = new AtomicReference<>();
            waitUntilCondition(
                    () -> {
                        restored.set(
                                miniCluster
                                        .getExecutionGraph(jobId)
                                        .get()
                                        .getCheckpointStatsSnapshot()
                                        .getLatestRestoredCheckpoint());
                        return restored.get() != null;
                    });
            final Path restoredCheckpoint = Paths.get(URI.create(restored.get().getExternalPath()));
            assertThat(readMetadataVersion(restoredCheckpoint))
                    .isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(restoredCheckpoint)).hasSize(1);
        }
    }

    @ParameterizedTest(name = "claim mode = {0}")
    @EnumSource(
            value = RecoveryClaimMode.class,
            names = {"CLAIM", "NO_CLAIM"})
    void testRestoreFromARetainedCheckpoint(RecoveryClaimMode claimMode) throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        configuration.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final JobGraph original = createJobGraph(configuration, null);
            miniCluster.submitJob(original).get();
            waitForCheckpoint(original.getJobID(), miniCluster, 1);
            miniCluster.cancelJob(original.getJobID()).get();
            miniCluster.requestJobResult(original.getJobID()).get();
            final Path checkpoint =
                    Paths.get(
                            URI.create(
                                    getLatestCompletedCheckpointPath(
                                                    original.getJobID(), miniCluster)
                                            .get()));
            assertThat(readMetadataVersion(checkpoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(checkpoint)).hasSize(1);

            final JobGraph restored = createJobGraph(configuration, null);
            restored.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(
                            checkpoint.toUri().toString(), false, claimMode));
            miniCluster.submitJob(restored).get();

            assertThat(waitForRestoredEnumeratorState(miniCluster, restored.getJobID()))
                    .isEqualTo(ENUMERATOR_STATE);
            if (claimMode == RecoveryClaimMode.CLAIM) {
                // the job owns the claimed checkpoint, and deletes it with its file once a later
                // checkpoint subsumes it
                waitUntilCondition(() -> !Files.exists(checkpoint));
            } else {
                // the job does not own the checkpoint, which keeps its file
                waitForCheckpoint(restored.getJobID(), miniCluster, 2);
                assertThat(filesOtherThanMetadata(checkpoint)).hasSize(1);
            }
        }
    }

    @Test
    void testRegionFailoverDoesNotReadTheCoordinatorStateFile() throws Exception {
        final Configuration configuration = createConfiguration();
        // the default scheduler restarts only the region of a failed task, and does not reset
        // the coordinators
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Default);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));
        // keep the checkpoints, so that the files of the latest can be checked
        configuration.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1000);
        EnumeratorStateTestSource.FAIL_READER_AFTER_CHECKPOINT.set(true);

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final JobGraph jobGraph = createJobGraph(configuration, null);
            final JobID jobId = jobGraph.getJobID();
            miniCluster.submitJob(jobGraph).get();

            // the task is restored from a checkpoint, and the job then completes another one,
            // unless the coordinator is reset, which the assertions below report
            waitUntilCondition(
                    () ->
                            EnumeratorStateTestSource.restoredEnumeratorState.isDone()
                                    || hasCompletedCheckpointAfterRestore(
                                            miniCluster
                                                    .getExecutionGraph(jobId)
                                                    .get()
                                                    .getCheckpointStatsSnapshot()));

            assertThat(EnumeratorStateTestSource.restoredEnumeratorState.isDone())
                    .as("the coordinator was reset")
                    .isFalse();
            assertThat(coordinatorLog.getMessages())
                    .noneMatch(message -> message.startsWith("Reading the coordinator state"));
            final Path latest =
                    Paths.get(
                            URI.create(getLatestCompletedCheckpointPath(jobId, miniCluster).get()));
            assertThat(readMetadataVersion(latest)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(latest)).hasSize(1);
        }
    }

    @Test
    void testRestoreAfterARescaleOfTheAdaptiveScheduler() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());

        try (MiniCluster miniCluster = startMiniCluster(configuration, 2)) {
            final JobGraph jobGraph = createJobGraph(configuration, null);
            final JobID jobId = jobGraph.getJobID();
            miniCluster.submitJob(jobGraph).get();
            // the rescaled job is restored from the latest checkpoint
            waitForCheckpoint(jobId, miniCluster, 1);

            final JobResourceRequirements.Builder requirements =
                    JobResourceRequirements.newBuilder();
            for (JobVertex vertex : jobGraph.getVertices()) {
                requirements.setParallelismForJobVertex(vertex.getID(), 2, 2);
            }
            miniCluster.updateJobResourceRequirements(jobId, requirements.build()).get();

            assertThat(waitForRestoredEnumeratorState(miniCluster, jobId))
                    .isEqualTo(ENUMERATOR_STATE);
            assertThat(coordinatorLog.getMessages())
                    .anyMatch(message -> message.startsWith("Read "));
            assertThat(miniCluster.getExecutionGraph(jobId).get().getAllVertices().values())
                    .allSatisfy(vertex -> assertThat(vertex.getParallelism()).isEqualTo(2));
        }
    }

    @Test
    void testRestoreWithTheOptionDisabled() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        final Configuration disabled = new Configuration(configuration);
        disabled.set(CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, false);

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            assertThat(readMetadataVersion(savepoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(savepoint)).hasSize(1);

            final Path nextSavepoint = takeSavepoint(miniCluster, disabled, savepoint);

            assertThat(readMetadataVersion(nextSavepoint)).isEqualTo(MetadataV6Serializer.VERSION);
            assertThat(filesOtherThanMetadata(nextSavepoint)).isEmpty();
        }
    }

    @Test
    void testRestoreInlineStateWithTheOptionEnabled() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        final Configuration disabled = new Configuration(configuration);
        disabled.set(CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, false);

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, disabled, null);
            assertThat(readMetadataVersion(savepoint)).isEqualTo(MetadataV6Serializer.VERSION);
            assertThat(filesOtherThanMetadata(savepoint)).isEmpty();

            final Path nextSavepoint = takeSavepoint(miniCluster, configuration, savepoint);

            assertThat(readMetadataVersion(nextSavepoint)).isEqualTo(MetadataV7Serializer.VERSION);
            assertThat(filesOtherThanMetadata(nextSavepoint)).hasSize(1);
        }
    }

    @Test
    void testMissingCoordinatorStateFileFailsTheJob() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            for (Path file : filesOtherThanMetadata(savepoint)) {
                Files.delete(file);
            }

            final JobGraph jobGraph = createJobGraph(configuration, savepoint);
            miniCluster.submitJob(jobGraph).get();
            final JobResult jobResult = miniCluster.requestJobResult(jobGraph.getJobID()).get();

            assertThat(jobResult.getSerializedThrowable()).isPresent();
            final Throwable failure =
                    jobResult
                            .getSerializedThrowable()
                            .get()
                            .deserializeError(getClass().getClassLoader());
            assertThat(ExceptionUtils.findThrowable(failure, FileNotFoundException.class))
                    .isPresent();
        }
    }

    @ParameterizedTest(name = "inline threshold = {0}")
    @CsvSource({"128kb, false", "1kb, true"})
    void testInlineThresholdDecidesWhetherTheStateIsStoredInItsOwnFile(
            String threshold, boolean inItsOwnFile) throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        configuration.set(
                CheckpointingOptions.COORDINATOR_STATE_INLINE_THRESHOLD,
                MemorySize.parse(threshold));

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            assertThat(readMetadataVersion(savepoint))
                    .isEqualTo(
                            inItsOwnFile
                                    ? MetadataV7Serializer.VERSION
                                    : MetadataV6Serializer.VERSION);
            assertThat(filesOtherThanMetadata(savepoint)).hasSize(inItsOwnFile ? 1 : 0);

            takeSavepoint(miniCluster, configuration, savepoint);
        }
    }

    @Test
    void testDisposingASavepointDeletesItsCoordinatorStateFile() throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            final List<Path> coordinatorStateFiles = filesOtherThanMetadata(savepoint);
            assertThat(coordinatorStateFiles).hasSize(1);

            miniCluster.disposeSavepoint(savepoint.toUri().toString()).get();

            assertThat(coordinatorStateFiles.get(0)).doesNotExist();
            assertThat(savepoint.resolve(METADATA_FILE_NAME)).doesNotExist();
        }
    }

    @ParameterizedTest(name = "scheduler = {0}")
    @EnumSource(
            value = JobManagerOptions.SchedulerType.class,
            names = {"Default", "Adaptive"})
    void testMissingCoordinatorStateFileFailsEveryRestartOfTheRestartStrategy(
            JobManagerOptions.SchedulerType scheduler) throws Exception {
        final Configuration configuration = createConfiguration();
        configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 2);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(10));

        try (MiniCluster miniCluster = startMiniCluster(configuration)) {
            final Path savepoint = takeSavepoint(miniCluster, configuration, null);
            for (Path file : filesOtherThanMetadata(savepoint)) {
                Files.delete(file);
            }

            final JobGraph jobGraph = createJobGraph(configuration, savepoint);
            miniCluster.submitJob(jobGraph).get();
            assertThat(
                            miniCluster
                                    .requestJobResult(jobGraph.getJobID())
                                    .get()
                                    .getSerializedThrowable())
                    .isPresent();

            final List<Throwable> failures = new ArrayList<>();
            for (RootExceptionHistoryEntry entry :
                    miniCluster
                            .getExecutionGraphInfo(jobGraph.getJobID())
                            .get()
                            .getExceptionHistory()) {
                failures.add(entry.getException().deserializeError(getClass().getClassLoader()));
            }
            // the first attempt and both restarts fail to read the file
            assertThat(failures)
                    .hasSize(3)
                    .allSatisfy(
                            failure ->
                                    assertThat(
                                                    ExceptionUtils.findThrowable(
                                                            failure, FileNotFoundException.class))
                                            .isPresent());
        }
    }

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                tmp.resolve("checkpoints").toUri().toString());
        configuration.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                tmp.resolve("savepoints").toUri().toString());
        configuration.set(CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, true);
        configuration.set(RestOptions.BIND_PORT, "0");
        return configuration;
    }

    private static MiniCluster startMiniCluster(Configuration configuration) throws Exception {
        return startMiniCluster(configuration, 1);
    }

    private static MiniCluster startMiniCluster(Configuration configuration, int slots)
            throws Exception {
        final MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(slots)
                                .build());
        miniCluster.start();
        return miniCluster;
    }

    /**
     * Runs the job until it has completed a checkpoint, and stops it with a savepoint. If the job
     * is restored from a savepoint, checks the state that its enumerator is restored with.
     */
    private static Path takeSavepoint(
            MiniCluster miniCluster, Configuration configuration, @Nullable Path restoreFrom)
            throws Exception {
        final JobGraph jobGraph = createJobGraph(configuration, restoreFrom);
        final JobID jobId = jobGraph.getJobID();
        EnumeratorStateTestSource.restoredEnumeratorState = new CompletableFuture<>();
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

    private static JobGraph createJobGraph(Configuration configuration, @Nullable Path savepoint) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.fromSource(
                        new EnumeratorStateTestSource(),
                        WatermarkStrategy.noWatermarks(),
                        "enumerator-state-source")
                .sinkTo(new DiscardingSink<>());
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        if (savepoint != null) {
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepoint.toUri().toString(), false));
        }
        return jobGraph;
    }
}

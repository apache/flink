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

package org.apache.flink.test.state.operator.restore;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.test.util.MigrationTest;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Abstract class to verify that it is possible to migrate a savepoint across upgraded Flink
 * versions and that the topology can be modified from that point on.
 *
 * <p>The verification is done in 2 Steps: Step 1: Migrate the job to the newer version by
 * submitting the same job used for the old version savepoint, and create a new savepoint. Step 2:
 * Modify the job topology, and restore from the savepoint created in step 1.
 *
 * <p>The savepoint _metadata file for the current branch is stored in the savepointPath in {@link
 * AbstractOperatorRestoreTestBase#migrateJob}.
 */
public abstract class AbstractOperatorRestoreTestBase extends TestLogger implements MigrationTest {

    private final FlinkVersion flinkVersion;

    @Parameterized.Parameters(name = "Migrate Savepoint: {0}")
    public static Collection<FlinkVersion> parameters() {
        return FlinkVersion.rangeOf(
                FlinkVersion.v1_8, MigrationTest.getMostRecentlyPublishedVersion());
    }

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final int NUM_TMS = 1;
    private static final int NUM_SLOTS_PER_TM = 4;
    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(10000L);
    private static final Pattern PATTERN_CANCEL_WITH_SAVEPOINT_TOLERATED_EXCEPTIONS =
            Pattern.compile(
                    Stream.of(
                                    "was not running",
                                    CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING
                                            .message(),
                                    CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY
                                            .message(),
                                    CheckpointFailureReason
                                            .CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER
                                            .message())
                            .map(AbstractOperatorRestoreTestBase::escapeRegexCharacters)
                            .collect(Collectors.joining(")|(", "(", ")")));

    @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule
    public final MiniClusterWithClientResource cluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(NUM_TMS)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
                            .build());

    private final ScheduledExecutor scheduledExecutor =
            new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor());

    protected AbstractOperatorRestoreTestBase(FlinkVersion flinkVersion) {
        this.flinkVersion = flinkVersion;
    }

    protected void internalGenerateSnapshots(FlinkVersion targetVersion) throws Exception {
        ClusterClient<?> clusterClient = cluster.getClusterClient();
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);

        // Submit job with old version savepoint and create a migrated savepoint in the new version.
        // Any old version is ok, and we choose 1.8 directly.
        String savepointPath = migrateJob(FlinkVersion.v1_8, clusterClient, deadline);

        Path targetPath = getSavepointPath(targetVersion);
        Files.createDirectories(targetPath);

        // copy the savepoint to the given directory
        try (DirectoryStream<Path> childrenFiles =
                Files.newDirectoryStream(Paths.get(new URI(savepointPath)))) {
            for (Path filePath : childrenFiles) {
                Files.copy(
                        filePath,
                        Paths.get(targetPath.toString(), filePath.getFileName().toString()));
            }
        }
    }

    @Test
    public void testMigrationAndRestore() throws Throwable {
        ClusterClient<?> clusterClient = cluster.getClusterClient();
        final Deadline deadline = Deadline.now().plus(TEST_TIMEOUT);

        // submit job with old version savepoint and create a migrated savepoint in the new version
        String savepointPath = migrateJob(flinkVersion, clusterClient, deadline);

        // restore from migrated new version savepoint
        restoreJob(clusterClient, deadline, savepointPath);
    }

    private String migrateJob(
            FlinkVersion flinkVersion, ClusterClient<?> clusterClient, Deadline deadline)
            throws Exception {

        URL savepointResource =
                AbstractOperatorRestoreTestBase.class
                        .getClassLoader()
                        .getResource("operatorstate/" + getMigrationSavepointName(flinkVersion));
        if (savepointResource == null) {
            throw new IllegalArgumentException("Savepoint file does not exist.");
        }
        JobGraph jobToMigrate = createJobGraph(ExecutionMode.MIGRATE);
        jobToMigrate.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointResource.getFile()));

        assertNotNull(jobToMigrate.getJobID());

        clusterClient.submitJob(jobToMigrate).get();

        CompletableFuture<JobStatus> jobRunningFuture =
                FutureUtils.retrySuccessfulWithDelay(
                        () -> clusterClient.getJobStatus(jobToMigrate.getJobID()),
                        Duration.ofMillis(50),
                        deadline,
                        (jobStatus) -> jobStatus == JobStatus.RUNNING,
                        scheduledExecutor);
        assertEquals(
                JobStatus.RUNNING,
                jobRunningFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

        // Trigger savepoint
        File targetDirectory = tmpFolder.newFolder();
        String savepointPath = null;

        // FLINK-6918: Retry cancel with savepoint message in case that StreamTasks were not running
        // TODO: The retry logic should be removed once the StreamTask lifecycle has been fixed (see
        // FLINK-4714)
        while (deadline.hasTimeLeft() && savepointPath == null) {
            try {
                savepointPath =
                        clusterClient
                                .cancelWithSavepoint(
                                        jobToMigrate.getJobID(),
                                        targetDirectory.getAbsolutePath(),
                                        SavepointFormatType.CANONICAL)
                                .get();
            } catch (Exception e) {
                String exceptionString = ExceptionUtils.stringifyException(e);
                if (!PATTERN_CANCEL_WITH_SAVEPOINT_TOLERATED_EXCEPTIONS
                        .matcher(exceptionString)
                        .find()) {
                    throw e;
                }
            }
        }

        assertNotNull("Could not take savepoint.", savepointPath);

        CompletableFuture<JobStatus> jobCanceledFuture =
                FutureUtils.retrySuccessfulWithDelay(
                        () -> clusterClient.getJobStatus(jobToMigrate.getJobID()),
                        Duration.ofMillis(50),
                        deadline,
                        (jobStatus) -> jobStatus == JobStatus.CANCELED,
                        scheduledExecutor);
        assertEquals(
                JobStatus.CANCELED,
                jobCanceledFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

        return savepointPath;
    }

    private void restoreJob(ClusterClient<?> clusterClient, Deadline deadline, String savepointPath)
            throws Exception {
        JobGraph jobToRestore = createJobGraph(ExecutionMode.RESTORE);
        jobToRestore.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, true));

        assertNotNull("Job doesn't have a JobID.", jobToRestore.getJobID());

        clusterClient.submitJob(jobToRestore).get();

        CompletableFuture<JobStatus> jobStatusFuture =
                FutureUtils.retrySuccessfulWithDelay(
                        () -> clusterClient.getJobStatus(jobToRestore.getJobID()),
                        Duration.ofMillis(50),
                        deadline,
                        (jobStatus) -> jobStatus == JobStatus.FINISHED,
                        scheduledExecutor);
        assertEquals(
                JobStatus.FINISHED,
                jobStatusFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));
    }

    private JobGraph createJobGraph(ExecutionMode mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.setStateBackend((StateBackend) new MemoryStateBackend());

        switch (mode) {
            case MIGRATE:
                createMigrationJob(env);
                break;
            case RESTORE:
                createRestoredJob(env);
                break;
        }

        return StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph());
    }

    private Path getSavepointPath(FlinkVersion version) {
        return Paths.get(
                System.getProperty("user.dir"),
                "src/test/resources/operatorstate/" + getMigrationSavepointName(version));
    }

    /**
     * Recreates the job used to create the new version savepoint.
     *
     * @param env StreamExecutionEnvironment to use
     */
    protected abstract void createMigrationJob(StreamExecutionEnvironment env);

    /**
     * Creates a modified version of the job used to create the new version savepoint.
     *
     * @param env StreamExecutionEnvironment to use
     */
    protected abstract void createRestoredJob(StreamExecutionEnvironment env);

    /**
     * Returns the name of the savepoint directory to use, relative to "resources/operatorstate".
     *
     * @return savepoint directory to use
     */
    protected abstract String getMigrationSavepointName(FlinkVersion version);

    private static String escapeRegexCharacters(String string) {
        return string.replaceAll("\\(", "\\\\(").replaceAll("\\)", "\\\\)");
    }
}

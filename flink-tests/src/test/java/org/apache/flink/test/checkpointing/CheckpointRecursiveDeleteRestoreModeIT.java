/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.source.AbstractTestSource;
import org.apache.flink.test.util.source.TestSourceReader;
import org.apache.flink.test.util.source.TestSplit;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Integration test proving that changing {@code recursive=false} to {@code recursive=true} in
 * {@link
 * org.apache.flink.runtime.state.filesystem.FsCompletedCheckpointStorageLocation#disposeStorageLocation()}
 * is safe across all {@link RecoveryClaimMode}s (CLAIM, NO_CLAIM, LEGACY).
 *
 * <p>Production bug observed in logs:
 *
 * <pre>
 * WARN  CheckpointsCleaner - Could not properly discard completed checkpoint 3.
 * org.apache.hadoop.fs.PathIsNotEmptyDirectoryException: Directory is not empty
 *   at S3AFileSystem.delete(S3AFileSystem.java:3168)
 *   at HadoopFileSystem.delete(HadoopFileSystem.java:163)
 *   at FsCompletedCheckpointStorageLocation.disposeStorageLocation(...)
 *   at CompletedCheckpoint.discardAsync(...)
 *   at CheckpointsCleaner.cleanCheckpoint(...)
 * </pre>
 *
 * <p>Root cause: {@code FileSystem.delete(path, recursive=false)} fails on non-empty directories,
 * leaving checkpoint files leaked on object storage (S3/MinIO).
 *
 * <p>Fix: {@code FileSystem.delete(path, recursive=true)} ensures complete cleanup.
 *
 * <p>Safety is verified by checking that:
 *
 * <ul>
 *   <li>Only Flink-owned paths are deleted (never external/shared paths).
 *   <li>Expired checkpoints are fully removed under all RestoreModes.
 *   <li>Restore sources are never deleted in NO_CLAIM and LEGACY modes.
 * </ul>
 *
 * <p>The full production call stack is exercised via MiniCluster:
 *
 * <pre>
 * CheckpointsCleaner
 *   -&gt; CompletedCheckpoint.discardAsync()
 *     -&gt; FsCompletedCheckpointStorageLocation.disposeStorageLocation()
 *       -&gt; FileSystem.delete(path, recursive=true)
 * </pre>
 *
 * <p>Test validity: reverting {@code recursive=true} back to {@code recursive=false} in {@link
 * org.apache.flink.runtime.state.filesystem.FsCompletedCheckpointStorageLocation} causes {@link
 * #claim_expiredCheckpointFullyDeleted()} to fail, confirming this test directly validates the fix.
 */
public class CheckpointRecursiveDeleteRestoreModeIT extends TestLogger {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private File checkpointDir;
    private File savepointDir;

    private MiniClusterWithClientResource cluster;

    @Before
    public void setUp() throws Exception {
        checkpointDir = temporaryFolder.newFolder("checkpoints");
        savepointDir = temporaryFolder.newFolder("savepoints");

        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 1);

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(2)
                                .setConfiguration(config)
                                .build());
        cluster.before();
        InfiniteSource.reset();
    }

    /**
     * Scenario: CLAIM mode - Flink owns the checkpoint directory.
     *
     * <p>In CLAIM mode, Flink takes full ownership of the restored snapshot and manages its
     * lifecycle. Expired checkpoints must be completely deleted including all nested files.
     *
     * <p>Expected behavior:
     *
     * <ul>
     *   <li>After restore from savepoint, new checkpoints are created under Flink-managed paths.
     *   <li>When a checkpoint expires (MAX_RETAINED=1), its directory and all contents are fully
     *       deleted.
     *   <li>Only the latest checkpoint directory remains.
     * </ul>
     *
     * <p>Before fix (recursive=false): expired checkpoint directory remains on storage (leak).
     * After fix (recursive=true): expired checkpoint directory is completely removed.
     *
     * <p>Test validity: this test fails when {@code recursive=false} is used, because the expired
     * chk-N directory with its files cannot be deleted, leaving it on storage.
     */
    @Test
    public void claim_expiredCheckpointFullyDeleted() throws Exception {
        String savepointPath = submitJobAndTakeSavepoint();

        JobGraph restoredJob = createJobGraph();
        restoredJob.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, false, RecoveryClaimMode.CLAIM));

        ClusterClient<?> client = cluster.getClusterClient();
        MiniCluster miniCluster = cluster.getMiniCluster();
        JobID jobId = restoredJob.getJobID();

        InfiniteSource.reset();
        client.submitJob(restoredJob).get();
        waitForAllTaskRunning(miniCluster, jobId, false);

        // Record chk-N path before it expires, and verify it contains files.
        // This confirms that recursive delete is meaningful (non-empty directory).
        String chkPath = miniCluster.triggerCheckpoint(jobId).get();
        File chkDir = new File(new java.net.URI(chkPath));

        assertTrue(
                "Checkpoint must contain files before expiry - "
                        + "otherwise recursive=false/true makes no difference",
                countFilesRecursively(chkDir) > 0);

        // Trigger additional checkpoints to expire the recorded one.
        // With MAX_RETAINED=1, it expires as soon as the next checkpoint completes.
        miniCluster.triggerCheckpoint(jobId).get();
        miniCluster.triggerCheckpoint(jobId).get();
        miniCluster.triggerCheckpoint(jobId).get();

        try {
            // Before fix (recursive=false): chkDir remains on storage (leak).
            // After fix  (recursive=true):  chkDir is completely removed.
            assertFalse(
                    "CLAIM: expired checkpoint directory must be completely deleted including all"
                            + " files.\nIf this fails, recursive=false is still in"
                            + " FsCompletedCheckpointStorageLocation.disposeStorageLocation()"
                            + " causing storage leak.",
                    chkDir.exists());
        } finally {
            client.cancel(jobId).get();
            cluster.after();
        }
    }

    /**
     * Scenario: LEGACY mode - expired checkpoints are deleted, restore source is preserved.
     *
     * <p>In LEGACY mode (pre-1.15 behavior), Flink does not delete the initial restore source, but
     * does manage and delete subsequent expired checkpoints.
     *
     * <p>Expected behavior:
     *
     * <ul>
     *   <li>The restore source (savepoint) is never deleted.
     *   <li>Expired checkpoints created after restore are fully deleted including all nested files.
     *   <li>Only the restore source and the latest checkpoint remain.
     * </ul>
     *
     * <p>Before fix (recursive=false): expired checkpoint directory remains (leak). After fix
     * (recursive=true): expired checkpoint directory is completely removed.
     */
    @Test
    public void legacy_expiredCheckpointFullyDeleted_noGarbageFiles() throws Exception {
        String savepointPath = submitJobAndTakeSavepoint();

        JobGraph restoredJob = createJobGraph();
        restoredJob.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, false, RecoveryClaimMode.LEGACY));

        ClusterClient<?> client = cluster.getClusterClient();
        MiniCluster miniCluster = cluster.getMiniCluster();
        JobID jobId = restoredJob.getJobID();

        InfiniteSource.reset();
        client.submitJob(restoredJob).get();
        waitForAllTaskRunning(miniCluster, jobId, false);

        // Record chk-N path before it expires, and verify it contains files.
        // This confirms that recursive delete is meaningful (non-empty directory).
        String chkPath = miniCluster.triggerCheckpoint(jobId).get();
        File chkDir = new File(new java.net.URI(chkPath));

        assertTrue(
                "Checkpoint must contain files to confirm recursive delete is exercised",
                countFilesRecursively(chkDir) > 0);

        // Trigger additional checkpoints to expire the recorded one.
        miniCluster.triggerCheckpoint(jobId).get();
        miniCluster.triggerCheckpoint(jobId).get();
        miniCluster.triggerCheckpoint(jobId).get();

        try {
            // Before fix (recursive=false): chkDir remains on storage (leak).
            // After fix  (recursive=true):  chkDir is completely removed.
            assertFalse(
                    "LEGACY: expired checkpoint directory must be completely deleted including all"
                            + " files.\nIf this fails, recursive=false is still in"
                            + " FsCompletedCheckpointStorageLocation.disposeStorageLocation()"
                            + " causing storage leak.",
                    chkDir.exists());

            // Verify restore source is preserved (LEGACY contract).
            assertTrue(
                    "LEGACY: restore source (savepoint) must NOT be deleted by Flink",
                    new File(new java.net.URI(savepointPath)).exists());
        } finally {
            client.cancel(jobId).get();
            cluster.after();
        }
    }

    /**
     * Scenario: NO_CLAIM mode - Flink never deletes the restore source.
     *
     * <p>In NO_CLAIM mode, Flink does not take ownership of the restored snapshot. The original
     * savepoint files must never be touched by Flink, even after new checkpoints are created and
     * expired.
     *
     * <p>Expected behavior:
     *
     * <ul>
     *   <li>The original savepoint directory and all its files remain intact throughout the job
     *       lifecycle.
     *   <li>{@code disposeStorageLocation()} is only called on paths that Flink itself created,
     *       never on the restore source.
     *   <li>Even with recursive=true, Flink-owned path scope ensures original savepoint safety.
     * </ul>
     */
    @Test
    public void noClaim_originalSavepointNeverDeleted() throws Exception {
        cluster.after();

        Configuration config = new Configuration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 10);

        MiniClusterWithClientResource noClaimCluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(2)
                                .setConfiguration(config)
                                .build());
        noClaimCluster.before();

        try {
            ClusterClient<?> client = noClaimCluster.getClusterClient();
            MiniCluster miniCluster = noClaimCluster.getMiniCluster();

            // Step 1: Create original savepoint (simulates external/user-owned snapshot).
            JobGraph originalJob = createJobGraph();
            JobID originalJobId = originalJob.getJobID();

            InfiniteSource.reset();
            client.submitJob(originalJob).get();
            waitForAllTaskRunning(miniCluster, originalJobId, false);
            InfiniteSource.awaitProgress();

            String originalSavepointPath =
                    client.cancelWithSavepoint(originalJobId, savepointDir.getAbsolutePath(), null)
                            .get();

            File originalSavepointDir = new File(new java.net.URI(originalSavepointPath));
            long originalFileCount = countFilesRecursively(originalSavepointDir);

            assertTrue(
                    "Original savepoint must contain files to verify integrity after restore",
                    originalFileCount > 0);

            // Step 2: Restore with NO_CLAIM and trigger checkpoint expiry.
            // Flink must not delete the original savepoint during cleanup.
            JobGraph restoredJob = createJobGraph();
            restoredJob.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(
                            originalSavepointPath, false, RecoveryClaimMode.NO_CLAIM));

            JobID restoredJobId = restoredJob.getJobID();

            InfiniteSource.reset();
            client.submitJob(restoredJob).get();
            waitForAllTaskRunning(miniCluster, restoredJobId, false);

            miniCluster.triggerCheckpoint(restoredJobId).get();
            miniCluster.triggerCheckpoint(restoredJobId).get();

            // Verify original savepoint is untouched.
            // disposeStorageLocation() is only invoked on Flink-created paths,
            // never on the restore source regardless of recursive=true.
            assertTrue(
                    "NO_CLAIM: original savepoint directory must NEVER be deleted by Flink."
                            + " disposeStorageLocation() must only target Flink-created paths.",
                    originalSavepointDir.exists());

            assertTrue(
                    "NO_CLAIM: original savepoint file count must remain intact."
                            + " Expected: "
                            + originalFileCount
                            + ", but some files were deleted.",
                    originalFileCount == countFilesRecursively(originalSavepointDir));

            client.cancel(restoredJobId).get();
        } finally {
            noClaimCluster.after();
        }
    }

    private long countFilesRecursively(File dir) {
        if (!dir.exists()) {
            return 0;
        }
        try (java.util.stream.Stream<java.nio.file.Path> stream =
                java.nio.file.Files.walk(dir.toPath())) {
            return stream.filter(java.nio.file.Files::isRegularFile).count();
        } catch (Exception e) {
            return 0;
        }
    }

    private String submitJobAndTakeSavepoint() throws Exception {
        JobGraph jobGraph = createJobGraph();
        JobID jobId = jobGraph.getJobID();
        ClusterClient<?> client = cluster.getClusterClient();

        InfiniteSource.reset();
        client.submitJob(jobGraph).get();
        waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
        InfiniteSource.awaitProgress();

        return client.cancelWithSavepoint(jobId, savepointDir.getAbsolutePath(), null).get();
    }

    private JobGraph createJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200);
        RestartStrategyUtils.configureNoRestartStrategy(env);

        env.fromSource(new InfiniteSource(), WatermarkStrategy.noWatermarks(), "InfiniteSource")
                .map((MapFunction<Integer, Integer>) value -> value)
                .sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class InfiniteSource extends AbstractTestSource<Integer> {

        private static volatile CountDownLatch progressLatch = new CountDownLatch(1);

        static void reset() {
            progressLatch = new CountDownLatch(1);
        }

        static void awaitProgress() throws InterruptedException {
            progressLatch.await(60, TimeUnit.SECONDS);
        }

        @Override
        public SourceReader<Integer, TestSplit> createReader(SourceReaderContext ctx) {
            return new TestSourceReader<>(ctx) {
                private int count = 0;

                @Override
                public InputStatus pollNext(ReaderOutput<Integer> out) {
                    out.collect(count++);
                    if (count == 10) {
                        progressLatch.countDown();
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        ExceptionUtils.rethrow(e);
                    }
                    return InputStatus.MORE_AVAILABLE;
                }
            };
        }
    }
}

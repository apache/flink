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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/** Test savepoint migration. */

/**
 * Base for testing snapshot migration. The base test supports snapshots types as defined in {@link
 * SnapshotType}.
 */
public abstract class SnapshotMigrationTestBase extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Rule public final MiniClusterWithClientResource miniClusterResource;

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotMigrationTestBase.class);

    protected static final int DEFAULT_PARALLELISM = 4;

    /**
     * Modes for migration test execution. This enum is supposed to serve as a switch between two
     * modes of test execution: 1) create snapshots and 2) verify snapshots:
     */
    public enum ExecutionMode {
        /** Create binary snapshot(s), i.e. run the checkpointing functions. */
        CREATE_SNAPSHOT,
        /** Verify snapshot(s), i.e, restore snapshot and check execution result. */
        VERIFY_SNAPSHOT
    }

    /** Types of snapshot supported by this base test. */
    public enum SnapshotType {
        /** Savepoints with Flink canonical format. */
        SAVEPOINT_CANONICAL,
        /** Savepoint with native format of respective state backend. */
        SAVEPOINT_NATIVE,
        /** Checkpoint. */
        CHECKPOINT
    }

    /**
     * A snapshot specification (immutable) for migration tests that consists of {@link
     * FlinkVersion} that the snapshot has been created with, {@link
     * SnapshotMigrationTestBase.SnapshotType}, and state backend type that the snapshot has been
     * ctreated from.
     */
    public static class SnapshotSpec implements Serializable {
        private final FlinkVersion flinkVersion;
        private final String stateBackendType;
        private final SnapshotMigrationTestBase.SnapshotType snapshotType;

        /**
         * Creates a {@link SnapshotSpec} with specified parameters.
         *
         * @param flinkVersion Specifies the {@link FlinkVersion}.
         * @param stateBackendType Specifies the state backend type.
         * @param snapshotType Specifies the {@link SnapshotMigrationTestBase.SnapshotType}.
         */
        public SnapshotSpec(
                FlinkVersion flinkVersion,
                String stateBackendType,
                SnapshotMigrationTestBase.SnapshotType snapshotType) {
            this.flinkVersion = flinkVersion;
            this.stateBackendType = stateBackendType;
            this.snapshotType = snapshotType;
        }

        /**
         * Gets the {@link FlinkVersion} that the snapshot has been created with.
         *
         * @return {@link FlinkVersion}
         */
        public FlinkVersion getFlinkVersion() {
            return flinkVersion;
        }

        /**
         * Gets the state backend type that the snapshot has been created from.
         *
         * @return State backend type.
         */
        public String getStateBackendType() {
            return stateBackendType;
        }

        /**
         * Gets the {@link SnapshotMigrationTestBase.SnapshotType}.
         *
         * @return {@link SnapshotMigrationTestBase.SnapshotType}
         */
        public SnapshotMigrationTestBase.SnapshotType getSnapshotType() {
            return snapshotType;
        }

        /**
         * Creates a collection of {@link SnapshotSpec} for a given collection of {@link
         * FlinkVersion} with the same parameters but different {@link FlinkVersion}.
         *
         * @param stateBackendType Specifies the state backend type.
         * @param snapshotType Specifies the snapshot type.
         * @param flinkVersions A collection of {@link FlinkVersion}.
         * @return A collection of {@link SnapshotSpec} that differ only by means of {@link
         *     FlinkVersion}.
         */
        public static Collection<SnapshotSpec> withVersions(
                String stateBackendType,
                SnapshotMigrationTestBase.SnapshotType snapshotType,
                Collection<FlinkVersion> flinkVersions) {
            List<SnapshotSpec> snapshotSpecCollection = new LinkedList<>();
            for (FlinkVersion version : flinkVersions) {
                snapshotSpecCollection.add(
                        new SnapshotSpec(version, stateBackendType, snapshotType));
            }
            return snapshotSpecCollection;
        }

        @Override
        public String toString() {
            StringBuilder str = new StringBuilder("flink" + flinkVersion);
            switch (stateBackendType) {
                case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
                    str.append("-rocksdb");
                    break;
                case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
                    // This is implicit due to backwards compatibility with legacy artifact names.
                    break;
                case StateBackendLoader.HASHMAP_STATE_BACKEND_NAME:
                    str.append("-hashmap");
                    break;
                default:
                    throw new UnsupportedOperationException("State backend type not supported.");
            }
            switch (snapshotType) {
                case SAVEPOINT_CANONICAL:
                    str.append("-savepoint");
                    // Canonical implicit due to backwards compatibility with legacy artifact names.
                    break;
                case SAVEPOINT_NATIVE:
                    str.append("-savepoint-native");
                    break;
                case CHECKPOINT:
                    str.append("-checkpoint");
                    break;
                default:
                    throw new UnsupportedOperationException("Snapshot type not supported.");
            }
            return str.toString();
        }
    }

    protected static String getResourceFilename(String filename) {
        ClassLoader cl = SnapshotMigrationTestBase.class.getClassLoader();
        URL resource = cl.getResource(filename);
        if (resource == null) {
            throw new NullPointerException("Missing snapshot resource.");
        }
        return resource.getFile();
    }

    protected SnapshotMigrationTestBase() throws Exception {
        miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(getConfiguration())
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                .build());
    }

    private Configuration getConfiguration() throws Exception {
        // Flink configuration
        final Configuration config = new Configuration();

        config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, DEFAULT_PARALLELISM);

        UUID id = UUID.randomUUID();
        final File checkpointDir = TEMP_FOLDER.newFolder("checkpoints_" + id).getAbsoluteFile();
        final File savepointDir = TEMP_FOLDER.newFolder("savepoints_" + id).getAbsoluteFile();

        if (!checkpointDir.exists() || !savepointDir.exists()) {
            throw new Exception("Test setup failed: failed to create (temporary) directories.");
        }

        LOG.info("Created temporary checkpoint directory: " + checkpointDir + ".");
        LOG.info("Created savepoint directory: " + savepointDir + ".");

        config.setString(StateBackendOptions.STATE_BACKEND, "memory");
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
        config.setLong(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 300L);

        return config;
    }

    @SafeVarargs
    protected final void executeAndSnapshot(
            StreamExecutionEnvironment env,
            String snapshotPath,
            SnapshotType snapshotType,
            Tuple2<String, Integer>... expectedAccumulators)
            throws Exception {

        final Deadline deadLine = Deadline.fromNow(Duration.ofMinutes(5));

        ClusterClient<?> client = miniClusterResource.getClusterClient();

        // TODO [FLINK-29802] Remove this after ChangelogStateBackend supports native savepoint.
        if (snapshotType == SnapshotType.SAVEPOINT_NATIVE) {
            env.enableChangelogStateBackend(false);
        }

        // Submit the job
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        JobID jobID = client.submitJob(jobGraph).get();

        LOG.info("Submitted job {} and waiting...", jobID);

        boolean done = false;
        while (deadLine.hasTimeLeft()) {
            Thread.sleep(100);
            Map<String, Object> accumulators = client.getAccumulators(jobID).get();

            boolean allDone = true;
            for (Tuple2<String, Integer> acc : expectedAccumulators) {
                Object accumOpt = accumulators.get(acc.f0);
                if (accumOpt == null) {
                    allDone = false;
                    break;
                }

                Integer numFinished = (Integer) accumOpt;
                if (!numFinished.equals(acc.f1)) {
                    allDone = false;
                    break;
                }
            }
            if (allDone) {
                done = true;
                break;
            }
        }

        if (!done) {
            fail("Did not see the expected accumulator results within time limit.");
        }

        LOG.info("Triggering snapshot.");

        CompletableFuture<String> snapshotPathFuture;
        switch (snapshotType) {
            case SAVEPOINT_CANONICAL:
                snapshotPathFuture =
                        client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL);
                break;
            case SAVEPOINT_NATIVE:
                snapshotPathFuture =
                        client.triggerSavepoint(jobID, null, SavepointFormatType.NATIVE);
                break;
            case CHECKPOINT:
                snapshotPathFuture = miniClusterResource.getMiniCluster().triggerCheckpoint(jobID);
                break;
            default:
                throw new UnsupportedOperationException("Snapshot type not supported/implemented.");
        }

        String jobmanagerSnapshotPath =
                snapshotPathFuture.get(deadLine.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

        File jobManagerSnapshot = new File(new URI(jobmanagerSnapshotPath).getPath());
        // savepoints were changed to be directories in Flink 1.3
        if (jobManagerSnapshot.isDirectory()) {
            FileUtils.moveDirectory(jobManagerSnapshot, new File(snapshotPath));
        } else {
            FileUtils.moveFile(jobManagerSnapshot, new File(snapshotPath));
        }
    }

    @SafeVarargs
    protected final void restoreAndExecute(
            StreamExecutionEnvironment env,
            String snapshotPath,
            Tuple2<String, Integer>... expectedAccumulators)
            throws Exception {

        final Deadline deadLine = Deadline.fromNow(Duration.ofMinutes(5));

        ClusterClient<?> client = miniClusterResource.getClusterClient();

        // Submit the job
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(snapshotPath));

        JobID jobID = client.submitJob(jobGraph).get();

        boolean done = false;
        while (deadLine.hasTimeLeft()) {

            // try and get a job result, this will fail if the job already failed. Use this
            // to get out of this loop

            try {
                CompletableFuture<JobStatus> jobStatusFuture = client.getJobStatus(jobID);

                JobStatus jobStatus = jobStatusFuture.get(5, TimeUnit.SECONDS);

                if (jobStatus == JobStatus.FAILED) {
                    LOG.warn(
                            "Job reached status failed",
                            client.requestJobResult(jobID)
                                    .get()
                                    .getSerializedThrowable()
                                    .get()
                                    .deserializeError(ClassLoader.getSystemClassLoader()));
                }
                assertNotEquals(JobStatus.FAILED, jobStatus);
            } catch (Exception e) {
                fail("Could not connect to job: " + e);
            }

            Thread.sleep(100);
            Map<String, Object> accumulators = client.getAccumulators(jobID).get();

            boolean allDone = true;
            for (Tuple2<String, Integer> acc : expectedAccumulators) {
                Object numFinished = accumulators.get(acc.f0);
                if (numFinished == null) {
                    allDone = false;
                    break;
                }
                if (!numFinished.equals(acc.f1)) {
                    allDone = false;
                    break;
                }
            }

            if (allDone) {
                done = true;
                break;
            }
        }

        if (!done) {
            fail("Did not see the expected accumulator results within time limit.");
        }
    }
}

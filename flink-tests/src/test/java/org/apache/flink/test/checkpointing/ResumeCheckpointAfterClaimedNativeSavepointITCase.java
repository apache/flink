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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.test.util.TestUtils.loadCheckpointMetadata;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies, through a real cluster, that an incremental checkpoint taken after a CLAIM-mode restore
 * from a NATIVE savepoint remains restorable.
 *
 * <p>A NATIVE RocksDB savepoint references its SST files relative to the savepoint directory. A job
 * restored from it in CLAIM mode reuses those files, so its first incremental checkpoint references
 * files that live outside the checkpoint's own exclusive directory. The checkpoint metadata must
 * record such references by their absolute location: a relative encoding would be resolved against
 * the checkpoint directory when the metadata is read back, and restoring from the checkpoint would
 * fail looking for files that were never there.
 *
 * <p>{@code RocksDBClaimSavepointThenCheckpointRestoreTest} pins the same property at the
 * backend/serializer level, in-process, without the JobManager finalization wiring. This test
 * instead drives the whole path on a real cluster, so it also covers what the component test
 * cannot: in particular that the metadata finalization in {@code PendingCheckpoint} binds to the
 * {@code CheckpointMetadataOutputStream} overload of {@code Checkpoints#storeCheckpointMetadata},
 * which hands the checkpoint's exclusive directory to the serializer.
 */
class ResumeCheckpointAfterClaimedNativeSavepointITCase {

    private static final int PARALLELISM = 2;
    private static final int NUM_KEYS = 128;

    @TempDir private Path checkpointsDir;
    @TempDir private Path savepointsDir;

    @Test
    void testFirstCheckpointAfterClaimIsRestorable() throws Exception {
        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        config.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, false);
        config.set(CheckpointingOptions.FILE_MERGING_ENABLED, false);
        config.set(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, false);
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointsDir.toUri().toString());
        config.set(
                CheckpointingOptions.EXTERNALIZED_CHECKPOINT_RETENTION,
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);
        // Keep every state file a real file so the shared-file assertion sees actual paths.
        config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
        // Fail fast on a broken restore instead of looping through restarts.
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");

        // Manual lifecycle because the configuration depends on per-test @TempDirs.
        final MiniClusterWithClientResource cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(PARALLELISM)
                                .build());
        cluster.before();
        try {
            final ClusterClient<?> client = cluster.getClusterClient();
            final MiniCluster miniCluster = cluster.getMiniCluster();

            // Checkpoint once so the savepoint covers a mix of reused and freshly flushed SSTs,
            // then stop with a NATIVE savepoint.
            final JobGraph initialJob = createJobGraph(config);
            client.submitJob(initialJob).get();
            waitForAllTaskRunning(miniCluster, initialJob.getJobID(), false);
            miniCluster.triggerCheckpoint(initialJob.getJobID()).get();
            final String savepointPath =
                    client.stopWithSavepoint(
                                    initialJob.getJobID(),
                                    false,
                                    savepointsDir.toUri().toString(),
                                    SavepointFormatType.NATIVE)
                            .get();

            // Restore in CLAIM mode, the first incremental checkpoint reuses the savepoint's SSTs.
            final JobGraph restoredFromSavepoint = createJobGraph(config);
            restoredFromSavepoint.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(
                            savepointPath, false, RecoveryClaimMode.CLAIM));
            client.submitJob(restoredFromSavepoint).get();
            waitForAllTaskRunning(miniCluster, restoredFromSavepoint.getJobID(), false);
            final String checkpointPath =
                    miniCluster.triggerCheckpoint(restoredFromSavepoint.getJobID()).get();
            client.cancel(restoredFromSavepoint.getJobID()).get();
            miniCluster.requestJobResult(restoredFromSavepoint.getJobID()).get();

            // The regression guard: reused savepoint files must be recorded by their absolute
            // location. The trailing separator keeps a sibling directory prefix from matching.
            final String savepointPrefix =
                    savepointPath.endsWith("/") ? savepointPath : savepointPath + "/";
            assertThat(sharedStateFilePaths(checkpointPath))
                    .as(
                            "the first incremental checkpoint after claiming the savepoint must "
                                    + "reference reused savepoint files by their absolute location")
                    .isNotEmpty()
                    .anySatisfy(path -> assertThat(path).startsWith(savepointPrefix));

            // Restore from that checkpoint (a missing-file failure would go terminal and surface
            // here) and let the restored job complete one more checkpoint of its own.
            final JobGraph restoredFromCheckpoint = createJobGraph(config);
            restoredFromCheckpoint.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(checkpointPath, false));
            client.submitJob(restoredFromCheckpoint).get();
            waitForAllTaskRunning(miniCluster, restoredFromCheckpoint.getJobID(), false);
            miniCluster.triggerCheckpoint(restoredFromCheckpoint.getJobID()).get();
            client.cancel(restoredFromCheckpoint.getJobID()).get();
            miniCluster.requestJobResult(restoredFromCheckpoint.getJobID()).get();
        } finally {
            cluster.after();
        }
    }

    /** All absolute file paths referenced by the checkpoint's keyed shared state. */
    private static List<String> sharedStateFilePaths(String checkpointPath) throws Exception {
        final CheckpointMetadata metadata = loadCheckpointMetadata(checkpointPath);
        return metadata.getOperatorStates().stream()
                .flatMap(operatorState -> operatorState.getStates().stream())
                .flatMap(subtaskState -> subtaskState.getManagedKeyedState().stream())
                .filter(IncrementalRemoteKeyedStateHandle.class::isInstance)
                .map(IncrementalRemoteKeyedStateHandle.class::cast)
                .flatMap(handle -> handle.getSharedState().stream())
                .map(HandleAndLocalPath::getHandle)
                .filter(FileStateHandle.class::isInstance)
                .map(handle -> ((FileStateHandle) handle).getFilePath().toString())
                .collect(Collectors.toList());
    }

    private static JobGraph createJobGraph(Configuration config) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(PARALLELISM);

        // The throttle is chained to the source, so barriers never queue behind a record backlog.
        // Not the full Long range: splitting that overflows in NumberSequenceIterator#split.
        env.fromSequence(0, Long.MAX_VALUE - 1)
                .map(new Throttler())
                .keyBy(value -> Math.floorMod(value, NUM_KEYS))
                .map(new StatefulCounter())
                .uid("stateful-counter")
                .sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    /**
     * Slows production so the reused savepoint SSTs are not compacted away before the post-restore
     * checkpoint. Best-effort: if reuse stops happening, the shared-file assertion fails.
     */
    private static final class Throttler implements MapFunction<Long, Long> {

        private static final long THROTTLE_MILLIS = 1;

        @Override
        public Long map(Long value) throws Exception {
            Thread.sleep(THROTTLE_MILLIS);
            return value;
        }
    }

    /** Gives the job keyed RocksDB state, without it there would be no shared SSTs to assert on. */
    private static final class StatefulCounter extends RichMapFunction<Long, Long> {

        private ValueState<Long> counter;

        @Override
        public void open(OpenContext openContext) throws Exception {
            counter =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "counter", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public Long map(Long value) throws Exception {
            long next = Optional.ofNullable(counter.value()).orElse(0L) + value;
            counter.update(next);
            return next;
        }
    }
}

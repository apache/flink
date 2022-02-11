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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.event.Level;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for taking savepoint in different {@link SavepointFormatType format types}. */
public class SavepointFormatITCase {
    @TempDir Path checkpointsDir;
    @TempDir Path originalSavepointDir;
    @TempDir Path renamedSavepointDir;

    @RegisterExtension
    LoggerAuditingExtension loggerAuditingExtension =
            new LoggerAuditingExtension(SavepointFormatITCase.class, Level.INFO);

    private static Stream<Arguments> parameters() {
        return Stream.of(
                Arguments.of(
                        SavepointFormatType.CANONICAL,
                        HEAP,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(SavepointKeyedStateHandle.class))),
                Arguments.of(
                        SavepointFormatType.NATIVE,
                        HEAP,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(KeyGroupsStateHandle.class))),
                Arguments.of(
                        SavepointFormatType.CANONICAL,
                        ROCKSDB_FULL_SNAPSHOTS,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(SavepointKeyedStateHandle.class))),
                Arguments.of(
                        SavepointFormatType.NATIVE,
                        ROCKSDB_FULL_SNAPSHOTS,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(KeyGroupsStateHandle.class))),
                Arguments.of(
                        SavepointFormatType.CANONICAL,
                        ROCKSDB_INCREMENTAL_SNAPSHOTS,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(SavepointKeyedStateHandle.class))),
                Arguments.of(
                        SavepointFormatType.NATIVE,
                        ROCKSDB_INCREMENTAL_SNAPSHOTS,
                        (Consumer<KeyedStateHandle>)
                                keyedState ->
                                        assertThat(
                                                keyedState,
                                                instanceOf(
                                                        IncrementalRemoteKeyedStateHandle.class))));
    }

    private abstract static class StateBackendConfig {
        public abstract String getName();

        public abstract Configuration getConfiguration();

        public int getCheckpointsBeforeSavepoint() {
            return 0;
        }

        @Override
        public final String toString() {
            return getName();
        }
    }

    private static final StateBackendConfig HEAP =
            new StateBackendConfig() {
                @Override
                public String getName() {
                    return "HEAP";
                }

                @Override
                public Configuration getConfiguration() {
                    Configuration stateBackendConfig = new Configuration();
                    stateBackendConfig.setString(StateBackendOptions.STATE_BACKEND, "filesystem");
                    stateBackendConfig.set(
                            CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
                    return stateBackendConfig;
                }
            };

    private static final StateBackendConfig ROCKSDB_FULL_SNAPSHOTS =
            new StateBackendConfig() {
                @Override
                public String getName() {
                    return "ROCKSDB_FULL_SNAPSHOTS";
                }

                @Override
                public Configuration getConfiguration() {
                    Configuration stateBackendConfig = new Configuration();
                    stateBackendConfig.setString(StateBackendOptions.STATE_BACKEND, "rocksdb");
                    stateBackendConfig.set(
                            CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
                    stateBackendConfig.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
                    return stateBackendConfig;
                }
            };

    private static final StateBackendConfig ROCKSDB_INCREMENTAL_SNAPSHOTS =
            new StateBackendConfig() {
                @Override
                public String getName() {
                    return "ROCKSDB_INCREMENTAL_SNAPSHOTS";
                }

                @Override
                public int getCheckpointsBeforeSavepoint() {
                    return 1;
                }

                @Override
                public Configuration getConfiguration() {
                    Configuration stateBackendConfig = new Configuration();
                    stateBackendConfig.setString(StateBackendOptions.STATE_BACKEND, "rocksdb");
                    stateBackendConfig.set(
                            CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.ZERO);
                    stateBackendConfig.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
                    return stateBackendConfig;
                }
            };

    @ParameterizedTest(name = "[{index}] {0}, {1}")
    @MethodSource("parameters")
    public void testTriggerSavepointAndResumeWithFileBasedCheckpointsAndRelocateBasePath(
            SavepointFormatType formatType,
            StateBackendConfig stateBackendConfig,
            Consumer<KeyedStateHandle> stateHandleVerification)
            throws Exception {
        final int numTaskManagers = 2;
        final int numSlotsPerTaskManager = 2;

        final Configuration config = stateBackendConfig.getConfiguration();
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointsDir.toUri().toString());
        final MiniClusterWithClientResource miniClusterResource =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(numTaskManagers)
                                .setNumberSlotsPerTaskManager(numSlotsPerTaskManager)
                                .build());

        miniClusterResource.before();
        try {

            final String savepointPath =
                    submitJobAndTakeSavepoint(
                            miniClusterResource,
                            formatType,
                            stateBackendConfig.getCheckpointsBeforeSavepoint());
            final CheckpointMetadata metadata = loadCheckpointMetadata(savepointPath);

            final OperatorState operatorState =
                    metadata.getOperatorStates().stream().filter(hasKeyedState()).findFirst().get();
            operatorState
                    .getStates()
                    .forEach(
                            subtaskState -> {
                                subtaskState
                                        .getManagedKeyedState()
                                        .forEach(stateHandleVerification);
                            });
            relocateAndVerify(miniClusterResource, savepointPath, renamedSavepointDir);
        } finally {
            miniClusterResource.after();
        }
    }

    @NotNull
    private Predicate<OperatorState> hasKeyedState() {
        return op ->
                op.hasSubtaskStates()
                        && op.getStates().stream()
                                .findFirst()
                                .map(subtaskState -> subtaskState.getManagedKeyedState().hasState())
                                .orElse(false);
    }

    private CheckpointMetadata loadCheckpointMetadata(String savepointPath) throws IOException {
        CompletedCheckpointStorageLocation location =
                AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(savepointPath);

        try (DataInputStream stream =
                new DataInputStream(location.getMetadataHandle().openInputStream())) {
            return Checkpoints.loadCheckpointMetadata(
                    stream, Thread.currentThread().getContextClassLoader(), savepointPath);
        }
    }

    private void relocateAndVerify(
            MiniClusterWithClientResource cluster, String savepointPath, Path renamedSavepointDir)
            throws Exception {
        final org.apache.flink.core.fs.Path oldPath =
                new org.apache.flink.core.fs.Path(savepointPath);
        final org.apache.flink.core.fs.Path newPath =
                new org.apache.flink.core.fs.Path(renamedSavepointDir.toUri().toString());
        (new org.apache.flink.core.fs.Path(savepointPath).getFileSystem()).rename(oldPath, newPath);
        final JobGraph jobGraph = createJobGraph();
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(
                        renamedSavepointDir.toUri().toString(), false, RestoreMode.CLAIM));

        final JobID jobId = jobGraph.getJobID();
        ClusterClient<?> client = cluster.getClusterClient();
        client.submitJob(jobGraph).get();
        waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);
    }

    private String submitJobAndTakeSavepoint(
            MiniClusterWithClientResource cluster,
            SavepointFormatType formatType,
            int checkpointBeforeSavepoint)
            throws Exception {
        final JobGraph jobGraph = createJobGraph();

        final JobID jobId = jobGraph.getJobID();
        ClusterClient<?> client = cluster.getClusterClient();
        client.submitJob(jobGraph).get();
        waitForAllTaskRunning(cluster.getMiniCluster(), jobId, false);

        for (int i = 0; i < checkpointBeforeSavepoint; i++) {
            cluster.getMiniCluster().triggerCheckpoint(jobId).get();
        }

        return client.stopWithSavepoint(
                        jobId, false, originalSavepointDir.toUri().toString(), formatType)
                .get();
    }

    private static JobGraph createJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.disableOperatorChaining();

        env.fromSequence(Long.MIN_VALUE, Long.MAX_VALUE)
                .keyBy(i -> i % 1000)
                .map(new StatefulCounter())
                .addSink(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static final class StatefulCounter extends RichMapFunction<Long, Long> {
        private ValueState<Long> counter;

        @Override
        public void open(Configuration parameters) throws Exception {
            counter =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "counter", BasicTypeInfo.LONG_TYPE_INFO));
        }

        @Override
        public Long map(Long value) throws Exception {
            counter.update(Optional.ofNullable(counter.value()).orElse(0L) + value);
            return counter.value();
        }
    }
}

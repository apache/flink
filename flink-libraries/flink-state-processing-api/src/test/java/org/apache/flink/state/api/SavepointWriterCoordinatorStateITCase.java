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

package org.apache.flink.state.api;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataV7Serializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.logging.LoggerAuditingExtension;
import org.apache.flink.util.FileUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.event.Level;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForCheckpoint;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a savepoint whose source coordinator state is in its own file can be rewritten with
 * the State Processor API, and that the job restores from the rewritten savepoint after the
 * original one is deleted.
 */
class SavepointWriterCoordinatorStateITCase extends AbstractTestBase {

    @RegisterExtension
    final LoggerAuditingExtension coordinatorLog =
            new LoggerAuditingExtension(RecreateOnResetOperatorCoordinator.class, Level.INFO);

    private static final String SOURCE_UID = "sequence-source";
    private static final String SINK_UID = "sink";

    @Test
    void testRewrittenSavepointRestoresAfterTheOriginalIsDeleted(
            @InjectMiniCluster MiniCluster miniCluster) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, getTempDirPath("checkpoints"));
        configuration.set(CheckpointingOptions.COORDINATOR_STATE_IN_FILE_ENABLED, true);
        // so that even the small enumerator state of the sequence source goes to its own file
        configuration.set(CheckpointingOptions.COORDINATOR_STATE_INLINE_THRESHOLD, MemorySize.ZERO);
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "none");

        final JobGraph original = createJobGraph(configuration, null);
        miniCluster.submitJob(original).get();
        waitForCheckpoint(original.getJobID(), miniCluster, 1);
        final String savepoint = stopWithSavepoint(miniCluster, original.getJobID());
        final Path originalFile = coordinatorStateFile(savepoint);
        assertThat(originalFile).isNotNull();
        final byte[] originalState = Files.readAllBytes(originalFile);

        final String rewritten = getTempDirPath("rewritten");
        rewrite(savepoint, rewritten);
        FileUtils.deleteDirectory(new File(new URI(savepoint)));

        assertThat(readMetadataVersion(rewritten)).isEqualTo(MetadataV7Serializer.VERSION);
        final Path copied = coordinatorStateFile(rewritten);
        assertThat(copied).isNotNull().exists().startsWith(Paths.get(new URI(rewritten)));
        assertThat(Files.readAllBytes(copied)).isEqualTo(originalState);

        // calls to the coordinator are deferred until it is reset, so once a checkpoint has
        // completed, the state has been read, and as the original is deleted, from the copy,
        // which has the same file name
        final JobGraph restored = createJobGraph(configuration, rewritten);
        miniCluster.submitJob(restored).get();
        waitForCheckpoint(restored.getJobID(), miniCluster, 1);
        miniCluster.cancelJob(restored.getJobID()).get();
        assertThat(coordinatorLog.getMessages())
                .anyMatch(
                        message ->
                                message.startsWith("Read ")
                                        && message.contains(copied.getFileName().toString()));
    }

    private static JobGraph createJobGraph(
            Configuration configuration, @Nullable String restoreFrom) {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.enableCheckpointing(100);
        env.fromSequence(0, Long.MAX_VALUE)
                .uid(SOURCE_UID)
                .sinkTo(new DiscardingSink<>())
                .uid(SINK_UID);
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        if (restoreFrom != null) {
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(restoreFrom, false));
        }
        return jobGraph;
    }

    private String stopWithSavepoint(MiniCluster miniCluster, JobID jobId) throws Exception {
        return miniCluster
                .stopWithSavepoint(
                        jobId, getTempDirPath("savepoints"), false, SavepointFormatType.CANONICAL)
                .get();
    }

    private static void rewrite(String savepoint, String target) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // as the State Processor API requires for writing savepoints
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // the rewrite drops the state of the sink, which holds none, and keeps the source's
        SavepointWriter.fromExistingSavepoint(env, savepoint)
                .removeOperator(OperatorIdentifier.forUid(SINK_UID))
                .write(target);
        env.execute("Rewrite the savepoint");
    }

    /**
     * Checks that the savepoint's source coordinator state is not inline, and returns its file, or
     * null if it is not in a file.
     */
    @Nullable
    private static Path coordinatorStateFile(String savepoint) throws Exception {
        final CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(savepoint);
        final List<StreamStateHandle> handles =
                metadata.getOperatorStates().stream()
                        .map(OperatorState::getCoordinatorState)
                        .filter(handle -> handle != null)
                        .collect(Collectors.toList());
        assertThat(handles).hasSize(1).noneMatch(ByteStreamStateHandle.class::isInstance);
        return handles.get(0) instanceof FileStateHandle
                ? Paths.get(((FileStateHandle) handles.get(0)).getFilePath().toUri())
                : null;
    }

    private static int readMetadataVersion(String savepoint) throws Exception {
        final Path metadata =
                Paths.get(new URI(savepoint))
                        .resolve(AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        try (DataInputStream in = new DataInputStream(Files.newInputStream(metadata))) {
            assertThat(in.readInt()).isEqualTo(Checkpoints.HEADER_MAGIC_NUMBER);
            return in.readInt();
        }
    }
}

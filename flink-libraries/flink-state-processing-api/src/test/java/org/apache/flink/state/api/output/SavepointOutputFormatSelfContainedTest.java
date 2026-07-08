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

package org.apache.flink.state.api.output;

import org.apache.flink.api.common.io.FirstAttemptInitializationContext;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that a savepoint written by {@link SavepointOutputFormat} stays self-contained when it
 * retains operator state from an existing savepoint, i.e. that it holds no references back into the
 * source savepoint it was derived from.
 *
 * <p>{@code SavepointWriter.fromExistingSavepoint(...)} keeps retained operator state by copying
 * the state files into the new savepoint directory ({@link FileCopyFunction}, keeping the file
 * names), while the retained {@link RelativeFileStateHandle}s still carry the paths of the original
 * files in the source savepoint. The new savepoint is self-contained only if its metadata
 * references those files by file name alone: such a reference resolves against the directory the
 * metadata is read from, which is the new savepoint holding the copies. If the metadata recorded
 * the handles' absolute paths instead, the new savepoint would keep depending on the source
 * savepoint and become unrestorable once the source is deleted.
 *
 * <p>See the comment in {@link SavepointOutputFormat#writeRecord} for why the metadata must be
 * written with {@code Checkpoints.storeCheckpointMetadataWithoutExclusiveDir}. {@code
 * SavepointDeepCopyTest} covers the same guarantee end-to-end on a cluster, where a regression
 * surfaces only as a restore failure far from its cause; this test pins the encoding decision
 * itself, so a regression fails fast and points at the responsible code.
 */
class SavepointOutputFormatSelfContainedTest {

    private static final int PARALLELISM = 1;
    private static final int MAX_PARALLELISM = 128;
    private static final int SUBTASK_INDEX = 0;
    private static final long CHECKPOINT_ID = 1L;
    private static final String OPERATOR_UID = "uid";
    private static final String STATE_FILE_NAME = "sst-0001.state";

    @Test
    void testSavepointWithRetainedStateStaysSelfContained(@TempDir File tempDir) throws Exception {
        File sourceSavepointDir = new File(tempDir, "sp-old");
        File targetSavepointDir = new File(tempDir, "sp-new");
        assertThat(sourceSavepointDir.mkdirs()).isTrue();
        assertThat(targetSavepointDir.mkdirs()).isTrue();

        // A retained state file living in the source savepoint, referenced by a relative handle
        // whose full path still points at the file in the source directory (this is what loading
        // an existing savepoint produces).
        byte[] stateBytes = "retained-shared-state".getBytes(StandardCharsets.UTF_8);
        File sourceStateFile = new File(sourceSavepointDir, STATE_FILE_NAME);
        Files.write(sourceStateFile.toPath(), stateBytes);

        Path targetSavepointPath = new Path(targetSavepointDir.getAbsolutePath());
        Path sourceStatePath =
                new Path(new Path(sourceSavepointDir.getAbsolutePath()), STATE_FILE_NAME);
        RelativeFileStateHandle retainedHandle =
                new RelativeFileStateHandle(sourceStatePath, STATE_FILE_NAME, stateBytes.length);

        // Copy the retained file the way SavepointWriter.fromExistingSavepoint does: through
        // FileCopyFunction, which copies by bare file name into the new savepoint directory. The
        // metadata written below is self-contained only together with this naming contract.
        FileCopyFunction copyFunction = new FileCopyFunction(targetSavepointDir.getAbsolutePath());
        copyFunction.open(FirstAttemptInitializationContext.of(SUBTASK_INDEX, PARALLELISM));
        copyFunction.writeRecord(sourceStatePath);
        copyFunction.close();

        CheckpointMetadata metadata = createMetadata(retainedHandle);

        SavepointOutputFormat format = new SavepointOutputFormat(targetSavepointPath);
        format.setRuntimeContext(new MockStreamingRuntimeContext(PARALLELISM, SUBTASK_INDEX));
        format.open(FirstAttemptInitializationContext.of(SUBTASK_INDEX, PARALLELISM));
        format.writeRecord(metadata);
        format.close();

        // Reload the written metadata; relative handles resolve against the directory the
        // metadata is read from, i.e. the new savepoint holding the copies.
        CheckpointMetadata reloaded =
                SavepointLoader.loadSavepointMetadata(targetSavepointPath.getPath());
        StreamStateHandle reloadedShared = extractSingleSharedStateHandle(reloaded);

        assertThat(reloadedShared)
                .as(
                        "Retained state must reload as a relative handle that resolves inside "
                                + "the new savepoint; an absolute path back into the source "
                                + "savepoint would break the new savepoint once the source is "
                                + "deleted")
                .isInstanceOf(RelativeFileStateHandle.class);

        RelativeFileStateHandle reloadedRelative = (RelativeFileStateHandle) reloadedShared;
        assertThat(reloadedRelative.getRelativePath()).isEqualTo(STATE_FILE_NAME);
        assertThat(reloadedRelative.getFilePath().getPath())
                .isEqualTo(new Path(targetSavepointPath, STATE_FILE_NAME).getPath());
        assertThat(new File(reloadedRelative.getFilePath().getPath())).exists();
    }

    private static CheckpointMetadata createMetadata(RelativeFileStateHandle retainedHandle) {
        StreamStateHandle metaStateHandle =
                new ByteStreamStateHandle(
                        "backend-meta", "backend-meta".getBytes(StandardCharsets.UTF_8));
        IncrementalRemoteKeyedStateHandle keyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, MAX_PARALLELISM - 1),
                        CHECKPOINT_ID,
                        Collections.singletonList(
                                HandleAndLocalPath.of(retainedHandle, STATE_FILE_NAME)),
                        Collections.emptyList(),
                        metaStateHandle);

        OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder().setManagedKeyedState(keyedStateHandle).build();

        OperatorState operatorState =
                new OperatorState(
                        null,
                        OPERATOR_UID,
                        OperatorIDGenerator.fromUid(OPERATOR_UID),
                        PARALLELISM,
                        MAX_PARALLELISM);
        operatorState.putState(SUBTASK_INDEX, subtaskState);

        return new CheckpointMetadata(
                CHECKPOINT_ID, Collections.singleton(operatorState), Collections.emptyList());
    }

    private static StreamStateHandle extractSingleSharedStateHandle(CheckpointMetadata metadata) {
        IncrementalRemoteKeyedStateHandle keyedStateHandle =
                (IncrementalRemoteKeyedStateHandle)
                        metadata.getOperatorStates()
                                .iterator()
                                .next()
                                .getState(SUBTASK_INDEX)
                                .getManagedKeyedState()
                                .iterator()
                                .next();
        return keyedStateHandle.getSharedState().get(0).getHandle();
    }
}

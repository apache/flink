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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.ChangelogTestUtils;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Various tests for the version 3 format serializer of a checkpoint. */
class MetadataV3SerializerTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testCheckpointWithNoState() throws Exception {
        final Random rnd = new Random();

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;
            final Collection<OperatorState> taskStates = Collections.emptyList();
            final Collection<MasterState> masterStates = Collections.emptyList();

            testCheckpointSerialization(checkpointId, taskStates, masterStates, null);
        }
    }

    @Test
    void testCheckpointWithOnlyMasterState() throws Exception {
        final Random rnd = new Random();
        final int maxNumMasterStates = 5;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final Collection<OperatorState> operatorStates = Collections.emptyList();

            final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
            final Collection<MasterState> masterStates =
                    CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

            testCheckpointSerialization(checkpointId, operatorStates, masterStates, null);
        }
    }

    @Test
    void testCheckpointWithOnlyTaskStateForCheckpoint() throws Exception {
        testCheckpointWithOnlyTaskState(null);
    }

    @Test
    void testCheckpointWithOnlyTaskStateForSavepoint() throws Exception {
        testCheckpointWithOnlyTaskState(TempDirUtils.newFolder(temporaryFolder).toURI().toString());
    }

    private void testCheckpointWithOnlyTaskState(String basePath) throws Exception {
        final Random rnd = new Random();
        final int maxTaskStates = 20;
        final int maxNumSubtasks = 20;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final int numTasks = rnd.nextInt(maxTaskStates) + 1;
            final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
            final Collection<OperatorState> taskStates =
                    CheckpointTestUtils.createOperatorStates(
                            rnd, basePath, numTasks, 0, 0, numSubtasks);

            final Collection<MasterState> masterStates = Collections.emptyList();

            testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
        }
    }

    @Test
    void testCheckpointWithMasterAndTaskStateForCheckpoint() throws Exception {
        testCheckpointWithMasterAndTaskState(null);
    }

    @Test
    void testCheckpointWithMasterAndTaskStateForSavepoint() throws Exception {
        testCheckpointWithMasterAndTaskState(
                TempDirUtils.newFolder(temporaryFolder).toURI().toString());
    }

    private void testCheckpointWithMasterAndTaskState(String basePath) throws Exception {
        final Random rnd = new Random();

        final int maxNumMasterStates = 5;
        final int maxTaskStates = 20;
        final int maxNumSubtasks = 20;

        for (int i = 0; i < 100; ++i) {
            final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

            final int numTasks = rnd.nextInt(maxTaskStates) + 1;
            final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
            final Collection<OperatorState> taskStates =
                    CheckpointTestUtils.createOperatorStates(
                            rnd, basePath, numTasks, 0, 0, numSubtasks);

            final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
            final Collection<MasterState> masterStates =
                    CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

            testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
        }
    }

    @Test
    void testCheckpointWithFinishedTasksForCheckpoint() throws Exception {
        testCheckpointWithFinishedTasks(null);
    }

    @Test
    void testCheckpointWithFinishedTasksForSavepoint() throws Exception {
        testCheckpointWithFinishedTasks(TempDirUtils.newFolder(temporaryFolder).toURI().toString());
    }

    private void testCheckpointWithFinishedTasks(String basePath) throws Exception {
        final Random rnd = new Random();

        final int maxNumMasterStates = 5;
        final int maxNumSubtasks = 20;

        final int maxAllRunningTaskStates = 20;
        final int maxPartlyFinishedStates = 10;
        final int maxFullyFinishedSubtasks = 10;

        final long checkpointId = rnd.nextLong() & 0x7fffffffffffffffL;

        final int numSubtasks = rnd.nextInt(maxNumSubtasks) + 1;
        final int numAllRunningTasks = rnd.nextInt(maxAllRunningTaskStates) + 1;
        final int numPartlyFinishedTasks = rnd.nextInt(maxPartlyFinishedStates) + 1;
        final int numFullyFinishedTasks = rnd.nextInt(maxFullyFinishedSubtasks) + 1;
        final Collection<OperatorState> taskStates =
                CheckpointTestUtils.createOperatorStates(
                        rnd,
                        basePath,
                        numAllRunningTasks,
                        numPartlyFinishedTasks,
                        numFullyFinishedTasks,
                        numSubtasks);

        final int numMasterStates = rnd.nextInt(maxNumMasterStates) + 1;
        final Collection<MasterState> masterStates =
                CheckpointTestUtils.createRandomMasterStates(rnd, numMasterStates);

        testCheckpointSerialization(checkpointId, taskStates, masterStates, basePath);
    }

    /**
     * Test checkpoint metadata (de)serialization.
     *
     * @param checkpointId The given checkpointId will write into the metadata.
     * @param operatorStates the given states for all the operators.
     * @param masterStates the masterStates of the given checkpoint/savepoint.
     */
    private void testCheckpointSerialization(
            long checkpointId,
            Collection<OperatorState> operatorStates,
            Collection<MasterState> masterStates,
            @Nullable String basePath)
            throws IOException {

        MetadataV3Serializer serializer = MetadataV3Serializer.INSTANCE;

        ByteArrayOutputStreamWithPos baos = new ByteArrayOutputStreamWithPos();
        DataOutputStream out = new DataOutputViewStreamWrapper(baos);

        CheckpointMetadata metadata =
                new CheckpointMetadata(checkpointId, operatorStates, masterStates);
        MetadataV3Serializer.INSTANCE.serialize(metadata, out);
        out.close();

        // The relative pointer resolution in MetadataV2V3SerializerBase currently runs the same
        // code as the file system checkpoint location resolution. Because of that, it needs a
        // "_metadata" file present. we could change the code to resolve the pointer without doing
        // file I/O, but it is somewhat delicate to reproduce that logic without I/O and the same
        // guarantees to differentiate between the supported options of directory addressing and
        // metadata file addressing. So, better safe than sorry, we do actually do the file system
        // operations in the serializer for now, even if it makes the tests a tad bit more clumsy.
        if (basePath != null) {
            final Path metaPath = new Path(basePath, "_metadata");
            // this is in the temp folder, so it will get automatically deleted
            FileSystem.getLocalFileSystem()
                    .create(metaPath, FileSystem.WriteMode.OVERWRITE)
                    .close();
        }

        byte[] bytes = baos.toByteArray();

        DataInputStream in = new DataInputViewStreamWrapper(new ByteArrayInputStreamWithPos(bytes));
        CheckpointMetadata deserialized =
                serializer.deserialize(in, getClass().getClassLoader(), basePath);
        assertThat(deserialized.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(deserialized.getOperatorStates()).isEqualTo(operatorStates);
        assertThat(
                        deserialized.getOperatorStates().stream()
                                .map(OperatorState::isFullyFinished)
                                .collect(Collectors.toList()))
                .isEqualTo(
                        operatorStates.stream()
                                .map(OperatorState::isFullyFinished)
                                .collect(Collectors.toList()));

        assertThat(deserialized.getMasterStates()).hasSameSizeAs(masterStates);
        for (Iterator<MasterState> a = masterStates.iterator(),
                        b = deserialized.getMasterStates().iterator();
                a.hasNext(); ) {
            CheckpointTestUtils.assertMasterStateEquality(a.next(), b.next());
        }
    }

    @Test
    void testSerializeKeyGroupsStateHandle() throws IOException {
        KeyGroupRangeOffsets offsets = new KeyGroupRangeOffsets(0, 123);
        byte[] data = {1, 2, 3, 4};
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            MetadataV2V3SerializerBase.serializeStreamStateHandle(
                    new KeyGroupsStateHandle(offsets, new ByteStreamStateHandle("test", data)),
                    new DataOutputStream(out));
            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                StreamStateHandle handle =
                        MetadataV2V3SerializerBase.deserializeStreamStateHandle(
                                new DataInputStream(in), null);
                assertThat(handle).isNotNull().isInstanceOf(KeyGroupsStateHandle.class);
                assertThat(((KeyGroupsStateHandle) handle).getGroupRangeOffsets())
                        .isEqualTo(offsets);
                byte[] deserialized = new byte[data.length];
                try (FSDataInputStream dataStream = handle.openInputStream()) {
                    dataStream.read(deserialized);
                    assertThat(deserialized).isEqualTo(data);
                }
            }
        }
    }

    @Test
    void testSerializeIncrementalChangelogStateBackendHandle() throws IOException {
        testSerializeChangelogStateBackendHandle(false);
    }

    @Test
    void testSerializeFullChangelogStateBackendHandle() throws IOException {
        testSerializeChangelogStateBackendHandle(true);
    }

    private void testSerializeChangelogStateBackendHandle(boolean fullSnapshot) throws IOException {
        ChangelogStateBackendHandle handle = createChangelogStateBackendHandle(fullSnapshot);
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            MetadataV2V3SerializerBase.serializeKeyedStateHandle(handle, new DataOutputStream(out));
            try (ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray())) {
                KeyedStateHandle deserialized =
                        MetadataV2V3SerializerBase.deserializeKeyedStateHandle(
                                new DataInputStream(in), null);
                assertThat(deserialized)
                        .isInstanceOfSatisfying(
                                ChangelogStateBackendHandleImpl.class,
                                o ->
                                        assertThat(o.getMaterializedStateHandles())
                                                .isEqualTo(handle.getMaterializedStateHandles()));
            }
        }
    }

    private ChangelogStateBackendHandle createChangelogStateBackendHandle(boolean fullSnapshot) {
        KeyedStateHandle keyedStateHandle =
                fullSnapshot
                        ? CheckpointTestUtils.createDummyKeyGroupStateHandle(
                                ThreadLocalRandom.current(), null)
                        : CheckpointTestUtils.createDummyIncrementalKeyedStateHandle(
                                ThreadLocalRandom.current());
        return ChangelogTestUtils.createChangelogStateBackendHandle(keyedStateHandle);
    }
}

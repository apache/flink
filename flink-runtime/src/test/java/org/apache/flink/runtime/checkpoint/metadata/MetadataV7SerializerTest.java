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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link MetadataV7Serializer} test. */
class MetadataV7SerializerTest {

    private static final String FILE_NAME = "coordinator-state";

    @TempDir private Path tempDir;

    @Test
    void testCoordinatorStateInMemoryRoundTrips() throws IOException {
        final String dir = metadataDirectory("checkpoint");

        final StreamStateHandle restored =
                roundTrip(
                        new ByteStreamStateHandle("coordinator", new byte[] {1, 2, 3}), null, dir);

        assertThat(((ByteStreamStateHandle) restored).getData()).containsExactly(1, 2, 3);
    }

    @Test
    void testCoordinatorStateInFileRoundTrips() throws IOException {
        final String dir = metadataDirectory("checkpoint");
        final FileStateHandle file =
                new FileStateHandle(new org.apache.flink.core.fs.Path(dir, FILE_NAME), 4096);

        final StreamStateHandle restored = roundTrip(file, null, dir);

        assertThat(restored).isInstanceOf(FileStateHandle.class).isEqualTo(file);
        assertThat(restored.getStateSize()).isEqualTo(4096);
    }

    @Test
    void testCoordinatorStateInFileResolvesAgainstTheDirectoryItIsReadFrom() throws IOException {
        final org.apache.flink.core.fs.Path writtenTo =
                new org.apache.flink.core.fs.Path(metadataDirectory("savepoint"));
        final String movedTo = metadataDirectory("moved-savepoint");
        final RelativeFileStateHandle relative =
                new RelativeFileStateHandle(
                        new org.apache.flink.core.fs.Path(writtenTo, FILE_NAME), FILE_NAME, 4096);

        final StreamStateHandle restored = roundTrip(relative, writtenTo, movedTo);

        assertThat(restored).isInstanceOf(RelativeFileStateHandle.class);
        assertThat(((RelativeFileStateHandle) restored).getFilePath())
                .isEqualTo(new org.apache.flink.core.fs.Path(movedTo, FILE_NAME));
    }

    @ParameterizedTest(name = "savepoint: {0}")
    @ValueSource(booleans = {false, true})
    void testCheckpointWithEveryKindOfStateRoundTrips(boolean savepoint) throws IOException {
        final Random random = new Random();
        final String basePath = savepoint ? metadataDirectory("savepoint") : null;
        final List<OperatorState> operatorStates =
                new ArrayList<>(
                        CheckpointTestUtils.createOperatorStates(random, basePath, 3, 2, 1, 4));
        final OperatorState withStateInFile =
                new OperatorState("operator", "uid", new OperatorID(), 4, 128);
        withStateInFile.setCoordinatorState(
                new FileStateHandle(
                        new org.apache.flink.core.fs.Path(
                                metadataDirectory("checkpoint"), FILE_NAME),
                        4096));
        // with the merged channel state that version 6 added
        operatorStates
                .get(0)
                .getSubtaskStates()
                .forEach(
                        (subtask, state) ->
                                withStateInFile.putState(
                                        subtask,
                                        state.toBuilder()
                                                .setInputChannelState(
                                                        StateObjectCollection.singleton(
                                                                ChannelStateTestUtils
                                                                        .randomMergedInputChannelStateHandle()))
                                                .setResultSubpartitionState(
                                                        StateObjectCollection.singleton(
                                                                ChannelStateTestUtils
                                                                        .randomMergedResultSubpartitionStateHandle()))
                                                .build()));
        operatorStates.add(withStateInFile);
        final Collection<MasterState> masterStates =
                CheckpointTestUtils.createRandomMasterStates(random, 3);

        final byte[] bytes =
                serialize(
                        MetadataV7Serializer.INSTANCE,
                        new CheckpointMetadata(42L, operatorStates, masterStates, null),
                        null);
        final CheckpointMetadata restored;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            restored =
                    MetadataV7Serializer.INSTANCE.deserialize(
                            in, getClass().getClassLoader(), basePath);
        }

        assertThat(restored.getCheckpointId()).isEqualTo(42L);
        assertThat(restored.getOperatorStates()).isEqualTo(operatorStates);
        assertThat(
                        restored.getOperatorStates().stream()
                                .map(OperatorState::isFullyFinished)
                                .collect(Collectors.toList()))
                .isEqualTo(
                        operatorStates.stream()
                                .map(OperatorState::isFullyFinished)
                                .collect(Collectors.toList()));
        assertThat(restored.getMasterStates()).hasSameSizeAs(masterStates);
        for (Iterator<MasterState> expected = masterStates.iterator(),
                        actual = restored.getMasterStates().iterator();
                expected.hasNext(); ) {
            CheckpointTestUtils.assertMasterStateEquality(expected.next(), actual.next());
        }
    }

    @ParameterizedTest(name = "version {0}")
    @ValueSource(ints = {3, 4, 5, 6})
    void testEarlierVersionsRejectCoordinatorStateInFileWhenWriting(int version) {
        final MetadataSerializer serializer = MetadataSerializers.getSerializer(version);
        final CheckpointMetadata metadata =
                metadataWithCoordinatorState(
                        new FileStateHandle(new org.apache.flink.core.fs.Path("/tmp/state"), 1));

        assertThatThrownBy(() -> serialize(serializer, metadata, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("can only store coordinator state inline");
    }

    @ParameterizedTest(name = "version {0}")
    @ValueSource(ints = {3, 4, 5, 6})
    void testEarlierVersionsRejectCoordinatorStateInFileWhenReading(int version)
            throws IOException {
        final MetadataV3Serializer serializer =
                (MetadataV3Serializer) MetadataSerializers.getSerializer(version);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(out)) {
            MetadataV2V3SerializerBase.serializeStreamStateHandle(
                    new FileStateHandle(new org.apache.flink.core.fs.Path("/tmp/state"), 1),
                    dos,
                    MetadataV2V3SerializerBase.SerializationContext.withExclusiveDir(
                            new org.apache.flink.core.fs.Path("/tmp")));
        }

        try (DataInputStream in =
                new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
            assertThatThrownBy(() -> serializer.deserializeCoordinatorState(in, null))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Expected a ByteStreamStateHandle");
        }
    }

    private StreamStateHandle roundTrip(
            StreamStateHandle coordinatorState,
            @Nullable org.apache.flink.core.fs.Path exclusiveDir,
            String readFrom)
            throws IOException {
        final byte[] bytes =
                serialize(
                        MetadataV7Serializer.INSTANCE,
                        metadataWithCoordinatorState(coordinatorState),
                        exclusiveDir);
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            final CheckpointMetadata restored =
                    MetadataV7Serializer.INSTANCE.deserialize(
                            in, getClass().getClassLoader(), readFrom);
            return restored.getOperatorStates().iterator().next().getCoordinatorState();
        }
    }

    private static byte[] serialize(
            MetadataSerializer serializer,
            CheckpointMetadata metadata,
            @Nullable org.apache.flink.core.fs.Path exclusiveDir)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            serializer.serialize(metadata, dos, exclusiveDir);
            dos.flush();
            return out.toByteArray();
        }
    }

    private static CheckpointMetadata metadataWithCoordinatorState(
            StreamStateHandle coordinatorState) {
        final OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 1, 1);
        operatorState.setCoordinatorState(coordinatorState);
        return new CheckpointMetadata(
                1L, Collections.singletonList(operatorState), Collections.emptyList(), null);
    }

    /** A directory with a metadata file in it, so that it can be used as a checkpoint pointer. */
    private String metadataDirectory(String name) throws IOException {
        final String dir = tempDir.resolve(name).toUri().toString();
        FileSystem.getLocalFileSystem()
                .create(
                        new org.apache.flink.core.fs.Path(
                                dir, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME),
                        FileSystem.WriteMode.OVERWRITE)
                .close();
        return dir;
    }
}

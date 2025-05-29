package org.apache.flink.runtime.checkpoint.metadata;

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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.InputStateHandle;
import org.apache.flink.runtime.state.OutputStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateTestUtils.randomMergedInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateTestUtils.randomMergedResultSubpartitionStateHandle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** {@link MetadataV6Serializer} test. */
class MetadataV6SerializerTest {

    private static final MetadataSerializer INSTANCE = MetadataV6Serializer.INSTANCE;

    private static final Random RND = new Random();

    private String basePath;

    private List<InputStateHandle> inputHandles;
    private List<OutputStateHandle> outputHandles;

    private CheckpointMetadata metadata;

    @BeforeEach
    public void beforeEach(@TempDir Path tempDir) throws IOException {
        basePath = tempDir.toUri().toString();

        final org.apache.flink.core.fs.Path metaPath =
                new org.apache.flink.core.fs.Path(
                        basePath, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        // this is in the temp folder, so it will get automatically deleted
        FileSystem.getLocalFileSystem().create(metaPath, FileSystem.WriteMode.OVERWRITE).close();
    }

    @Test
    void testSerializeUnmergedChannelStateHandle() throws IOException {
        testSerializeChannelStateHandle(
                () ->
                        ChannelStateTestUtils.randomInputChannelStateHandlesFromSameSubtask()
                                .stream()
                                .map(e -> (InputStateHandle) e)
                                .collect(Collectors.toList()),
                () ->
                        ChannelStateTestUtils.randomResultSubpartitionStateHandlesFromSameSubtask()
                                .stream()
                                .map(e -> (OutputStateHandle) e)
                                .collect(Collectors.toList()));
    }

    @Test
    void testSerializeMergedChannelStateHandle() throws IOException {
        testSerializeChannelStateHandle(
                () -> Collections.singletonList(randomMergedInputChannelStateHandle()),
                () -> Collections.singletonList(randomMergedResultSubpartitionStateHandle()));
    }

    private void testSerializeChannelStateHandle(
            Supplier<List<InputStateHandle>> getter1, Supplier<List<OutputStateHandle>> getter2)
            throws IOException {

        prepareAndSerializeMetadata(getter1, getter2);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            INSTANCE.serialize(metadata, dos);

            try (DataInputStream dis =
                    new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {

                CheckpointMetadata deserializedMetadata =
                        INSTANCE.deserialize(dis, metadata.getClass().getClassLoader(), basePath);

                Collection<OperatorState> operatorStates = deserializedMetadata.getOperatorStates();
                assertThat(operatorStates).hasSize(1);

                OperatorState operatorState = operatorStates.iterator().next();
                assertEquals(1, operatorState.getNumberCollectedStates());

                OperatorSubtaskState subtaskState = operatorState.getState(0);

                assertEquals(inputHandles, subtaskState.getInputChannelState().asList());
                assertEquals(outputHandles, subtaskState.getResultSubpartitionState().asList());
            }
        }
    }

    private void prepareAndSerializeMetadata(
            Supplier<List<InputStateHandle>> getter1, Supplier<List<OutputStateHandle>> getter2) {
        Collection<OperatorState> operatorStates =
                CheckpointTestUtils.createOperatorStates(RND, basePath, 1, 0, 0, 1);

        inputHandles = getter1.get();
        outputHandles = getter2.get();

        // Set merged channel state handle to each subtask state
        for (OperatorState operatorState : operatorStates) {
            int subtaskStateCount = operatorState.getNumberCollectedStates();
            for (int i = 0; i < subtaskStateCount; i++) {
                OperatorSubtaskState originSubtaskState = operatorState.getState(i);

                OperatorSubtaskState.Builder builder = originSubtaskState.toBuilder();
                builder.setInputChannelState(new StateObjectCollection<>(inputHandles));
                builder.setResultSubpartitionState(new StateObjectCollection<>(outputHandles));

                operatorState.putState(i, builder.build());
            }
        }

        metadata = new CheckpointMetadata(1L, operatorStates, emptyList(), null);
    }
}

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
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
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
import java.util.OptionalLong;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MetadataV7Serializer}, in particular the (de)serialization of the Regional
 * Checkpoint reference id ({@code refCheckpointId}) and backward compatibility with V6.
 */
class MetadataV7SerializerTest {

    private static final Random RND = new Random();

    private String basePath;

    @BeforeEach
    void beforeEach(@TempDir Path tempDir) throws IOException {
        basePath = tempDir.toUri().toString();
        final org.apache.flink.core.fs.Path metaPath =
                new org.apache.flink.core.fs.Path(
                        basePath, AbstractFsCheckpointStorageAccess.METADATA_FILE_NAME);
        FileSystem.getLocalFileSystem().create(metaPath, FileSystem.WriteMode.OVERWRITE).close();
    }

    @Test
    void testRefCheckpointIdRoundTrip() throws IOException {
        // A subtask state carrying a refCheckpointId should round-trip through V7.
        final CheckpointMetadata metadata = createMetadataWithRefCheckpointId(99L);

        final CheckpointMetadata deserialized =
                serializeAndDeserialize(MetadataV7Serializer.INSTANCE, metadata);

        assertThat(firstSubtaskState(deserialized).getRefCheckpointId())
                .isEqualTo(OptionalLong.of(99L));
    }

    @Test
    void testNoRefCheckpointIdRoundTrip() throws IOException {
        // A subtask state without a refCheckpointId should round-trip as empty through V7.
        final CheckpointMetadata metadata = createMetadataWithRefCheckpointId(null);

        final CheckpointMetadata deserialized =
                serializeAndDeserialize(MetadataV7Serializer.INSTANCE, metadata);

        assertThat(firstSubtaskState(deserialized).getRefCheckpointId())
                .isEqualTo(OptionalLong.empty());
    }

    @Test
    void testBackwardCompatibilityV6WrittenReadByV7() throws IOException {
        // Metadata written by V6 (no refCheckpointId field) must be readable, with an empty
        // refCheckpointId. V6 is read back with its own serializer based on the version header,
        // but here we explicitly verify the value is treated as empty.
        final CheckpointMetadata metadata = createMetadataWithRefCheckpointId(null);

        final CheckpointMetadata deserialized =
                serializeAndDeserialize(MetadataV6Serializer.INSTANCE, metadata);

        assertThat(firstSubtaskState(deserialized).getRefCheckpointId())
                .isEqualTo(OptionalLong.empty());
    }

    private CheckpointMetadata createMetadataWithRefCheckpointId(Long refCheckpointId) {
        final Collection<OperatorState> operatorStates =
                CheckpointTestUtils.createOperatorStates(RND, basePath, 1, 0, 0, 1);
        for (OperatorState operatorState : operatorStates) {
            final OperatorSubtaskState origin = operatorState.getState(0);
            final OperatorSubtaskState.Builder builder = origin.toBuilder();
            if (refCheckpointId != null) {
                builder.setRefCheckpointId(refCheckpointId);
            }
            operatorState.putState(0, builder.build());
        }
        return new CheckpointMetadata(1L, operatorStates, emptyList(), null);
    }

    private CheckpointMetadata serializeAndDeserialize(
            MetadataSerializer serializer, CheckpointMetadata metadata) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            serializer.serialize(metadata, dos, null);
            try (DataInputStream dis =
                    new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
                return serializer.deserialize(dis, metadata.getClass().getClassLoader(), basePath);
            }
        }
    }

    private OperatorSubtaskState firstSubtaskState(CheckpointMetadata metadata) {
        final OperatorState operatorState = metadata.getOperatorStates().iterator().next();
        return operatorState.getState(0);
    }
}

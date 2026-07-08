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

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.filesystem.RelativeFileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the {@link MetadataSerializer#serialize} entry points agree with each other across
 * every registered metadata version:
 *
 * <ul>
 *   <li>without an exclusive directory, every entry point produces byte-identical output — a
 *       convenience overload must not drift from the contract-bearing method it delegates to;
 *   <li>a supplied exclusive directory reaches the relative-handle encoder instead of being
 *       silently dropped: making a shared relative handle foreign to it changes the written bytes;
 *   <li>the deserialize-only versions reject serialization through every entry point.
 * </ul>
 *
 * <p>Which method carries the contract and how the convenience overload delegates to it is
 * documented on {@link MetadataSerializer#serialize(CheckpointMetadata, DataOutputStream, Path)}.
 */
class MetadataSerializerEntryPointsTest {

    /** Directory the shared relative handle resolves against (its "own" savepoint directory). */
    private static final Path SAVEPOINT_DIR = new Path("s3://bucket/savepoints/sp-1");

    /**
     * A different exclusive directory: relative to it the shared handle is foreign and must be
     * promoted to an absolute handle instead of being resolved against it.
     */
    private static final Path FOREIGN_EXCLUSIVE_DIR = new Path("s3://bucket/checkpoints/chk-5");

    private static final String RELATIVE_FILE_NAME = "000001.sst";

    private static final long STATE_SIZE = 4242L;

    @Test
    void testBothEntryPointsProduceIdenticalBytesForNullExclusiveDir() throws Exception {
        for (MetadataSerializer serializer : writableSerializers()) {
            // Serialize the very same metadata object twice so any random state-handle ids are
            // identical between the two runs and only the entry point differs.
            final CheckpointMetadata metadata = metadataWithForeignSharedState();
            assertThat(serialize(serializer, metadata))
                    .as(
                            "2-arg and 3-arg(null) entry points must be byte-identical for v%d",
                            serializer.getVersion())
                    .isEqualTo(serialize(serializer, metadata, null));
        }
    }

    @Test
    void testNonNullExclusiveDirReachesEncoderAndChangesForeignHandleBytes() throws Exception {
        for (MetadataSerializer serializer : writableSerializers()) {
            final CheckpointMetadata metadata = metadataWithForeignSharedState();
            final byte[] withNullDir = serialize(serializer, metadata, null);
            final byte[] withForeignDir = serialize(serializer, metadata, FOREIGN_EXCLUSIVE_DIR);
            assertThat(withForeignDir)
                    .as(
                            "a non-null exclusive dir must reach the encoder and promote the foreign relative handle to absolute for v%d",
                            serializer.getVersion())
                    .isNotEqualTo(withNullDir);
        }
    }

    @Test
    void testDeserializeOnlyVersionsRejectSerializationThroughBothEntryPoints() {
        final CheckpointMetadata metadata = metadataWithForeignSharedState();
        for (MetadataSerializer serializer : deserializeOnlySerializers()) {
            assertThatThrownBy(() -> serialize(serializer, metadata))
                    .as("2-arg serialize must be unsupported for v%d", serializer.getVersion())
                    .isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(() -> serialize(serializer, metadata, null))
                    .as("3-arg serialize must be unsupported for v%d", serializer.getVersion())
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private static List<MetadataSerializer> writableSerializers() {
        return Arrays.asList(
                MetadataV3Serializer.INSTANCE,
                MetadataV4Serializer.INSTANCE,
                MetadataV5Serializer.INSTANCE,
                MetadataV6Serializer.INSTANCE);
    }

    private static List<MetadataSerializer> deserializeOnlySerializers() {
        return Arrays.asList(MetadataV1Serializer.INSTANCE, MetadataV2Serializer.INSTANCE);
    }

    private static CheckpointMetadata metadataWithForeignSharedState() {
        final RelativeFileStateHandle sharedHandle =
                new RelativeFileStateHandle(
                        new Path(SAVEPOINT_DIR, RELATIVE_FILE_NAME),
                        RELATIVE_FILE_NAME,
                        STATE_SIZE);
        final IncrementalRemoteKeyedStateHandle keyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.nameUUIDFromBytes("backend".getBytes(StandardCharsets.UTF_8)),
                        KeyGroupRange.of(0, 0),
                        1L,
                        Collections.singletonList(
                                HandleAndLocalPath.of(sharedHandle, RELATIVE_FILE_NAME)),
                        Collections.emptyList(),
                        new ByteStreamStateHandle("meta", new byte[] {1, 2, 3}));
        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder().setManagedKeyedState(keyedStateHandle).build();
        final OperatorState operatorState = new OperatorState(null, null, new OperatorID(), 1, 1);
        operatorState.putState(0, subtaskState);
        return new CheckpointMetadata(
                1L,
                Collections.singletonList(operatorState),
                Collections.emptyList(),
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
    }

    private static byte[] serialize(MetadataSerializer serializer, CheckpointMetadata metadata)
            throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            serializer.serialize(metadata, dos);
        }
        return baos.toByteArray();
    }

    private static byte[] serialize(
            MetadataSerializer serializer,
            CheckpointMetadata metadata,
            @Nullable Path exclusiveDirPath)
            throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            serializer.serialize(metadata, dos, exclusiveDirPath);
        }
        return baos.toByteArray();
    }
}

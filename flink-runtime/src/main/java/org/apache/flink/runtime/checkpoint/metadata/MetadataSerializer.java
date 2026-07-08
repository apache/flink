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
import org.apache.flink.core.io.Versioned;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Deserializer for checkpoint metadata. Different deserializers exist to deserialize from different
 * format versions.
 *
 * <p>Version-specific serializers are accessed via the {@link MetadataSerializers} helper.
 */
public interface MetadataSerializer extends Versioned {

    /**
     * Deserializes a savepoint from an input stream.
     *
     * @param dis Input stream to deserialize savepoint from
     * @param userCodeClassLoader the user code class loader
     * @param externalPointer the external pointer of the given checkpoint
     * @return The deserialized savepoint
     * @throws IOException Serialization failures are forwarded
     */
    CheckpointMetadata deserialize(
            DataInputStream dis, ClassLoader userCodeClassLoader, String externalPointer)
            throws IOException;

    /**
     * Serializes a savepoint or checkpoint metadata to an output stream without knowledge of the
     * exclusive directory the metadata is written into.
     *
     * <p>This is a convenience variant of {@link #serialize(CheckpointMetadata, DataOutputStream,
     * Path)} with a {@code null} exclusive directory: relative file references keep the relative
     * encoding unconditionally and are resolved against whatever directory the metadata is later
     * read from, which is only correct if every referenced file actually lives in that directory.
     *
     * @throws IOException Serialization failures are forwarded
     */
    default void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos)
            throws IOException {
        serialize(checkpointMetadata, dos, null);
    }

    /**
     * Serializes a savepoint or checkpoint metadata to an output stream, knowing the exclusive
     * directory into which the metadata (and the checkpoint/savepoint it describes) is being
     * written.
     *
     * <p>The exclusive directory is used to keep the on-disk references self-consistent: a relative
     * file handle that does not belong to {@code exclusiveDirPath} (e.g. a savepoint SST reused by
     * the first incremental checkpoint after a CLAIM-mode restore) is persisted with its absolute
     * path instead of being (incorrectly) resolved against {@code exclusiveDirPath} on recovery.
     *
     * <p>This is the method implementations must provide (rather than a default dropping the
     * exclusive directory), so that a serializer cannot silently lose the exclusive-directory
     * information.
     *
     * @param exclusiveDirPath the exclusive directory of the checkpoint/savepoint being written, or
     *     {@code null} if it is unknown.
     * @throws IOException Serialization failures are forwarded
     */
    void serialize(
            CheckpointMetadata checkpointMetadata,
            DataOutputStream dos,
            @Nullable Path exclusiveDirPath)
            throws IOException;
}

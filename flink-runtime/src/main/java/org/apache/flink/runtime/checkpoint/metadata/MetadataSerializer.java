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
import org.apache.flink.runtime.checkpoint.Checkpoints;

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
     * Serializes a savepoint or checkpoint metadata to an output stream.
     *
     * <p>Implementations check every relative file reference against {@code exclusiveDirPath}:
     * references to files inside that directory keep the relative encoding, references to files
     * anywhere else (e.g. a savepoint SST reused by the first incremental checkpoint after a
     * CLAIM-mode restore) are persisted with their absolute path, because the relative form would
     * be resolved against the wrong directory on recovery. See {@link Checkpoints} for what the
     * exclusive directory is.
     *
     * @param exclusiveDirPath the directory that will contain the metadata file (the checkpoint's
     *     exclusive directory), or {@code null} to keep every relative reference relative
     *     regardless of where its file lives
     * @throws IOException Serialization failures are forwarded
     */
    void serialize(
            CheckpointMetadata checkpointMetadata,
            DataOutputStream dos,
            @Nullable Path exclusiveDirPath)
            throws IOException;
}

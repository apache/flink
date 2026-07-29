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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * An output stream for checkpoint metadata.
 *
 * <p>This stream is similar to the {@link CheckpointStateOutputStream}, but for metadata files
 * rather than data files.
 *
 * <p>This stream always creates a file, regardless of the amount of data written.
 */
public abstract class CheckpointMetadataOutputStream extends FSDataOutputStream {

    /**
     * Closes the stream after all metadata was written and finalizes the checkpoint location.
     *
     * @return An object representing a finalized checkpoint storage location.
     * @throws IOException Thrown, if the stream cannot be closed or the finalization fails.
     */
    public abstract CompletedCheckpointStorageLocation closeAndFinalizeCheckpoint()
            throws IOException;

    /**
     * Returns the exclusive directory of the checkpoint/savepoint whose metadata is written through
     * this stream, or {@code null} if it is unknown (e.g. for storage that does not lay out state
     * in an exclusive directory).
     *
     * <p>This is used while serializing the metadata to keep relative file references
     * self-consistent (see {@code MetadataV2V3SerializerBase}).
     *
     * <p>Implementations whose state files can be referenced through {@code
     * RelativeFileStateHandle}s (in particular any storage laying out state in a per-checkpoint
     * exclusive directory) must override this method. Returning {@code null} keeps every relative
     * handle relative unconditionally, which is correct as long as every relative handle points
     * into the directory the metadata is written to; a relative handle pointing elsewhere would be
     * resolved against the wrong directory on recovery, making the checkpoint unrestorable. For
     * storage without an exclusive directory layout, returning {@code null} is the right choice.
     */
    @Nullable
    public Path getExclusiveCheckpointDir() {
        return null;
    }

    /**
     * This method should close the stream, if has not been closed before. If this method actually
     * closes the stream, it should delete/release the resource behind the stream, such as the file
     * that the stream writes to.
     *
     * <p>The above implies that this method is intended to be the "unsuccessful close", such as
     * when cancelling the stream writing, or when an exception occurs. Closing the stream for the
     * successful case must go through {@link #closeAndFinalizeCheckpoint()}.
     *
     * @throws IOException Thrown, if the stream cannot be closed.
     */
    @Override
    public abstract void close() throws IOException;
}

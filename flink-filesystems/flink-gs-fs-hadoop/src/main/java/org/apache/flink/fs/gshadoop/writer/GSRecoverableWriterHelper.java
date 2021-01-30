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

package org.apache.flink.fs.gshadoop.writer;

import com.google.cloud.storage.BlobId;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/** This class abstracts away the Google Storage implementation details, allowing for mocking. */
public abstract class GSRecoverableWriterHelper {

    /**
     * Provides access to the inner storage interface.
     *
     * @return Storage interface.
     */
    public abstract Storage getStorage();

    /**
     * Wraps the operations performed against the {@link com.google.cloud.storage.Storage} class.
     */
    public interface Storage {

        /**
         * Creates a writer object for a resumable upload.
         *
         * @param blobId The blob id to write to
         * @param contentType The content type for the upload
         * @param chunkSize The min chunk size for the upload; if zero, the default value will be
         *     used
         * @return The writer interface
         */
        Writer createWriter(BlobId blobId, String contentType, int chunkSize);

        /**
         * Determines whether a given blob exists.
         *
         * @param blobId The blob id to check
         * @return True if the blob exists; false otherwise
         */
        boolean exists(BlobId blobId);

        /**
         * Copies a blob from one location to another. Note this does not delete the source blob, it
         * is a copy, not a move.
         *
         * @param sourceBlobId The source blob id
         * @param targetBlobId The target blob id
         */
        void copy(BlobId sourceBlobId, BlobId targetBlobId);

        /**
         * Deletes a blob. Note that this does not fail if the blob doesn't exist.
         *
         * @param blobId The blob id to delete
         * @return True if the blob did exist and was deleted; false if the blob didn't exist
         */
        boolean delete(BlobId blobId);
    }

    /** Wraps the operations performed by the {@link com.google.cloud.WriteChannel} interface. */
    public interface Writer {

        /**
         * Writes a buffer of bytes.
         *
         * @param byteBuffer The bytes to write
         * @return The number of bytes written, this can be less than the entire buffer size
         * @throws IOException On failure of the underlying write
         */
        int write(ByteBuffer byteBuffer) throws IOException;

        /**
         * Closes the writer.
         *
         * @throws IOException On failure of the underlying close
         */
        void close() throws IOException;

        /**
         * Captures the state of the writer so that it can be restored later.
         *
         * @return The writer state
         */
        WriterState capture();
    }

    /**
     * Wraps the operations performed by the {@link com.google.cloud.RestorableState} interface. for
     * a {@link com.google.cloud.WriteChannel}.
     *
     * <p>This is serializable because, in the default implementation of this helper, the writer
     * state is represented by an instance of {@link com.google.cloud.RestorableState}, which is an
     * opaque object that is serializable, and Java serialization must be used to persist it.
     */
    public interface WriterState extends Serializable {

        /**
         * Restores a previously captured writer.
         *
         * @return The restored writer
         */
        Writer restore();
    }
}

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

package org.apache.flink.fs.gs.storage;

import com.google.cloud.storage.BlobId;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Abstract blob storage, used to simplify interface to Google storage and make it mockable. */
public interface BlobStorage {

    /** Abstract blob metadata. */
    interface BlobMetadata {

        /**
         * The crc32 checksum for the blob.
         *
         * @return The checksum
         */
        String getChecksum();
    }

    /** Abstract blob write channel. */
    interface WriteChannel {

        /**
         * Sets the chunk size for upload.
         *
         * @param chunkSize The chunk size
         */
        void setChunkSize(int chunkSize);

        /**
         * Writes data to the channel.
         *
         * @param content The data buffer
         * @param start Start offset in the data buffer
         * @param length Number of bytes to write
         * @return The number of bytes written
         * @throws IOException On underlying failure
         */
        int write(byte[] content, int start, int length) throws IOException;

        /**
         * Closes the channel.
         *
         * @throws IOException On underlying failure
         */
        void close() throws IOException;
    }

    /**
     * Creates a write channel.
     *
     * @param blobId The blob id to write
     * @param uploadContentType The content type for the upload
     * @return The WriteChannel helper
     */
    WriteChannel write(BlobId blobId, String uploadContentType);

    /**
     * Gets blob metadata.
     *
     * @param blobId The blob id to check
     * @return The blob metadata, if the blob exists. Empty if the blob doesn't exist.
     */
    Optional<BlobMetadata> getMetadata(BlobId blobId);

    /**
     * Lists all the blobs in a bucket matching a given prefix.
     *
     * @param bucketName The bucket name
     * @param prefix The object prefix
     * @return The found blobs ids
     */
    List<BlobId> list(String bucketName, String prefix);

    /**
     * Copies from a source blob id to a target blob id. Does not delete the source blob.
     *
     * @param sourceBlobId The source blob id
     * @param targetBlobId The target glob id
     */
    void copy(BlobId sourceBlobId, BlobId targetBlobId);

    /**
     * Composes multiple blobs into one. Does not delete any of the source blobs.
     *
     * @param sourceBlobIds The source blob ids to combine, max of 32
     * @param targetBlobId The target blob id
     * @param contentType The content type for the composed blob.
     */
    void compose(List<BlobId> sourceBlobIds, BlobId targetBlobId, String contentType);

    /**
     * Deletes blobs. Note that this does not fail if blobs don't exist.
     *
     * @param blobIds The blob ids to delete
     * @return The results of each delete operation.
     */
    List<Boolean> delete(Iterable<BlobId> blobIds);
}

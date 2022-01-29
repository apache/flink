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

import org.apache.flink.configuration.MemorySize;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/** Abstract blob storage, used to simplify interface to Google storage and make it mockable. */
public interface GSBlobStorage {

    /**
     * Creates a write channel with the default chunk size.
     *
     * @param blobIdentifier The blob identifier to which to write
     * @return The WriteChannel helper
     */
    WriteChannel writeBlob(GSBlobIdentifier blobIdentifier);

    /**
     * Creates a write channel with the specified chunk size.
     *
     * @param blobIdentifier The blob identifier to which to write
     * @param chunkSize The chunk size, must be > 0
     * @return The WriteChannel helper
     */
    WriteChannel writeBlob(GSBlobIdentifier blobIdentifier, MemorySize chunkSize);

    /**
     * Create an empty blob.
     *
     * @param blobIdentifier The blob to create
     */
    void createBlob(GSBlobIdentifier blobIdentifier);

    /**
     * Gets blob metadata.
     *
     * @param blobIdentifier The blob identifier
     * @return The blob metadata, if the blob exists. Empty if the blob doesn't exist.
     */
    Optional<BlobMetadata> getMetadata(GSBlobIdentifier blobIdentifier);

    /**
     * Lists all the blobs in a bucket matching a given prefix.
     *
     * @param bucketName The bucket name
     * @param prefix The object prefix
     * @return The found blobs ids
     */
    List<GSBlobIdentifier> list(String bucketName, String prefix);

    /**
     * Copies from a source blob id to a target blob id. Does not delete the source blob.
     *
     * @param sourceBlobIdentifier The source blob identifier
     * @param targetBlobIdentifier The target glob identifier
     */
    void copy(GSBlobIdentifier sourceBlobIdentifier, GSBlobIdentifier targetBlobIdentifier);

    /**
     * Composes multiple blobs into one. Does not delete any of the source blobs.
     *
     * @param sourceBlobIdentifiers The source blob identifiers to combine, max of 32
     * @param targetBlobIdentifier The target blob identifier
     */
    void compose(
            List<GSBlobIdentifier> sourceBlobIdentifiers, GSBlobIdentifier targetBlobIdentifier);

    /**
     * Deletes blobs. Note that this does not fail if blobs don't exist.
     *
     * @param blobIdentifiers The blob identifiers to delete
     * @return The results of each delete operation.
     */
    List<Boolean> delete(Iterable<GSBlobIdentifier> blobIdentifiers);

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
}

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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.utils.ChecksumUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.hash.Hasher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/** A wrapper for a blob storage write channel that computes a checksum. */
class GSChecksumWriteChannel {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSChecksumWriteChannel.class);

    /** The blob storage instance. */
    private final GSBlobStorage storage;

    /** The write channel. */
    private final GSBlobStorage.WriteChannel writeChannel;

    /** The blob identifier to write. */
    private final GSBlobIdentifier blobIdentifier;

    /** The hasher used to compute the checksum. */
    private final Hasher hasher;

    /**
     * Construct a recoverable writer write channel.
     *
     * @param storage The storage instance
     * @param writeChannel The write channel instance
     * @param blobIdentifier The blob identifier to write
     */
    public GSChecksumWriteChannel(
            GSBlobStorage storage,
            GSBlobStorage.WriteChannel writeChannel,
            GSBlobIdentifier blobIdentifier) {
        LOGGER.trace("Creating GSChecksumWriteChannel for blob {}", blobIdentifier);
        this.storage = Preconditions.checkNotNull(storage);
        this.writeChannel = Preconditions.checkNotNull(writeChannel);
        this.blobIdentifier = Preconditions.checkNotNull(blobIdentifier);
        this.hasher = ChecksumUtils.CRC_HASH_FUNCTION.newHasher();
    }

    /**
     * Writes bytes to the underlying channel and updates checksum.
     *
     * @param content The content to write
     * @param start The start position
     * @param length The number of bytes to write
     * @return The number of bytes written
     * @throws IOException On underlying failure
     */
    public int write(byte[] content, int start, int length) throws IOException {
        LOGGER.trace("Writing {} bytes to blob {}", length, blobIdentifier);
        Preconditions.checkNotNull(content);
        Preconditions.checkArgument(start >= 0);
        Preconditions.checkArgument(length >= 0);

        hasher.putBytes(content, start, length);
        return writeChannel.write(content, start, length);
    }

    /**
     * Closes the channel and validates the checksum against the storage. Manually verifying
     * checksums for streaming uploads is recommended by Google, see here:
     * https://cloud.google.com/storage/docs/streaming
     *
     * @throws IOException On underlying failure or non-matching checksums
     */
    public void close() throws IOException {
        LOGGER.trace("Closing write channel to blob {}", blobIdentifier);

        // close channel and get blob metadata
        writeChannel.close();
        Optional<GSBlobStorage.BlobMetadata> blobMetadata = storage.getMetadata(blobIdentifier);
        if (!blobMetadata.isPresent()) {
            throw new IOException(
                    String.format("Failed to read metadata for blob %s", blobIdentifier));
        }

        // make sure checksums match
        String writeChecksum = ChecksumUtils.convertChecksumToString(hasher.hash().asInt());
        String blobChecksum = blobMetadata.get().getChecksum();
        if (!writeChecksum.equals(blobChecksum)) {
            throw new IOException(
                    String.format(
                            "Checksum mismatch writing blob %s: expected %s but found %s",
                            blobIdentifier, writeChecksum, blobChecksum));
        }
    }
}

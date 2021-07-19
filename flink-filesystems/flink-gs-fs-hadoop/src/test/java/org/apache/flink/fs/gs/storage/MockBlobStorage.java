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
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.fs.gs.utils.ChecksumUtils;

import com.google.cloud.storage.StorageException;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** Mock blob storage implementation, using in-memory structures. */
public class MockBlobStorage implements GSBlobStorage {

    /** Mock blob value with metadata. */
    public static class BlobValue {

        public final byte[] content;

        BlobValue(byte[] content) {
            this.content = content;
        }
    }

    /** The mock blob metadata. */
    static class BlobMetadata implements GSBlobStorage.BlobMetadata {

        private final BlobValue blobValue;

        BlobMetadata(BlobValue blobValue) {
            this.blobValue = blobValue;
        }

        @Override
        public String getChecksum() {
            int checksum = ChecksumUtils.CRC_HASH_FUNCTION.hashBytes(blobValue.content).asInt();
            return ChecksumUtils.convertChecksumToString(checksum);
        }
    }

    /** The mock write channel, which writes to the memory-based storage. */
    public class WriteChannel implements GSBlobStorage.WriteChannel {

        private final GSBlobIdentifier blobIdentifier;

        @Nullable public final MemorySize chunkSize;

        private final ByteArrayOutputStream stream;

        private boolean closed;

        WriteChannel(GSBlobIdentifier blobIdentifier, @Nullable MemorySize chunkSize) {
            this.blobIdentifier = blobIdentifier;
            this.stream = new ByteArrayOutputStream();
            this.closed = false;
            this.chunkSize = chunkSize;
        }

        @Override
        public int write(byte[] content, int start, int length) throws IOException {
            if (closed) {
                throw new ClosedChannelException();
            }
            stream.write(content, start, length);
            return length;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                stream.close();
                blobs.put(blobIdentifier, new BlobValue(stream.toByteArray()));
                closed = true;
            }
        }
    }

    public final Map<GSBlobIdentifier, BlobValue> blobs;

    public MockBlobStorage() {
        this.blobs = new HashMap<>();
    }

    @Override
    public GSBlobStorage.WriteChannel writeBlob(GSBlobIdentifier blobId) {
        return new WriteChannel(blobId, null);
    }

    @Override
    public GSBlobStorage.WriteChannel writeBlob(GSBlobIdentifier blobId, MemorySize chunkSize) {
        return new WriteChannel(blobId, chunkSize);
    }

    @Override
    public Optional<GSBlobStorage.BlobMetadata> getMetadata(GSBlobIdentifier blobIdentifier) {
        BlobValue blobValue = blobs.get(blobIdentifier);
        if (blobValue != null) {
            return Optional.of(new BlobMetadata(blobValue));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<GSBlobIdentifier> list(String bucketName, String prefix) {
        return blobs.keySet().stream()
                .filter(
                        blobId ->
                                blobId.bucketName.equals(bucketName)
                                        && blobId.objectName.startsWith(prefix))
                .collect(Collectors.toList());
    }

    @Override
    public void copy(GSBlobIdentifier sourceBlobIdentifier, GSBlobIdentifier targetBlobIdentifier) {
        BlobValue blobValue = blobs.get(sourceBlobIdentifier);
        if (blobValue == null) {
            throw new StorageException(404, "Copy source not found");
        }
        blobs.put(targetBlobIdentifier, blobValue);
    }

    @Override
    public void compose(
            List<GSBlobIdentifier> sourceBlobIdentifiers, GSBlobIdentifier targetBlobIdentifier) {

        if (sourceBlobIdentifiers.size() > BlobUtils.COMPOSE_MAX_BLOBS) {
            throw new UnsupportedOperationException();
        }

        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            // write all the source blobs into the stream
            for (GSBlobIdentifier blobIdentifier : sourceBlobIdentifiers) {
                BlobValue sourceBlobValue = blobs.get(blobIdentifier);
                if (sourceBlobValue == null) {
                    throw new StorageException(404, "Compose source not found");
                }
                stream.write(sourceBlobValue.content);
            }

            // write the resulting blob
            BlobValue targetBlobValue = new BlobValue(stream.toByteArray());
            blobs.put(targetBlobIdentifier, targetBlobValue);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Boolean> delete(Iterable<GSBlobIdentifier> blobIdentifiers) {
        return StreamSupport.stream(blobIdentifiers.spliterator(), false)
                .map(blobId -> blobs.remove(blobId) != null)
                .collect(Collectors.toList());
    }
}

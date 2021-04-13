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

import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.fs.gs.utils.ChecksumUtils;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageException;

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
public class MockBlobStorage implements BlobStorage {

    /** Mock blob value with metadata. */
    public static class BlobValue {

        public final byte[] content;
        public final String contentType;

        BlobValue(byte[] content, String contentType) {
            this.content = content;
            this.contentType = contentType;
        }
    }

    /** The mock blob metadata. */
    static class BlobMetadata implements BlobStorage.BlobMetadata {

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
    public class WriteChannel implements BlobStorage.WriteChannel {

        private final BlobId blobId;

        private final String contentType;

        public int chunkSize;

        private final ByteArrayOutputStream stream;

        private boolean closed;

        WriteChannel(BlobId blobId, String contentType) {
            this.blobId = blobId;
            this.contentType = contentType;
            this.stream = new ByteArrayOutputStream();
            this.closed = false;
        }

        @Override
        public void setChunkSize(int chunkSize) {
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
                blobs.put(blobId, new BlobValue(stream.toByteArray(), contentType));
                closed = true;
            }
        }
    }

    public final Map<BlobId, BlobValue> blobs;

    public MockBlobStorage() {
        this.blobs = new HashMap<>();
    }

    @Override
    public BlobStorage.WriteChannel write(BlobId blobId, String contentType) {
        return new WriteChannel(blobId, contentType);
    }

    @Override
    public Optional<BlobStorage.BlobMetadata> getMetadata(BlobId blobId) {
        BlobValue blobValue = blobs.get(blobId);
        if (blobValue != null) {
            return Optional.of(new BlobMetadata(blobValue));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public List<BlobId> list(String bucketName, String prefix) {
        return blobs.keySet().stream()
                .filter(
                        blobId ->
                                blobId.getBucket().equals(bucketName)
                                        && blobId.getName().startsWith(prefix))
                .collect(Collectors.toList());
    }

    @Override
    public void copy(BlobId sourceBlobId, BlobId targetBlobId) {
        BlobValue blobValue = blobs.get(sourceBlobId);
        if (blobValue == null) {
            throw new StorageException(404, "Copy source not found");
        }
        blobs.put(targetBlobId, blobValue);
    }

    @Override
    public void compose(List<BlobId> sourceBlobIds, BlobId targetBlobId, String contentType) {

        if (sourceBlobIds.size() > BlobUtils.COMPOSE_MAX_BLOBS) {
            throw new UnsupportedOperationException();
        }

        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {

            // write all the source blobs into the stream
            for (BlobId blobId : sourceBlobIds) {
                BlobValue sourceBlobValue = blobs.get(blobId);
                if (sourceBlobValue == null) {
                    throw new StorageException(404, "Compose source not found");
                }
                stream.write(sourceBlobValue.content);
            }

            // write the resulting blob
            BlobValue targetBlobValue = new BlobValue(stream.toByteArray(), contentType);
            blobs.put(targetBlobId, targetBlobValue);

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public List<Boolean> delete(Iterable<BlobId> blobIds) {
        return StreamSupport.stream(blobIds.spliterator(), false)
                .map(blobId -> blobs.remove(blobId) != null)
                .collect(Collectors.toList());
    }
}

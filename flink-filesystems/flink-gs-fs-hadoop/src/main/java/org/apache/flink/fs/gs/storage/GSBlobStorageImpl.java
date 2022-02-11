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
import org.apache.flink.util.Preconditions;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** BlobStorage implementation for Google storage. */
public class GSBlobStorageImpl implements GSBlobStorage {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSBlobStorageImpl.class);

    /** The wrapped Google Storage instance. */
    private final Storage storage;

    /**
     * Constructs a GSBlobStorage instance.
     *
     * @param storage The wrapped Google Storage instance.
     */
    public GSBlobStorageImpl(Storage storage) {
        LOGGER.debug("Creating GSBlobStorageImpl");
        this.storage = Preconditions.checkNotNull(storage);
    }

    @Override
    public GSBlobStorage.WriteChannel writeBlob(GSBlobIdentifier blobIdentifier) {
        LOGGER.trace("Creating writeable blob for identifier {}", blobIdentifier);
        Preconditions.checkNotNull(blobIdentifier);

        BlobInfo blobInfo = BlobInfo.newBuilder(blobIdentifier.getBlobId()).build();
        com.google.cloud.WriteChannel writeChannel = storage.writer(blobInfo);
        return new WriteChannel(blobIdentifier, writeChannel);
    }

    @Override
    public GSBlobStorage.WriteChannel writeBlob(
            GSBlobIdentifier blobIdentifier, MemorySize chunkSize) {
        LOGGER.trace(
                "Creating writeable blob for identifier {} with chunk size {}",
                blobIdentifier,
                chunkSize);
        Preconditions.checkNotNull(blobIdentifier);
        Preconditions.checkArgument(chunkSize.getBytes() > 0);

        BlobInfo blobInfo = BlobInfo.newBuilder(blobIdentifier.getBlobId()).build();
        com.google.cloud.WriteChannel writeChannel = storage.writer(blobInfo);
        writeChannel.setChunkSize((int) chunkSize.getBytes());
        return new WriteChannel(blobIdentifier, writeChannel);
    }

    @Override
    public void createBlob(GSBlobIdentifier blobIdentifier) {
        LOGGER.trace("Creating empty blob {}", blobIdentifier);
        Preconditions.checkNotNull(blobIdentifier);

        BlobInfo blobInfo = BlobInfo.newBuilder(blobIdentifier.getBlobId()).build();
        storage.create(blobInfo);
    }

    @Override
    public Optional<GSBlobStorage.BlobMetadata> getMetadata(GSBlobIdentifier blobIdentifier) {
        LOGGER.trace("Getting metadata for blob {}", blobIdentifier);
        Preconditions.checkNotNull(blobIdentifier);

        Optional<GSBlobStorage.BlobMetadata> metadata =
                Optional.ofNullable(storage.get(blobIdentifier.getBlobId()))
                        .map(blob -> new BlobMetadata(blobIdentifier, blob));
        if (metadata.isPresent()) {
            LOGGER.trace("Found metadata for blob {}", blobIdentifier);
        } else {
            LOGGER.trace("Did not find metadata for blob {}", blobIdentifier);
        }
        return metadata;
    }

    @Override
    public List<GSBlobIdentifier> list(String bucketName, String objectPrefix) {
        LOGGER.trace("Listing blobs in bucket {} with object prefix {}", bucketName, objectPrefix);
        Preconditions.checkNotNull(bucketName);
        Preconditions.checkNotNull(objectPrefix);

        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(objectPrefix));
        List<GSBlobIdentifier> blobIdentifiers =
                StreamSupport.stream(blobs.iterateAll().spliterator(), false)
                        .map(BlobInfo::getBlobId)
                        .map(GSBlobIdentifier::fromBlobId)
                        .collect(Collectors.toList());
        LOGGER.trace(
                "Found blobs in bucket {} with object prefix {}: {}",
                bucketName,
                objectPrefix,
                blobIdentifiers);
        return blobIdentifiers;
    }

    @Override
    public void copy(GSBlobIdentifier sourceBlobIdentifier, GSBlobIdentifier targetBlobIdentifier) {
        LOGGER.trace("Copying blob {} to blob {}", sourceBlobIdentifier, targetBlobIdentifier);
        Preconditions.checkNotNull(sourceBlobIdentifier);
        Preconditions.checkNotNull(targetBlobIdentifier);

        storage.get(sourceBlobIdentifier.getBlobId())
                .copyTo(targetBlobIdentifier.getBlobId())
                .getResult();
    }

    @Override
    public void compose(
            List<GSBlobIdentifier> sourceBlobIdentifiers, GSBlobIdentifier targetBlobIdentifier) {
        LOGGER.trace("Composing blobs {} to blob {}", sourceBlobIdentifiers, targetBlobIdentifier);
        Preconditions.checkNotNull(sourceBlobIdentifiers);
        Preconditions.checkArgument(sourceBlobIdentifiers.size() > 0);
        Preconditions.checkArgument(sourceBlobIdentifiers.size() <= BlobUtils.COMPOSE_MAX_BLOBS);
        Preconditions.checkNotNull(targetBlobIdentifier);

        // build a request to compose all the source blobs into the target blob
        Storage.ComposeRequest.Builder builder = Storage.ComposeRequest.newBuilder();
        BlobInfo targetBlobInfo = BlobInfo.newBuilder(targetBlobIdentifier.getBlobId()).build();
        builder.setTarget(targetBlobInfo);
        for (GSBlobIdentifier blobIdentifier : sourceBlobIdentifiers) {
            builder.addSource(blobIdentifier.objectName);
        }

        Storage.ComposeRequest request = builder.build();
        storage.compose(request);
    }

    @Override
    public List<Boolean> delete(Iterable<GSBlobIdentifier> blobIdentifiers) {
        LOGGER.trace("Deleting blobs {}", blobIdentifiers);
        Preconditions.checkNotNull(blobIdentifiers);

        Iterable<BlobId> blobIds =
                StreamSupport.stream(blobIdentifiers.spliterator(), false)
                        .map(GSBlobIdentifier::getBlobId)
                        .collect(Collectors.toList());
        return storage.delete(blobIds);
    }

    /** Blob metadata, wraps Google storage Blob. */
    static class BlobMetadata implements GSBlobStorage.BlobMetadata {

        private final GSBlobIdentifier blobIdentifier;

        private final Blob blob;

        private BlobMetadata(GSBlobIdentifier blobIdentifier, Blob blob) {
            this.blobIdentifier = Preconditions.checkNotNull(blobIdentifier);
            this.blob = Preconditions.checkNotNull(blob);
        }

        @Override
        public String getChecksum() {
            LOGGER.trace("Getting checksum for blob {}", blobIdentifier);
            String checksum = blob.getCrc32c();
            LOGGER.trace("Found checksum for blob {}: {}", blobIdentifier, checksum);
            return checksum;
        }
    }

    /** Blob write channel, wraps Google storage WriteChannel. */
    static class WriteChannel implements GSBlobStorage.WriteChannel {

        private final GSBlobIdentifier blobIdentifier;

        private final com.google.cloud.WriteChannel writeChannel;

        private WriteChannel(
                GSBlobIdentifier blobIdentifier, com.google.cloud.WriteChannel writeChannel) {
            this.blobIdentifier = Preconditions.checkNotNull(blobIdentifier);
            this.writeChannel = Preconditions.checkNotNull(writeChannel);
        }

        @Override
        public int write(byte[] content, int start, int length) throws IOException {
            LOGGER.trace("Writing {} bytes to blob {}", length, blobIdentifier);
            Preconditions.checkNotNull(content);
            Preconditions.checkArgument(start >= 0);
            Preconditions.checkArgument(length >= 0);

            ByteBuffer byteBuffer = ByteBuffer.wrap(content, start, length);
            int written = writeChannel.write(byteBuffer);
            LOGGER.trace("Wrote {} bytes to blob {}", written, blobIdentifier);
            return written;
        }

        @Override
        public void close() throws IOException {
            LOGGER.trace("Closing write channel to blob {}", blobIdentifier);
            writeChannel.close();
        }
    }
}

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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;

/** The data output stream implementation for the GS recoverable writer. */
class GSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GSRecoverableFsDataOutputStream.class);

    /** The underlying blob storage. */
    private final GSBlobStorage storage;

    /** The GS file system options. */
    private final GSFileSystemOptions options;

    /** The blob id to which the recoverable write operation is writing. */
    private final GSBlobIdentifier finalBlobIdentifier;

    /** The write position, i.e. number of bytes that have been written so far. */
    private long position;

    /** Indicates if the write has been closed. */
    private boolean closed;

    /** The object ids for the temporary objects that should be composed to form the final blob. */
    private final ArrayList<UUID> componentObjectIds;

    /**
     * The current write channel, if one exists. A channel is created when one doesn't exist and
     * bytes are written, and the channel is closed/destroyed when explicitly closed by the consumer
     * (via close or closeForCommit) or when the data output stream is persisted (via persist).
     * Calling persist does not close the data output stream, so it's possible that more bytes will
     * be written, which will cause another channel to be created. So, multiple write channels may
     * be created and destroyed during the lifetime of the data output stream.
     */
    @Nullable private GSChecksumWriteChannel currentWriteChannel;

    /**
     * Constructs a new, initially empty output stream.
     *
     * @param storage The storage implementation
     * @param options The file system options
     * @param finalBlobIdentifier The final blob identifier to which to write
     */
    GSRecoverableFsDataOutputStream(
            GSBlobStorage storage,
            GSFileSystemOptions options,
            GSBlobIdentifier finalBlobIdentifier) {
        LOGGER.debug(
                "Creating new GSRecoverableFsDataOutputStream for blob {} with options {}",
                finalBlobIdentifier,
                options);
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
        this.finalBlobIdentifier = Preconditions.checkNotNull(finalBlobIdentifier);
        this.position = 0;
        this.closed = false;
        this.componentObjectIds = new ArrayList<>();
    }

    /**
     * Constructs an output stream from a recoverable.
     *
     * @param storage The storage implementation
     * @param options The file system options
     * @param recoverable The recoverable
     */
    GSRecoverableFsDataOutputStream(
            GSBlobStorage storage, GSFileSystemOptions options, GSResumeRecoverable recoverable) {
        LOGGER.debug(
                "Recovering GSRecoverableFsDataOutputStream for blob {} with options {}",
                recoverable.finalBlobIdentifier,
                options);
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
        this.finalBlobIdentifier = Preconditions.checkNotNull(recoverable.finalBlobIdentifier);
        Preconditions.checkArgument(recoverable.position >= 0);
        this.position = recoverable.position;
        this.closed = recoverable.closed;
        this.componentObjectIds = new ArrayList<>(recoverable.componentObjectIds);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(int byteValue) throws IOException {
        byte[] bytes = new byte[] {(byte) byteValue};
        write(bytes);
    }

    @Override
    public void write(@Nonnull byte[] content) throws IOException {
        Preconditions.checkNotNull(content);

        write(content, 0, content.length);
    }

    @Override
    public void write(@Nonnull byte[] content, int start, int length) throws IOException {
        Preconditions.checkNotNull(content);
        Preconditions.checkArgument(start >= 0);
        Preconditions.checkArgument(length >= 0);

        // if the data stream is already closed, throw an exception
        if (closed) {
            throw new IOException("Illegal attempt to write to closed output stream");
        }

        // if necessary, create a write channel
        if (currentWriteChannel == null) {
            LOGGER.debug("Creating write channel for blob {}", finalBlobIdentifier);
            currentWriteChannel = createWriteChannel();
        }

        // write to the stream. the docs say that, in some circumstances, though an attempt will be
        // made to write all of the requested bytes, there are some cases where only some bytes will
        // be written. it's not clear whether this could ever happen with a Google storage
        // WriteChannel; in any case, recoverable writers don't support partial writes, so if this
        // ever happens, we must fail the write.:
        // https://docs.oracle.com/javase/7/docs/api/java/nio/channels/WritableByteChannel.html#write(java.nio.ByteBuffer)
        LOGGER.trace("Writing {} bytes", length);
        int bytesWritten = currentWriteChannel.write(content, start, length);
        if (bytesWritten != length) {
            throw new IOException(
                    String.format(
                            "WriteChannel.write wrote %d of %d requested bytes, failing.",
                            bytesWritten, length));
        }

        // update count of total bytes written
        position += bytesWritten;
    }

    @Override
    public void flush() throws IOException {
        LOGGER.trace("Flushing write channel for blob {}", finalBlobIdentifier);
        closeWriteChannelIfExists();
    }

    @Override
    public void sync() throws IOException {
        LOGGER.trace("Syncing write channel for blob {}", finalBlobIdentifier);
        closeWriteChannelIfExists();
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        LOGGER.trace("Persisting write channel for blob {}", finalBlobIdentifier);
        closeWriteChannelIfExists();
        return createResumeRecoverable();
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("Closing write channel for blob {}", finalBlobIdentifier);
        closeWriteChannelIfExists();
        closed = true;
    }

    @Override
    public Committer closeForCommit() throws IOException {
        LOGGER.trace("Closing write channel for commit for blob {}", finalBlobIdentifier);
        close();
        return new GSRecoverableWriterCommitter(storage, options, createCommitRecoverable());
    }

    private GSCommitRecoverable createCommitRecoverable() {
        return new GSCommitRecoverable(finalBlobIdentifier, componentObjectIds);
    }

    private GSResumeRecoverable createResumeRecoverable() {
        return new GSResumeRecoverable(finalBlobIdentifier, componentObjectIds, position, closed);
    }

    private GSChecksumWriteChannel createWriteChannel() {

        // add a new component blob id for the new channel to write to
        UUID componentObjectId = UUID.randomUUID();
        componentObjectIds.add(componentObjectId);
        GSBlobIdentifier blobIdentifier =
                BlobUtils.getTemporaryBlobIdentifier(
                        finalBlobIdentifier, componentObjectId, options);

        // create the channel, using an explicit chunk size if specified in options
        Optional<MemorySize> writerChunkSize = options.getWriterChunkSize();
        GSBlobStorage.WriteChannel writeChannel =
                writerChunkSize.isPresent()
                        ? storage.writeBlob(blobIdentifier, writerChunkSize.get())
                        : storage.writeBlob(blobIdentifier);
        return new GSChecksumWriteChannel(storage, writeChannel, blobIdentifier);
    }

    private void closeWriteChannelIfExists() throws IOException {

        if (currentWriteChannel != null) {

            // close the channel, this causes all written data to be committed.
            // note that this also validates checksums and will throw an exception
            // if they don't match
            currentWriteChannel.close();
            currentWriteChannel = null;
        }
    }
}

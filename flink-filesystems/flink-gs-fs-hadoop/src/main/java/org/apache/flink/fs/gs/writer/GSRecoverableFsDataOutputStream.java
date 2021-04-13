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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.BlobStorage;
import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/** The data output stream implementation for the GS recoverable writer. */
class GSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    /** The underlying blob storage. */
    private final BlobStorage storage;

    /** The GS file system options. */
    private final GSFileSystemOptions options;

    /** The recoverable writer. */
    private final GSRecoverableWriter writer;

    /** The recoverable writer state. */
    private final GSRecoverableWriterState state;

    /**
     * The current write channel, if one exists. A channel is created when one doesn't exist and
     * bytes are written, and the channel is closed/destroyed when explicitly closed by the consumer
     * (via close or closeForCommit) or when the data output stream is persisted (via persist).
     * Calling persist does not close the data output stream, so it's possible that more bytes will
     * be written, which will cause another channel to be created. So, multiple write channels may
     * be created and destroyed during the lifetime of the data output stream.
     */
    @Nullable GSChecksumWriteChannel currentWriteChannel;

    GSRecoverableFsDataOutputStream(
            BlobStorage storage,
            GSFileSystemOptions options,
            GSRecoverableWriter writer,
            GSRecoverableWriterState state) {
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
        this.writer = Preconditions.checkNotNull(writer);
        this.state = Preconditions.checkNotNull(state);
    }

    @Override
    public long getPos() throws IOException {
        return state.bytesWritten;
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
        if (state.closed) {
            throw new IOException("Illegal attempt to write to closed output stream");
        }

        // if necessary, create a write channel
        if (currentWriteChannel == null) {
            currentWriteChannel = createWriteChannel();
        }

        // write to the stream. the docs say that, in some circumstances, though an attempt will be
        // made to write all of the requested bytes, there are some cases where only some bytes will
        // be written. it's not clear whether this could ever happen with a Google storage
        // WriteChannel; in any case, recoverable writers don't support partial writes, so if this
        // ever happens, we must fail the write.:
        // https://docs.oracle.com/javase/7/docs/api/java/nio/channels/WritableByteChannel.html#write(java.nio.ByteBuffer)
        int bytesWritten = currentWriteChannel.write(content, start, length);
        if (bytesWritten != length) {
            throw new IOException(
                    String.format(
                            "WriteChannel.write wrote %d of %d requested bytes, failing.",
                            bytesWritten, length));
        }

        // update count of total bytes written
        state.bytesWritten += length;
    }

    @Override
    public void flush() throws IOException {
        // not supported for GS, flushing frequency is controlled by the chunk size setting
        // https://googleapis.dev/java/google-cloud-clients/0.90.0-alpha/com/google/cloud/WriteChannel.html#setChunkSize-int-
    }

    @Override
    public void sync() throws IOException {
        // not supported for GS, flushing frequency is controlled by the chunk size setting
        // https://googleapis.dev/java/google-cloud-clients/0.90.0-alpha/com/google/cloud/WriteChannel.html#setChunkSize-int-
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        closeWriteChannelIfExists();
        return new GSRecoverableWriterState(state);
    }

    @Override
    public void close() throws IOException {
        closeWriteChannelIfExists();
        state.closed = true;
    }

    @Override
    public Committer closeForCommit() throws IOException {
        close();
        return new GSRecoverableWriterCommitter(storage, options, writer, state);
    }

    private GSChecksumWriteChannel createWriteChannel() {

        // add a new component blob id for the new channel to write to
        BlobId blobId = state.createComponentBlobId(options);

        // create the channel and set the chunk size if specified in options
        BlobStorage.WriteChannel writeChannel = storage.write(blobId, options.writerContentType);
        if (options.writerChunkSize > 0) {
            writeChannel.setChunkSize(options.writerChunkSize);
        }

        return new GSChecksumWriteChannel(storage, writeChannel, blobId);
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

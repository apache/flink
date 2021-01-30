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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implementation of {@link org.apache.flink.core.fs.RecoverableFsDataOutputStream} for Google
 * Storage.
 */
class GSRecoverableFsDataOutputStream extends RecoverableFsDataOutputStream {

    private final GSRecoverableOptions options;

    @VisibleForTesting final GSRecoverablePlan plan;

    private final GSRecoverableWriterHelper.Writer writer;

    private long position;

    GSRecoverableFsDataOutputStream(
            GSRecoverableOptions options,
            GSRecoverablePlan plan,
            GSRecoverableWriterHelper.Writer writer,
            long position) {
        this.options = Preconditions.checkNotNull(options);
        this.plan = Preconditions.checkNotNull(plan);
        this.writer = Preconditions.checkNotNull(writer);
        this.position = position;
        Preconditions.checkArgument(position >= 0, "position must be non-negative: %s", position);
    }

    @Override
    public long getPos() throws IOException {
        return position;
    }

    @Override
    public void write(byte[] bytes, int start, int length) throws IOException {
        Preconditions.checkNotNull(bytes);
        Preconditions.checkArgument(start >= 0, "start must be non-negative: %s", start);
        Preconditions.checkArgument(length >= 0, "length must be non-negative: %s", length);

        ByteBuffer buffer = ByteBuffer.wrap(bytes, start, length);
        int writtenCount = writer.write(buffer);

        // we have no way to inform the caller of (or otherwise handle) a partial write, so if the
        // number of bytes written doesn't match the number of bytes that were requested to
        // be written, throw an exception
        if (length != writtenCount) {
            throw new IOException(
                    String.format(
                            "Only %s of %s bytes were written to the write channel",
                            writtenCount, length));
        }

        // adjust the position to account for the successful write
        position += writtenCount;
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        Preconditions.checkNotNull(bytes);

        write(bytes, 0, bytes.length);
    }

    @Override
    public void write(int byteValue) throws IOException {
        byte[] bytes = new byte[] {(byte) byteValue};
        write(bytes);
    }

    @Override
    public void flush() throws IOException {
        // flush is not supported by WriteChannel
    }

    @Override
    public void sync() throws IOException {
        // sync is not supported by WriteChannel
    }

    @Override
    public RecoverableWriter.ResumeRecoverable persist() throws IOException {
        GSRecoverableWriterHelper.WriterState writerState = writer.capture();
        return new GSRecoverable(plan, writerState, position);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    @Override
    public Committer closeForCommit() throws IOException {
        GSRecoverable recoverable = (GSRecoverable) persist();
        close();
        return new GSRecoverableCommitter(options, recoverable);
    }
}

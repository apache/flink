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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/**
 * An implementation of {@link FSDataOutputStream} which batch flush data to external filesystem.
 */
class FsBatchFlushOutputStream extends FSDataOutputStream {

    private final byte[] writeBuffer;

    private final FSDataOutputStream outputStream;

    private final Path filePath;

    private int pos;

    private volatile boolean closed;

    public FsBatchFlushOutputStream(
            FileSystem fileSystem,
            Path filePath,
            FileSystem.WriteMode overwriteMode,
            int bufferSize)
            throws IOException {
        this.filePath = filePath;
        this.outputStream = fileSystem.create(filePath, overwriteMode);
        this.writeBuffer = new byte[bufferSize];
    }

    @Override
    public long getPos() throws IOException {
        return pos + (outputStream == null ? 0 : outputStream.getPos());
    }

    @Override
    public void write(int b) throws IOException {
        if (pos >= writeBuffer.length) {
            flush();
        }
        writeBuffer[pos++] = (byte) b;
    }

    public void writeInt(int num) throws IOException {
        write((num >>> 24) & 0xFF);
        write((num >>> 16) & 0xFF);
        write((num >>> 8) & 0xFF);
        write((num) & 0xFF);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (len < writeBuffer.length) {
            // copy it into our write buffer first
            final int remaining = writeBuffer.length - pos;
            if (len >= remaining) {
                // copy as much as fits
                System.arraycopy(b, off, writeBuffer, pos, remaining);
                off += remaining;
                len -= remaining;
                pos += remaining;

                // flush the write buffer to make it clear again
                flush();
            }
            if (len > 0) {
                // copy what is in the buffer
                System.arraycopy(b, off, writeBuffer, pos, len);
                pos += len;
            }
        } else {
            // flush the current buffer
            flush();
            // write the bytes directly
            outputStream.write(b, off, len);
        }
    }

    @Override
    public void flush() throws IOException {
        if (!closed) {
            // now flush
            if (pos > 0) {
                outputStream.write(writeBuffer, 0, pos);
                // flush to external system right now.
                outputStream.flush();
                pos = 0;
            }
        }
    }

    @Override
    public void sync() throws IOException {
        outputStream.sync();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {

            try {
                if (outputStream != null) {
                    flush();
                    pos = writeBuffer.length;
                    outputStream.close();
                }
            } finally {
                closed = true;
            }
        }
    }
}

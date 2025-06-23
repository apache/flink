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

package org.apache.flink.state.forst.fs;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.MemoryUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link FSDataOutputStream} delegates requests to other one and supports writing data with
 * {@link ByteBuffer}.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
public class ByteBufferWritableFSDataOutputStream extends FSDataOutputStream {

    /** The unsafe handle for transparent memory copied (heap / off-heap). */
    @SuppressWarnings("restriction")
    private static final sun.misc.Unsafe UNSAFE = MemoryUtils.UNSAFE;

    /** The beginning of the byte array contents, relative to the byte array object. */
    @SuppressWarnings("restriction")
    private static final long BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);

    private static final int SEGMENT_BUFFER_SIZE = 16 * 1024;

    private final FSDataOutputStream originalOutputStream;

    public ByteBufferWritableFSDataOutputStream(FSDataOutputStream originalOutputStream) {
        this.originalOutputStream = originalOutputStream;
    }

    /**
     * Writes <code>ByteBuffer#remaining</code> bytes from the ByteBuffer to this output stream. Not
     * Thread-safe yet since the interface of write of ForSt only be accessed by one thread at a
     * time.
     *
     * <p>If <code>bb</code> is <code>null</code>, a <code>NullPointerException</code> is thrown.
     *
     * @throws IOException if an I/O error occurs. In particular, an <code>IOException</code> is
     *     thrown if the output stream is closed.
     */
    public void write(ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new NullPointerException();
        } else if (bb.remaining() == 0) {
            return;
        }
        if (bb.hasArray()) {
            write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        } else if (bb.isDirect()) {
            int len = bb.remaining();
            int segment = Math.min(len, SEGMENT_BUFFER_SIZE);
            byte[] bytes = new byte[segment];
            for (int i = 0; i < len; ) {
                int copy = Math.min(segment, bb.remaining());
                UNSAFE.copyMemory(
                        null,
                        MemoryUtils.getByteBufferAddress(bb) + bb.position(),
                        bytes,
                        BYTE_ARRAY_BASE_OFFSET,
                        copy);
                originalOutputStream.write(bytes, 0, copy);
                bb.position(bb.position() + copy);
                i += copy;
            }
        } else {
            int len = bb.remaining();
            for (int i = 0; i < len; i++) {
                originalOutputStream.write(bb.get());
            }
        }
    }

    @Override
    public long getPos() throws IOException {
        return originalOutputStream.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        originalOutputStream.write(b);
    }

    public void write(byte[] b) throws IOException {
        originalOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        originalOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        originalOutputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        originalOutputStream.sync();

        // Data only be visible after closing for some object storages e.g. oss, s3
        // TODO: 1. Support to close when sync for some object storages.
        //  (maybe introduce isSupportSync for FileSystem)
        // TODO: 2. Support to handle specific files, e.g. MANIFEST, LOG.
    }

    @Override
    public void close() throws IOException {
        originalOutputStream.close();
    }
}

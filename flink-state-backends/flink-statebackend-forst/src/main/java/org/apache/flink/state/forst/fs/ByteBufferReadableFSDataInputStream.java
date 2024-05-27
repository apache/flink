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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.fs.ByteBufferReadable;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A {@link FSDataInputStream} delegates requests to other one and supports reading data with {@link
 * ByteBuffer}.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
@Experimental
public class ByteBufferReadableFSDataInputStream extends FSDataInputStream {

    private final FSDataInputStream originalInputStream;

    /**
     * InputStream Pool which provides multiple input streams to random read concurrently. An input
     * stream should only be used by a thread at a point in time.
     */
    private final Queue<FSDataInputStream> readInputStreamPool;

    private final Callable<FSDataInputStream> inputStreamBuilder;

    private final long totalFileSize;

    public ByteBufferReadableFSDataInputStream(
            Callable<FSDataInputStream> inputStreamBuilder,
            int inputStreamCapacity,
            long totalFileSize)
            throws IOException {
        try {
            this.originalInputStream = inputStreamBuilder.call();
        } catch (Exception e) {
            throw new IOException("Exception when build original input stream", e);
        }
        this.inputStreamBuilder = inputStreamBuilder;
        this.readInputStreamPool = new LinkedBlockingQueue<>(inputStreamCapacity);
        this.totalFileSize = totalFileSize;
    }

    /**
     * Reads up to <code>ByteBuffer#remaining</code> bytes of data from the input stream into a
     * ByteBuffer. Not Thread-safe yet since the interface of sequential read of ForSt only be
     * accessed by one thread at a time. TODO: Rename all methods about 'readFully' to 'read' when
     * next version of ForSt is ready.
     *
     * @param bb the buffer into which the data is read.
     * @return the total number of bytes read into the buffer.
     * @throws IOException If the first byte cannot be read for any reason other than end of file,
     *     or if the input stream has been closed, or if some other I/O error occurs.
     * @throws NullPointerException If <code>bb</code> is <code>null</code>.
     */
    public int readFully(ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new NullPointerException();
        } else if (bb.remaining() == 0) {
            return 0;
        }
        return originalInputStream instanceof ByteBufferReadable
                ? ((ByteBufferReadable) originalInputStream).read(bb)
                : readFullyFromFSDataInputStream(originalInputStream, bb);
    }

    /**
     * Reads up to <code>ByteBuffer#remaining</code> bytes of data from the specific position of the
     * input stream into a ByteBuffer. Thread-safe since the interface of random read of ForSt may
     * be concurrently accessed by multiple threads. TODO: Support to split this method to other
     * class.
     *
     * @param position the start offset in input stream at which the data is read.
     * @param bb the buffer into which the data is read.
     * @return the total number of bytes read into the buffer.
     * @throws IOException If the first byte cannot be read for any reason other than end of file,
     *     or if the input stream has been closed, or if some other I/O error occurs.
     * @throws NullPointerException If <code>bb</code> is <code>null</code>.
     */
    public int readFully(long position, ByteBuffer bb) throws Exception {
        if (bb == null) {
            throw new NullPointerException();
        } else if (position >= totalFileSize) {
            throw new IllegalArgumentException(
                    String.format(
                            "position [%s] is larger than or equals to totalFileSize [%s]",
                            position, totalFileSize));
        }

        // Avoid bb.remaining() exceeding the file size limit.
        bb.limit(
                Math.min(
                        bb.limit(),
                        (int)
                                Math.min(
                                        totalFileSize - position + bb.position(),
                                        Integer.MAX_VALUE)));

        if (bb.remaining() == 0) {
            return 0;
        }

        FSDataInputStream fsDataInputStream = readInputStreamPool.poll();
        if (fsDataInputStream == null) {
            fsDataInputStream = inputStreamBuilder.call();
        }

        int result;
        if (fsDataInputStream instanceof ByteBufferReadable) {
            result = ((ByteBufferReadable) fsDataInputStream).read(position, bb);
        } else {
            fsDataInputStream.seek(position);
            result = readFullyFromFSDataInputStream(fsDataInputStream, bb);
        }

        boolean offered;
        try {
            offered = readInputStreamPool.offer(fsDataInputStream);
        } catch (Exception ex) {
            // Close input stream and rethrow when any exception
            fsDataInputStream.close();
            throw ex;
        }

        if (!offered) {
            fsDataInputStream.close();
        }

        return result;
    }

    private int readFullyFromFSDataInputStream(FSDataInputStream originalInputStream, ByteBuffer bb)
            throws IOException {
        int c = originalInputStream.read();
        if (c == -1) {
            return -1;
        }
        bb.put((byte) c);

        int n = 1, len = bb.remaining() + 1;
        for (; n < len; n++) {
            c = originalInputStream.read();
            if (c == -1) {
                break;
            }
            bb.put((byte) c);
        }
        return n;
    }

    @Override
    public void seek(long desired) throws IOException {
        originalInputStream.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return originalInputStream.getPos();
    }

    @Override
    public int read() throws IOException {
        return originalInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return originalInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return originalInputStream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        long position = getPos();
        seek(position + n);
        return getPos() - position;
    }

    @Override
    public int available() throws IOException {
        return originalInputStream.available();
    }

    @Override
    public void close() throws IOException {
        originalInputStream.close();
        for (FSDataInputStream fsDataInputStream : readInputStreamPool) {
            fsDataInputStream.close();
        }
    }

    @Override
    public synchronized void mark(int readlimit) {
        originalInputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        originalInputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return originalInputStream.markSupported();
    }
}

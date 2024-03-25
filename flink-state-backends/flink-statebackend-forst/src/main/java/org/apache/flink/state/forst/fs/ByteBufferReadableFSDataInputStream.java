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

import org.apache.flink.core.fs.FSDataInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link FSDataInputStream} delegates requests to other one and supports reading data with {@link
 * ByteBuffer}.
 *
 * <p>All methods in this class maybe used by ForSt, please start a discussion firstly if it has to
 * be modified.
 */
public class ByteBufferReadableFSDataInputStream extends FSDataInputStream {

    private final FSDataInputStream originalInputStream;

    private volatile long toSeek = -1L;

    private final Object lock;

    public ByteBufferReadableFSDataInputStream(FSDataInputStream originalInputStream) {
        this.originalInputStream = originalInputStream;
        this.lock = new Object();
    }

    /**
     * Reads up to <code>ByteBuffer#remaining</code> bytes of data from the input stream into a
     * ByteBuffer. Not Tread-safe yet since the interface of sequential read of ForSt only be
     * accessed by one thread at a time.
     *
     * @param bb the buffer into which the data is read.
     * @return the total number of bytes read into the buffer.
     * @exception IOException If the first byte cannot be read for any reason other than end of
     *     file, or if the input stream has been closed, or if some other I/O error occurs.
     */
    public int readFully(ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new NullPointerException();
        } else if (bb.remaining() == 0) {
            return 0;
        }
        seekIfNeeded();
        return readFullyFromFSDataInputStream(originalInputStream, bb);
    }

    /**
     * Reads up to <code>ByteBuffer#remaining</code> bytes of data from the specific position of the
     * input stream into a ByteBuffer. Tread-safe since the interface of random read of ForSt may be
     * concurrently accessed by multiple threads.
     *
     * @param position the start offset in input stream at which the data is read.
     * @param bb the buffer into which the data is read.
     * @return the total number of bytes read into the buffer.
     * @exception IOException If the first byte cannot be read for any reason other than end of
     *     file, or if the input stream has been closed, or if some other I/O error occurs.
     */
    public int readFully(long position, ByteBuffer bb) throws IOException {
        // TODO: Improve the performance
        synchronized (lock) {
            originalInputStream.seek(position);
            return readFullyFromFSDataInputStream(originalInputStream, bb);
        }
    }

    private int readFullyFromFSDataInputStream(FSDataInputStream originalInputStream, ByteBuffer bb)
            throws IOException {
        byte[] tmp = new byte[bb.remaining()];
        int n = 0;
        while (n < tmp.length) {
            int read = originalInputStream.read(tmp, n, tmp.length - n);
            if (read == -1) {
                break;
            }
            n += read;
        }
        if (n > 0) {
            bb.put(tmp, 0, n);
        }
        return n;
    }

    private void seekIfNeeded() throws IOException {
        if (toSeek >= 0) {
            originalInputStream.seek(toSeek);
            toSeek = -1L;
        }
    }

    @Override
    public void seek(long desired) {
        toSeek = desired;
    }

    @Override
    public long getPos() throws IOException {
        if (toSeek >= 0) {
            return toSeek;
        }
        return originalInputStream.getPos();
    }

    @Override
    public int read() throws IOException {
        seekIfNeeded();
        return originalInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        seekIfNeeded();
        return originalInputStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        seekIfNeeded();
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
        seekIfNeeded();
        return originalInputStream.available();
    }

    @Override
    public void close() throws IOException {
        originalInputStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        originalInputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        toSeek = -1L;
        originalInputStream.reset();
    }

    @Override
    public boolean markSupported() {
        return originalInputStream.markSupported();
    }
}

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

package org.apache.flink.state.forst.fs.cache;

import org.apache.flink.core.fs.ByteBufferReadable;
import org.apache.flink.core.fs.FSDataInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

/**
 * A {@link FSDataInputStream} delegates requests to other one and supports reading data with {@link
 * ByteBuffer}. One CachedDataInputStream only supports one thread reading which is guaranteed by
 * ByteBufferReadableFSDataInputStream. The cached input stream might be closed by eviction by other
 * thread, and the concurrency between reading and evicting is controlled by the reference count of
 * the cache entry.
 */
public class CachedDataInputStream extends FSDataInputStream implements ByteBufferReadable {

    private static final Logger LOG = LoggerFactory.getLogger(CachedDataInputStream.class);

    /** The reference to the cache entry. */
    private final FileCacheEntry cacheEntry;

    private volatile FSDataInputStream fsdis;

    private volatile StreamStatus streamStatus;

    /**
     * The position of the cached stream, when cached stream is closed, the position is stored. When
     * switch to original stream, the position is restored.
     */
    private volatile long position;

    private final FSDataInputStream originalStream;

    private Semaphore semaphore;

    public CachedDataInputStream(
            FileCacheEntry cacheEntry,
            FSDataInputStream cacheStream,
            FSDataInputStream originalStream) {
        this.cacheEntry = cacheEntry;
        this.fsdis = cacheStream;
        this.originalStream = originalStream;
        this.streamStatus = StreamStatus.CACHED_OPEN;
        this.semaphore = new Semaphore(0);
    }

    private FSDataInputStream getStream() throws IOException {
        if (streamStatus == StreamStatus.CACHED_OPEN && cacheEntry.tryRetain() > 0) {
            return fsdis;
        } else if (streamStatus != StreamStatus.ORIGINAL) {
            try {
                semaphore.acquire(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            originalStream.seek(position);
            position = -1;
            streamStatus = StreamStatus.ORIGINAL;
            return originalStream;
        } else {
            return originalStream;
        }
    }

    private void closeStream() throws IOException {
        if (streamStatus == StreamStatus.CACHED_OPEN) {
            streamStatus = StreamStatus.CACHED_CLOSED;
            position = fsdis.getPos();
            fsdis.close();
            fsdis = null;
            semaphore.release(1);
        }
    }

    private void finish() {
        if (streamStatus == StreamStatus.CACHED_OPEN) {
            cacheEntry.release();
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        try {
            getStream().seek(desired);
        } finally {
            finish();
        }
    }

    @Override
    public long getPos() throws IOException {
        try {
            return getStream().getPos();
        } finally {
            finish();
        }
    }

    @Override
    public int read() throws IOException {
        try {
            return getStream().read();
        } finally {
            finish();
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        try {
            return getStream().read(b);
        } finally {
            finish();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return getStream().read(b, off, len);
        } finally {
            finish();
        }
    }

    @Override
    public long skip(long n) throws IOException {
        try {
            return getStream().skip(n);
        } finally {
            finish();
        }
    }

    @Override
    public int available() throws IOException {
        try {
            return getStream().available();
        } finally {
            finish();
        }
    }

    @Override
    public void close() throws IOException {
        closeStream();
    }

    @Override
    public void mark(int readlimit) {
        try {
            getStream().mark(readlimit);
        } catch (Exception e) {
            LOG.warn("Mark error.", e);
        } finally {
            finish();
        }
    }

    @Override
    public void reset() throws IOException {
        try {
            getStream().reset();
        } finally {
            finish();
        }
    }

    @Override
    public boolean markSupported() {
        try {
            return getStream().markSupported();
        } catch (IOException e) {
            LOG.warn("MarkSupported error.", e);
            return false;
        } finally {
            finish();
        }
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new NullPointerException();
        } else if (bb.remaining() == 0) {
            return 0;
        }
        try {
            FSDataInputStream stream = getStream();
            return stream instanceof ByteBufferReadable
                    ? ((ByteBufferReadable) stream).read(bb)
                    : readFullyFromFSDataInputStream(stream, bb);
        } finally {
            finish();
        }
    }

    @Override
    public int read(long position, ByteBuffer bb) throws IOException {
        try {
            FSDataInputStream stream = getStream();
            if (stream instanceof ByteBufferReadable) {
                return ((ByteBufferReadable) stream).read(position, bb);
            } else {
                stream.seek(position);
                return readFullyFromFSDataInputStream(stream, bb);
            }
        } finally {
            finish();
        }
    }

    private static int readFullyFromFSDataInputStream(
            FSDataInputStream originalInputStream, ByteBuffer bb) throws IOException {
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

    /** The status of the underlying stream. */
    enum StreamStatus {
        CACHED_OPEN,
        CACHED_CLOSED,
        ORIGINAL
    }
}

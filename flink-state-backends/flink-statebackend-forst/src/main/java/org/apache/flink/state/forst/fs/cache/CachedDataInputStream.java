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

import static org.apache.flink.state.forst.fs.cache.FileBasedCache.isFlinkThread;

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

    private final FileBasedCache fileBasedCache;

    private boolean closed = false;

    public CachedDataInputStream(
            FileBasedCache fileBasedCache,
            FileCacheEntry cacheEntry,
            FSDataInputStream cacheStream,
            FSDataInputStream originalStream) {
        this.fileBasedCache = fileBasedCache;
        this.cacheEntry = cacheEntry;
        this.fsdis = cacheStream;
        this.originalStream = originalStream;
        this.streamStatus = StreamStatus.CACHED_OPEN;
        this.semaphore = new Semaphore(0);
        LOG.trace("Create CachedDataInputStream for {} with CACHED_OPEN", cacheEntry.cachePath);
    }

    public CachedDataInputStream(
            FileBasedCache fileBasedCache,
            FileCacheEntry cacheEntry,
            FSDataInputStream originalStream) {
        this.fileBasedCache = fileBasedCache;
        this.cacheEntry = cacheEntry;
        this.fsdis = null;
        this.originalStream = originalStream;
        this.streamStatus = StreamStatus.CACHED_CLOSED;
        this.semaphore = new Semaphore(0);
        LOG.trace("Create CachedDataInputStream for {} with CACHED_CLOSED", cacheEntry.cachePath);
    }

    /**
     * Retrieves the appropriate input stream for reading data. This method attempts to use the
     * cached stream if it is available and valid. If the cached stream is not available, it falls
     * back to the original stream. The method also handles the transition between cached and
     * original streams based on the current status of the stream. The invoker must ensure to
     * release the cache stream after use.
     *
     * @return the input stream to be used for reading data
     * @throws IOException if an I/O error occurs while accessing the stream
     */
    private FSDataInputStream getStream() throws IOException {
        if (isFlinkThread()) {
            cacheEntry.touch();
        }
        int round = 0;
        // Repeat at most 3 times. If fails, we will get the original stream for read.
        while (round++ < 3) {
            // Firstly, we try to get cache stream
            FSDataInputStream stream = tryGetCacheStream();
            if (stream != null) {
                fileBasedCache.incHitCounter();
                return stream;
            }
            // No cache stream
            if (streamStatus == StreamStatus.CACHED_CLOSING) {
                // if closing, update the position
                try {
                    semaphore.acquire(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                originalStream.seek(position);
                position = -1;
                LOG.trace(
                        "Cached Stream {} status from {} to {}",
                        cacheEntry.cachePath,
                        streamStatus,
                        StreamStatus.CACHED_CLOSED);
                streamStatus = StreamStatus.CACHED_CLOSED;
            }
            // if it is CACHED_CLOSED, we try to reopen it
            if (streamStatus == StreamStatus.CACHED_CLOSED) {
                stream = tryReopenCachedStream();
                if (stream != null) {
                    fileBasedCache.incHitCounter();
                    return stream;
                }
                fileBasedCache.incMissCounter();
                return originalStream;
            } else if (streamStatus == StreamStatus.ORIGINAL) {
                fileBasedCache.incMissCounter();
                return originalStream;
            } else {
                // The stream is not closed, but we cannot get the cache stream.
                // Meaning that it is in the process of closing, but the status has not been
                // updated. Thus, we'd better retry here until it reach a stable state (CLOSING).
                Thread.yield();
            }
        }
        return originalStream;
    }

    /**
     * Attempts to retrieve the cached stream if it is open and the reference count is greater than
     * zero. If successful, it retains the reference count and returns the cached stream. The
     * invoker must ensure to release the stream after use.
     *
     * @return the cached stream if available, or null if not
     */
    private FSDataInputStream tryGetCacheStream() {
        if (streamStatus == StreamStatus.CACHED_OPEN && cacheEntry.tryRetain() > 0) {
            // Double-check the status as it may change after retain.
            if (streamStatus == StreamStatus.CACHED_OPEN) {
                return fsdis;
            }
        }
        return null;
    }

    /**
     * Attempts to reopen the cached stream if it is closed and the current thread is a Flink
     * thread. If successful, it updates the stream status and seeks to the original stream's
     * position. Reference counting is retained, the invoked thread must dereference the stream
     * after use.
     *
     * @return the reopened cached stream, or null if reopening fails
     */
    private FSDataInputStream tryReopenCachedStream() {
        if (streamStatus == StreamStatus.CACHED_CLOSED && isFlinkThread()) {
            try {
                fsdis = cacheEntry.getCacheStream();
                if (fsdis != null) {
                    LOG.trace(
                            "Cached Stream {} status from {} to {}",
                            cacheEntry.cachePath,
                            streamStatus,
                            StreamStatus.CACHED_OPEN);
                    fsdis.seek(originalStream.getPos());
                    streamStatus = StreamStatus.CACHED_OPEN;
                    return fsdis;
                }
            } catch (IOException e) {
                LOG.warn("Reopen stream error.", e);
            }
        }
        return null;
    }

    /**
     * Closes the cached stream if it is open. Note that this might be invoked by different threads,
     * the user thread or the cache eviction (async) thread.
     */
    synchronized void closeCachedStream() throws IOException {
        if (streamStatus == StreamStatus.CACHED_OPEN) {
            LOG.trace(
                    "Stream {} status from {} to {}",
                    cacheEntry.cachePath,
                    streamStatus,
                    StreamStatus.CACHED_CLOSING);
            streamStatus = StreamStatus.CACHED_CLOSING;
            position = fsdis.getPos();
            fsdis.close();
            fsdis = null;
            semaphore.release(1);
        }
    }

    @Override
    public void seek(long desired) throws IOException {
        FSDataInputStream stream = getStream();
        try {
            stream.seek(desired);
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public long getPos() throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.getPos();
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public int read() throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.read();
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.read(b);
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.read(b, off, len);
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public long skip(long n) throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.skip(n);
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public int available() throws IOException {
        FSDataInputStream stream = getStream();
        try {
            return stream.available();
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        closeCachedStream();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void mark(int readlimit) {
        try {
            FSDataInputStream stream = getStream();
            try {
                stream.mark(readlimit);
            } finally {
                if (stream != originalStream) {
                    cacheEntry.release();
                }
            }
        } catch (Exception e) {
            LOG.warn("Mark error.", e);
        }
    }

    @Override
    public void reset() throws IOException {
        FSDataInputStream stream = getStream();
        try {
            stream.reset();
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public boolean markSupported() {
        try {
            FSDataInputStream stream = getStream();
            try {
                return stream.markSupported();
            } finally {
                if (stream != originalStream) {
                    cacheEntry.release();
                }
            }
        } catch (IOException e) {
            LOG.warn("MarkSupported error.", e);
            return false;
        }
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new NullPointerException();
        } else if (bb.remaining() == 0) {
            return 0;
        }
        FSDataInputStream stream = getStream();
        try {
            return stream instanceof ByteBufferReadable
                    ? ((ByteBufferReadable) stream).read(bb)
                    : readFullyFromFSDataInputStream(stream, bb);
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
        }
    }

    @Override
    public int read(long position, ByteBuffer bb) throws IOException {
        FSDataInputStream stream = getStream();
        try {
            if (stream instanceof ByteBufferReadable) {
                return ((ByteBufferReadable) stream).read(position, bb);
            } else {
                stream.seek(position);
                return readFullyFromFSDataInputStream(stream, bb);
            }
        } finally {
            if (stream != originalStream) {
                cacheEntry.release();
            }
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

    /** The status of the underlying cache stream. */
    enum StreamStatus {
        /** The cached stream is open and available for reading. */
        CACHED_OPEN,

        /** The cached stream is in the process of closing. */
        CACHED_CLOSING,

        /** The cached stream is closed and not available for reading. */
        CACHED_CLOSED,

        /** The original stream is being used instead of the cached stream. */
        ORIGINAL
    }
}

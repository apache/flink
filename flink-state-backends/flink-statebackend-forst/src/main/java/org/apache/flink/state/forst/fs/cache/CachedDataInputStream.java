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

/**
 * A {@link FSDataInputStream} delegates requests to other one and supports reading data with {@link
 * ByteBuffer}.
 */
public class CachedDataInputStream extends FSDataInputStream implements ByteBufferReadable {

    private static final Logger LOG = LoggerFactory.getLogger(CachedDataInputStream.class);

    private final FileCacheEntry cacheEntry;

    private final Object lock;

    private volatile FSDataInputStream fsdis;

    public CachedDataInputStream(FileCacheEntry cacheEntry) {
        this.cacheEntry = cacheEntry;
        this.lock = new Object();
    }

    private FSDataInputStream getStream() throws IOException {
        if (fsdis == null) {
            synchronized (lock) {
                if (fsdis == null) {
                    fsdis = cacheEntry.openForRead();
                }
            }
        }
        return fsdis;
    }

    private void closeStream() {
        synchronized (lock) {
            if (fsdis != null) {
                fsdis = null;
            }
        }
    }

    public boolean isAvailable() throws IOException {
        return getStream() != null;
    }

    @Override
    public void seek(long desired) throws IOException {
        cacheEntry.retain();
        getStream().seek(desired);
        cacheEntry.release();
    }

    @Override
    public long getPos() throws IOException {
        cacheEntry.retain();
        long pos = getStream().getPos();
        cacheEntry.release();
        return pos;
    }

    @Override
    public int read() throws IOException {
        cacheEntry.retain();
        int read = getStream().read();
        cacheEntry.release();
        return read;
    }

    @Override
    public int read(byte[] b) throws IOException {
        cacheEntry.retain();
        int read = getStream().read(b);
        cacheEntry.release();
        return read;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        cacheEntry.retain();
        int read = getStream().read(b, off, len);
        cacheEntry.release();
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        cacheEntry.retain();
        long skipped = getStream().skip(n);
        cacheEntry.release();
        return skipped;
    }

    @Override
    public int available() throws IOException {
        cacheEntry.retain();
        int available = getStream().available();
        cacheEntry.release();
        return available;
    }

    @Override
    public void close() throws IOException {
        closeStream();
    }

    @Override
    public void mark(int readlimit) {
        try {
            cacheEntry.retain();
            getStream().mark(readlimit);
            cacheEntry.release();
        } catch (Exception e) {
            LOG.warn("Mark error.", e);
        }
    }

    @Override
    public void reset() throws IOException {
        cacheEntry.retain();
        getStream().reset();
        cacheEntry.release();
    }

    @Override
    public boolean markSupported() {
        try {
            cacheEntry.retain();
            boolean support = getStream().markSupported();
            cacheEntry.release();
            return support;
        } catch (Exception e) {
            LOG.warn("MarkSupported error.", e);
        }
        return false;
    }

    @Override
    public int read(ByteBuffer bb) throws IOException {
        cacheEntry.retain();
        byte[] tmp = new byte[bb.remaining()];
        int n = 0;
        while (n < tmp.length) {
            int read = getStream().read(tmp, n, tmp.length - n);
            if (read == -1) {
                break;
            }
            n += read;
        }
        if (n > 0) {
            bb.put(tmp, 0, n);
        }
        cacheEntry.release();
        return n;
    }

    @Override
    public int read(long position, ByteBuffer bb) throws IOException {
        cacheEntry.retain();
        getStream().seek(position);
        cacheEntry.release();
        return read(bb);
    }
}

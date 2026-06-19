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

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link FSDataOutputStream} delegates requests to other one and supports writing data with
 * {@link ByteBuffer}. The data will be written to the original output stream and the cache output
 * stream. When the stream is closed, the data will be put into the cache and ready to be read.
 */
public class CachedDataOutputStream extends FSDataOutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(CachedDataOutputStream.class);

    /** The original path of file. */
    private final Path originalPath;

    /** The path in cache. */
    private final Path cachePath;

    @Nullable private FSDataOutputStream cacheOutputStream;
    private FSDataOutputStream originOutputStream;

    /** The reference of file cache. */
    private final FileBasedCache fileBasedCache;

    public CachedDataOutputStream(
            Path originalPath,
            Path cachePath,
            FSDataOutputStream originalOutputStream,
            @Nullable FSDataOutputStream cacheOutputStream,
            FileBasedCache cache) {
        this.originOutputStream = originalOutputStream;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.cacheOutputStream = cacheOutputStream;
        this.fileBasedCache = cache;
        LOG.trace("Create CachedDataOutputStream for {} and {}", originalPath, cachePath);
    }

    @Override
    public long getPos() throws IOException {
        if (cacheOutputStream == null) {
            return originOutputStream.getPos();
        } else {
            return cacheOutputStream.getPos();
        }
    }

    @Override
    public void write(int b) throws IOException {
        if (cacheOutputStream != null) {
            cacheOutputStream.write(b);
        }
        originOutputStream.write(b);
    }

    public void write(byte[] b) throws IOException {
        if (cacheOutputStream != null) {
            cacheOutputStream.write(b);
        }
        originOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (cacheOutputStream != null) {
            cacheOutputStream.write(b, off, len);
        }
        originOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        if (cacheOutputStream != null) {
            cacheOutputStream.flush();
        }
        originOutputStream.flush();
    }

    @Override
    public void sync() throws IOException {
        if (cacheOutputStream != null) {
            cacheOutputStream.sync();
        }
        originOutputStream.sync();
    }

    @Override
    public void close() throws IOException {
        long size = getPos();
        if (originOutputStream != null) {
            originOutputStream.close();
            originOutputStream = null;
        }
        if (cacheOutputStream != null) {
            putIntoCache(size);
            cacheOutputStream.close();
            cacheOutputStream = null;
        } else {
            registerIntoCache(size);
        }
    }

    private void putIntoCache(long size) {
        FileCacheEntry fileCacheEntry =
                new FileCacheEntry(fileBasedCache, originalPath, cachePath, size);
        fileCacheEntry.switchStatus(
                FileCacheEntry.EntryStatus.REMOVED, FileCacheEntry.EntryStatus.LOADED);
        fileCacheEntry.loaded();
        fileBasedCache.addFirst(cachePath.toString(), fileCacheEntry);
    }

    private void registerIntoCache(long size) {
        FileCacheEntry fileCacheEntry =
                new FileCacheEntry(fileBasedCache, originalPath, cachePath, size);
        fileBasedCache.addSecond(cachePath.toString(), fileCacheEntry);
    }
}

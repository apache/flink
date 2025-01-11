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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A file cache entry that encapsulates file and the size of the file, and provides methods to read
 * or write file. Not thread safe.
 */
public class FileCacheEntry extends ReferenceCounted {
    private static final Logger LOG = LoggerFactory.getLogger(FileCacheEntry.class);

    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The original path of file. */
    final Path originalPath;

    /** The path in cache. */
    final Path cachePath;

    /** The size of file. */
    long entrySize;

    volatile boolean closed;

    final Queue<CachedDataInputStream> openedStreams;

    FileCacheEntry(
            FileBasedCache fileBasedCache, Path originalPath, Path cachePath, long entrySize) {
        super(1);
        this.cacheFs = fileBasedCache.cacheFs;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.entrySize = entrySize;
        this.closed = false;
        this.openedStreams = new LinkedBlockingQueue<>();
    }

    public CachedDataInputStream open(FSDataInputStream originalStream) throws IOException {
        if (!closed && tryRetain() > 0) {
            CachedDataInputStream inputStream =
                    new CachedDataInputStream(this, cacheFs.open(cachePath), originalStream);
            openedStreams.add(inputStream);
            release();
            return inputStream;
        } else {
            return null;
        }
    }

    public void invalidate() {
        if (!closed) {
            closed = true;
            release();
        }
    }

    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        try {
            for (CachedDataInputStream stream : openedStreams) {
                stream.close();
            }
            cacheFs.delete(cachePath, false);
        } catch (Exception e) {
            LOG.warn("Failed to delete cache entry {}.", cachePath, e);
        }
    }
}

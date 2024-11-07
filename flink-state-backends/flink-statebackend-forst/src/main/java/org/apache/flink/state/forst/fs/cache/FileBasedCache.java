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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A file-granularity LRU cache. Only newly generated SSTs are written to the cache, the file
 * reading from the remote will not. Newly generated SSTs are written to the original file system
 * and cache simultaneously, so, the cached file can be directly deleted with persisting when
 * evicting.
 */
public class FileBasedCache extends LruCache<String, FileCacheEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedCache.class);
    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The base path of cache. */
    private final Path basePath;

    /** Whether the cache is closed. */
    private volatile boolean closed;

    public FileBasedCache(
            int capacity, CacheLimitPolicy cacheLimitPolicy, FileSystem cacheFs, Path basePath) {
        super(capacity, cacheLimitPolicy);
        this.closed = false;
        this.cacheFs = cacheFs;
        this.basePath = basePath;
        LOG.info(
                "FileBasedCache initialized, basePath: {}, cache limit policy: {}",
                basePath,
                cacheLimitPolicy);
    }

    Path getCachePath(Path fromOriginal) {
        return new Path(basePath, fromOriginal.getName());
    }

    public CachedDataInputStream open(Path path, FSDataInputStream originalStream)
            throws IOException {
        if (closed) {
            return null;
        }
        FileCacheEntry entry = get(getCachePath(path).toString());
        if (entry != null) {
            return entry.open(originalStream);
        } else {
            return null;
        }
    }

    public CachedDataOutputStream create(FSDataOutputStream originalOutputStream, Path path)
            throws IOException {
        if (closed) {
            return null;
        }
        Path cachePath = getCachePath(path);
        return new CachedDataOutputStream(
                path,
                cachePath,
                originalOutputStream,
                cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE),
                this);
    }

    public void delete(Path path) {
        if (!closed) {
            remove(getCachePath(path).toString());
        }
    }

    @Override
    FileCacheEntry internalGet(String key, FileCacheEntry value) {
        return value;
    }

    @Override
    void internalInsert(String key, FileCacheEntry value) {}

    @Override
    void internalRemove(FileCacheEntry value) {
        value.invalidate();
    }

    @Override
    long getValueResource(FileCacheEntry value) {
        return value.entrySize;
    }
}

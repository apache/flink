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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.state.forst.ForStOptions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A file-granularity LRU cache. Only newly generated SSTs are written to the cache, the file
 * reading from the remote will not. Newly generated SSTs are written to the original file system
 * and cache simultaneously, so, the cached file can be directly deleted with persisting when
 * evicting. The {@link FileBasedCache}, {@link FileCacheEntry}, {@link CachedDataInputStream}, and
 * {@link CachedDataOutputStream} classes work together to implement a file-based caching mechanism
 * in ForSt State Backend.
 * <li>FileBasedCache manages multiple FileCacheEntry instances.
 * <li>Each FileCacheEntry represents a cached file and can open CachedDataInputStream for reading
 *     the file.
 * <li>CachedDataInputStream instances are created by FileCacheEntry and can read data from either
 *     the cached file or the original file, depending on the cache entry's state. It has internal
 *     stream status to indicate the current reading source.
 * <li>CachedDataOutputStream instances are created by FileBasedCache and write data to both the
 *     original and cached files, creating a new cache entry in the cache when the writing is
 *     finished.
 */
public final class FileBasedCache extends DoubleListLru<String, FileCacheEntry>
        implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FileBasedCache.class);

    private static final String FORST_CACHE_PREFIX = "forst.fileCache";

    private static final ThreadLocal<Boolean> isFlinkThread = ThreadLocal.withInitial(() -> false);

    private final CacheLimitPolicy cacheLimitPolicy;

    /** The file system of cache. */
    final FileSystem cacheFs;

    /** The base path of cache. */
    private final Path basePath;

    /** The number of times a file is accessed before it is promoted to the first link . */
    private final int accessBeforePromote;

    /**
     * The threshold count of promotion times, beyond which the file will be blocked out of first
     * link.
     */
    private final int promoteLimit;

    /** Whether the cache is closed. */
    private volatile boolean closed;

    /** Executor service for handling cache operations. */
    private final ExecutorService executorService;

    /** Hit metric. */
    private Counter hitCounter;

    /** Miss metric. */
    private Counter missCounter;

    /** Metric for load back. */
    private Counter loadBackCounter;

    /** Metric for eviction. */
    private Counter evictCounter;

    /** Epoch for second link access. */
    private long secondAccessEpoch = 0L;

    public FileBasedCache(
            ReadableConfig configuration,
            CacheLimitPolicy cacheLimitPolicy,
            FileSystem cacheFs,
            Path basePath,
            MetricGroup metricGroup) {
        this.closed = false;
        this.cacheLimitPolicy = cacheLimitPolicy;
        this.cacheFs = cacheFs;
        this.basePath = basePath;
        this.accessBeforePromote =
                Math.max(1, configuration.get(ForStOptions.CACHE_LRU_ACCESS_BEFORE_PROMOTION));
        this.promoteLimit = configuration.get(ForStOptions.CACHE_LRU_PROMOTION_LIMIT);
        this.executorService =
                Executors.newFixedThreadPool(4, new ExecutorThreadFactory("ForSt-LruLoader"));
        if (metricGroup != null) {
            this.hitCounter =
                    metricGroup.counter(FORST_CACHE_PREFIX + ".hit", new ThreadSafeSimpleCounter());
            this.missCounter =
                    metricGroup.counter(
                            FORST_CACHE_PREFIX + ".miss", new ThreadSafeSimpleCounter());
            this.loadBackCounter =
                    metricGroup.counter(
                            FORST_CACHE_PREFIX + ".lru.loadback", new ThreadSafeSimpleCounter());
            this.evictCounter =
                    metricGroup.counter(
                            FORST_CACHE_PREFIX + ".lru.evict", new ThreadSafeSimpleCounter());
            metricGroup.gauge(FORST_CACHE_PREFIX + ".usedBytes", cacheLimitPolicy::usedBytes);
            cacheLimitPolicy.registerCustomizedMetrics(FORST_CACHE_PREFIX, metricGroup);
        }
        LOG.info(
                "FileBasedCache initialized, basePath: {}, cache limit policy: {}",
                basePath,
                cacheLimitPolicy);
    }

    /**
     * Sets the current thread as a Flink thread. This method is used to mark the thread as a Flink
     * thread, which can be used to determine whether the file access would affect the LRU cache
     * order, or metrics updates.
     */
    public static void setFlinkThread() {
        isFlinkThread.set(true);
    }

    @VisibleForTesting
    public static void unsetFlinkThread() {
        isFlinkThread.set(false);
    }

    /**
     * Checks if the current thread is a Flink thread. This method returns a boolean indicating
     * whether the current thread has been marked as a Flink thread using the {@link
     * #setFlinkThread()} method.
     *
     * @return true if the current thread is a Flink thread, false otherwise.
     */
    public static boolean isFlinkThread() {
        return isFlinkThread.get();
    }

    public void incHitCounter() {
        if (hitCounter != null && isFlinkThread.get()) {
            hitCounter.inc();
        }
    }

    public void incMissCounter() {
        if (missCounter != null && isFlinkThread.get()) {
            missCounter.inc();
        }
    }

    private Path getCachePath(Path fromOriginal) {
        return new Path(basePath, fromOriginal.getName());
    }

    public CachedDataInputStream open(Path path, FSDataInputStream originalStream)
            throws IOException {
        if (closed) {
            return null;
        }
        FileCacheEntry entry = get(getCachePath(path).toString(), isFlinkThread());
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
                cacheLimitPolicy.directWriteInCache()
                        ? cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE)
                        : null,
                this);
    }

    public void delete(Path path) {
        if (!closed) {
            remove(getCachePath(path).toString());
        }
    }

    // -----------------------------------------------------------------------
    // Overriding methods of {@link DoubleListLru} to provide thread-safe.
    // -----------------------------------------------------------------------

    @Override
    public FileCacheEntry get(String key, boolean affectOrder) {
        synchronized (this) {
            return super.get(key, affectOrder);
        }
    }

    @Override
    public void addFirst(String key, FileCacheEntry value) {
        synchronized (this) {
            super.addFirst(key, value);
        }
    }

    @Override
    public void addSecond(String key, FileCacheEntry value) {
        synchronized (this) {
            super.addSecond(key, value);
        }
    }

    @Override
    public FileCacheEntry remove(String key) {
        synchronized (this) {
            return super.remove(key);
        }
    }

    /** Directly insert in cache when restoring. */
    public void registerInCache(Path originalPath, long size) {
        Path cachePath = getCachePath(originalPath);
        FileCacheEntry fileCacheEntry = new FileCacheEntry(this, originalPath, cachePath, size);
        // We want the new registered cache to load ASAP, so assign a initial access count.
        fileCacheEntry.accessCountInColdLink = Math.max(0, accessBeforePromote - 2);
        addSecond(cachePath.toString(), fileCacheEntry);
    }

    void removeFile(FileCacheEntry entry) {
        if (closed) {
            entry.doRemoveFile();
        } else {
            executorService.submit(entry::doRemoveFile);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        executorService.shutdown();
        for (Tuple2<String, FileCacheEntry> entry : this) {
            entry.f1.close();
        }
    }

    // -----------------------------
    // Hook methods implementation
    // -----------------------------

    @Override
    boolean isSafeToAddFirst(FileCacheEntry value) {
        return cacheLimitPolicy.isSafeToAdd(value.entrySize);
    }

    @Override
    void newNodeCreated(FileCacheEntry value, DoubleListLru<String, FileCacheEntry>.Node n) {
        value.setTouchFunction(
                () -> {
                    // provide synchronized access to the LRU cache.
                    synchronized (FileBasedCache.this) {
                        accessNode(n);
                    }
                });
    }

    @Override
    void addedToFirst(FileCacheEntry value) {
        LOG.trace("Cache entry {} added to first link.", value.cachePath);
        while (cacheLimitPolicy.isOverflow(
                value.entrySize, value.checkStatus(FileCacheEntry.EntryStatus.LOADED))) {
            moveMiddleFront();
        }
        cacheLimitPolicy.acquire(value.entrySize);
    }

    @Override
    void addedToSecond(FileCacheEntry value) {
        LOG.trace("Cache entry {} added to second link.", value.cachePath);
        value.secondAccessEpoch = (++secondAccessEpoch);
        tryEvict(value);
    }

    @Override
    void removedFromFirst(FileCacheEntry value) {
        cacheLimitPolicy.release(value.entrySize);
        value.close();
    }

    @Override
    void removedFromSecond(FileCacheEntry value) {
        value.close();
    }

    @Override
    void movedToFirst(FileCacheEntry entry) {
        // here we won't consider the cache limit policy.
        // since there will be promotedToFirst called after this.
        LOG.trace("Cache entry {} moved to first link.", entry.cachePath);
        // trigger the loading
        if (entry.switchStatus(
                FileCacheEntry.EntryStatus.INVALID, FileCacheEntry.EntryStatus.LOADED)) {
            // just a try
            entry.loaded();
            if (loadBackCounter != null) {
                loadBackCounter.inc();
            }
        }
        if (entry.switchStatus(
                FileCacheEntry.EntryStatus.REMOVED, FileCacheEntry.EntryStatus.LOADING)) {
            executorService.submit(
                    () -> {
                        if (entry.checkStatus(FileCacheEntry.EntryStatus.LOADING)) {
                            Path path = entry.load();
                            if (path == null) {
                                entry.switchStatus(
                                        FileCacheEntry.EntryStatus.LOADING,
                                        FileCacheEntry.EntryStatus.REMOVED);
                            } else if (entry.switchStatus(
                                    FileCacheEntry.EntryStatus.LOADING,
                                    FileCacheEntry.EntryStatus.LOADED)) {
                                entry.loaded();
                                if (loadBackCounter != null) {
                                    loadBackCounter.inc();
                                }
                            } else {
                                try {
                                    path.getFileSystem().delete(path, false);
                                    // delete the file
                                } catch (IOException e) {
                                }
                            }
                        }
                    });
        }
    }

    @Override
    void movedToSecond(FileCacheEntry value) {
        // trigger the evicting
        LOG.trace("Cache entry {} moved to second link.", value.cachePath);
        cacheLimitPolicy.release(value.entrySize);
        tryEvict(value);
    }

    @Override
    boolean nodeAccessedAtSecond(FileCacheEntry value) {
        if (secondAccessEpoch - value.secondAccessEpoch >= getSecondSize() / 2) {
            // current entry is at last half of the second link
            value.accessCountInColdLink = 0;
            secondAccessEpoch++;
        }
        value.secondAccessEpoch = secondAccessEpoch;
        return value.evictCount < promoteLimit
                && ++value.accessCountInColdLink >= accessBeforePromote;
    }

    @Override
    void promotedToFirst(FileCacheEntry value) {
        value.accessCountInColdLink = 0;
        // the loading has started, so we believe the entry has file. Even if it's not, won't cause
        // anything here.
        while (cacheLimitPolicy.isOverflow(value.entrySize, true)) {
            moveMiddleFront();
        }
        cacheLimitPolicy.acquire(value.entrySize);
    }

    /** Tool method that evict a file cache, releasing the owned reference if needed. */
    private void tryEvict(FileCacheEntry value) {
        if (value.invalidate() && evictCounter != null) {
            evictCounter.inc();
            value.evictCount++;
        }
    }
}

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
import org.apache.flink.runtime.asyncprocessing.ReferenceCounted;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A file cache entry that encapsulates file and the size of the file, and provides methods to read
 * or write file. Not thread safe.
 */
public final class FileCacheEntry extends ReferenceCounted<Object> {
    private static final Logger LOG = LoggerFactory.getLogger(FileCacheEntry.class);
    private static final int READ_BUFFER_SIZE = 64 * 1024;

    /** The file-based cache that this entry belongs to. */
    final FileBasedCache fileBasedCache;

    /** The file system of the cache. */
    final FileSystem cacheFs;

    /** The original path of the file. */
    final Path originalPath;

    /** The path in the cache. */
    final Path cachePath;

    /** The size of the file. */
    final long entrySize;

    /** A queue of opened streams associated with this cache entry. */
    final Queue<CachedDataInputStream> openedStreams;

    /** The current status of the cache entry. */
    final AtomicReference<EntryStatus> status;

    /** A function to be called when the cache entry is accessed. */
    private Runnable touchFunction;

    /** The epoch time of the second access. */
    long secondAccessEpoch = 0L;

    /** The count of times this entry has been promoted. */
    int accessCountInColdLink = 0;

    /** The count of times this entry has been evicted. */
    int evictCount = 0;

    /** The status of a file cache entry. */
    public enum EntryStatus {
        /** The cache entry is fully loaded and available for use. */
        LOADED,

        /** The cache entry is in the process of being loaded. */
        LOADING,

        /** The cache entry is invalid and should not be used. */
        INVALID,

        /** The cache entry is in the process of being removed. */
        REMOVING,

        /** The cache entry has been removed and is not available. Can be reopeneded for reading. */
        REMOVED,

        /** The cache entry is closed and no longer available for use permanently. */
        CLOSED
    }

    /**
     * Constructs a new FileCacheEntry. This entry is initially in the REMOVED state with no
     * reference.
     *
     * @param fileBasedCache the file-based cache that this entry belongs to
     * @param originalPath the original path of the file
     * @param cachePath the path in the cache
     * @param entrySize the size of the file
     */
    FileCacheEntry(
            FileBasedCache fileBasedCache, Path originalPath, Path cachePath, long entrySize) {
        super(0);
        this.fileBasedCache = fileBasedCache;
        this.cacheFs = fileBasedCache.cacheFs;
        this.originalPath = originalPath;
        this.cachePath = cachePath;
        this.entrySize = entrySize;
        this.openedStreams = new LinkedBlockingQueue<>();
        this.status = new AtomicReference<>(EntryStatus.REMOVED);
        LOG.trace("Create new cache entry {}.", cachePath);
    }

    /**
     * Opens a new {@link CachedDataInputStream} from this cache entry. If the cache stream is
     * available, it will be used; otherwise, the original stream will be used. But the cache stream
     * will be used once available. The opened stream is added to the queue of opened streams
     * associated with this cache entry.
     *
     * @param originalStream the original input stream to be used if the cache stream is not
     *     available
     * @return a new {@link CachedDataInputStream} for reading data
     * @throws IOException if an I/O error occurs while opening the stream
     */
    public CachedDataInputStream open(FSDataInputStream originalStream) throws IOException {
        LOG.trace("Open new stream for cache entry {}.", cachePath);
        FSDataInputStream cacheStream = getCacheStream();
        if (cacheStream != null) {
            CachedDataInputStream inputStream =
                    new CachedDataInputStream(fileBasedCache, this, cacheStream, originalStream);
            openedStreams.add(inputStream);
            release();
            return inputStream;
        } else {
            CachedDataInputStream inputStream =
                    new CachedDataInputStream(fileBasedCache, this, originalStream);
            openedStreams.add(inputStream);
            return inputStream;
        }
    }

    /**
     * Retrieves the cached input stream for this cache entry if it is available and the entry is in
     * a valid state. The method attempts to open the cached stream if the entry is in the LOADED
     * state and retains a reference to it.
     *
     * @return the cached input stream if available, otherwise null
     * @throws IOException if an I/O error occurs while opening the cached stream
     */
    FSDataInputStream getCacheStream() throws IOException {
        if (status.get() == EntryStatus.LOADED && tryRetain() > 0) {
            return cacheFs.open(cachePath);
        }
        return null;
    }

    /**
     * Sets the touch function associated with this cache entry. The reason for setting the touch
     * function is to update the entry order in {@link FileBasedCache}. The touch function is not
     * initialized in constructor, since the node in LRU should be created before the touch function
     * is available, and this all happens after this entry is built.
     */
    void setTouchFunction(Runnable touchFunction) {
        this.touchFunction = touchFunction;
    }

    /**
     * Invokes the touch function associated with this cache entry. This method is called to
     * indicate that the cache entry has been accessed, and as a result, the entry order in {@link
     * FileBasedCache} is expected to be updated.
     */
    void touch() {
        if (touchFunction != null) {
            touchFunction.run();
        }
    }

    /**
     * Loads the file from the original path to the cache path. This method reads the file from the
     * original path and writes it to the cache path. If the file is successfully loaded, the cache
     * path is returned. If an I/O error occurs during the loading process, null is returned.
     *
     * @see FileBasedCache#movedToFirst(FileCacheEntry)
     * @return the cache path if the file is successfully loaded, otherwise null.
     */
    Path load() {
        FSDataInputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        try {
            final byte[] buffer = new byte[READ_BUFFER_SIZE];

            inputStream = originalPath.getFileSystem().open(originalPath, READ_BUFFER_SIZE);

            outputStream = cacheFs.create(cachePath, FileSystem.WriteMode.OVERWRITE);

            long maxTransferBytes =
                    originalPath.getFileSystem().getFileStatus(originalPath).getLen();

            while (maxTransferBytes > 0) {
                int maxReadBytes = (int) Math.min(maxTransferBytes, READ_BUFFER_SIZE);
                int readBytes = inputStream.read(buffer, 0, maxReadBytes);

                if (readBytes == -1) {
                    break;
                }

                outputStream.write(buffer, 0, readBytes);

                maxTransferBytes -= readBytes;
            }
            return cachePath;
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Only two scenario that the reference count can reach 0. 1. The cache entry is invalid the
     * reference count is released. {@see invalidate()} 2. The cache entry is closed and the
     * reference count is scheduled released. {@see close()}
     */
    @Override
    protected void referenceCountReachedZero(@Nullable Object o) {
        if (switchStatus(EntryStatus.INVALID, EntryStatus.REMOVING)
                || checkStatus(EntryStatus.CLOSED)) {
            fileBasedCache.removeFile(this);
        }
    }

    /**
     * Removes the cache file associated with this cache entry. This method deletes the cache file
     * and closes all opened streams associated with this cache entry.
     */
    synchronized void doRemoveFile() {
        try {
            Iterator<CachedDataInputStream> iterator = openedStreams.iterator();
            while (iterator.hasNext()) {
                CachedDataInputStream stream = iterator.next();
                if (stream.isClosed()) {
                    iterator.remove();
                } else {
                    stream.closeCachedStream();
                }
            }
            cacheFs.delete(cachePath, false);
            if (status.get() != EntryStatus.CLOSED) {
                status.set(FileCacheEntry.EntryStatus.REMOVED);
            }
        } catch (Exception e) {
            LOG.warn("Failed to delete cache entry {}.", cachePath, e);
        }
    }

    // -----------------------------------------------------
    // Status change methods, invoked by different threads.
    // -----------------------------------------------------

    synchronized void loaded() {
        // 0 -> 1
        if (checkStatus(EntryStatus.LOADED)) {
            retain();
        }
    }

    /**
     * Invalidate the cache entry if it is LOADED.
     *
     * @return true if the cache entry is actually invalidated, false otherwise.
     */
    synchronized boolean invalidate() {
        if (switchStatus(EntryStatus.LOADED, EntryStatus.INVALID)) {
            release();
            return true;
        }
        return false;
    }

    synchronized void close() {
        if (getAndSetStatus(EntryStatus.CLOSED) == EntryStatus.LOADED) {
            release();
        } else {
            status.set(EntryStatus.CLOSED);
        }
    }

    // ----------------------------
    // Status related methods
    // ----------------------------

    boolean switchStatus(EntryStatus from, EntryStatus to) {
        if (status.compareAndSet(from, to)) {
            LOG.trace(
                    "Cache {} (for {}) Switch status from {} to {}.",
                    originalPath,
                    cachePath,
                    from,
                    to);
            return true;
        } else {
            return false;
        }
    }

    boolean checkStatus(EntryStatus to) {
        return status.get() == to;
    }

    EntryStatus getAndSetStatus(EntryStatus to) {
        return status.getAndSet(to);
    }
}

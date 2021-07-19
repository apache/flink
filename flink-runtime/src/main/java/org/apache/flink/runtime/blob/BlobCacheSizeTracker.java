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
 * limitations under the License
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * BlobCacheSizeTracker uses {@link LinkedHashMap} to maintain the LRU order for the files in the
 * cache. When new files are intended to be put into cache, {@code checkLimit} is called to query
 * the files should be removed. This tracker maintains a lock to avoid concurrency modification. To
 * avoid the deadlock, make sure that hold the READ/WRITE lock in {@link AbstractBlobCache} first
 * and then hold the lock here.
 */
public class BlobCacheSizeTracker {

    private static final Logger LOG = LoggerFactory.getLogger(BlobCacheSizeTracker.class);

    private static final int INITIAL_SIZE = 10_000;

    private final Object lock = new Object();

    private final long capacity;

    @GuardedBy("lock")
    private long total;

    @GuardedBy("lock")
    private final LinkedHashMap<Tuple2<JobID, BlobKey>, Long> caches;

    public BlobCacheSizeTracker(long capacity) {
        this.capacity = capacity;
        this.total = 0L;
        this.caches = new LinkedHashMap<>(INITIAL_SIZE, 0.75F, true);
    }

    /**
     * Check the size limit and return the files to delete.
     *
     * <p>NOTE: Only called after holding the WRITE lock.
     *
     * @param size size of the target file intended to put into cache
     * @return list of files to delete before saving the target file
     */
    public List<Tuple2<JobID, BlobKey>> checkLimit(long size) {
        checkArgument(size >= 0);

        synchronized (lock) {
            List<Tuple2<JobID, BlobKey>> fileToDelete = new ArrayList<>();

            long current = total;

            for (Map.Entry<Tuple2<JobID, BlobKey>, Long> entry : caches.entrySet()) {
                if (current + size > capacity) {
                    fileToDelete.add(entry.getKey());
                    current -= entry.getValue();
                }
            }

            return fileToDelete;
        }
    }

    /**
     * Register the target file to the tracker.
     *
     * <p>NOTE: Only called after holding the WRITE lock.
     */
    public void add(@Nullable JobID jobId, BlobKey blobKey, long size) {
        checkNotNull(blobKey);
        checkArgument(size >= 0);

        synchronized (lock) {
            caches.put(Tuple2.of(jobId, blobKey), size);
            total += size;
            if (total > capacity) {
                LOG.warn(
                        "The capacity of blob cache exceeds the limit, capacity: {}, current: {}. This may indicates the size of file exceeds the size limit.",
                        capacity,
                        total);
            }
        }
    }

    /**
     * Remove the file from the tracker.
     *
     * <p>NOTE: Only called after the WRITE lock is held and the file has been delete.
     */
    public void remove(Tuple2<JobID, BlobKey> key) {
        checkNotNull(key);

        synchronized (lock) {
            Long size = caches.remove(key);
            if (size != null && size >= 0) {
                total -= size;
            }
        }
    }

    /**
     * Remove the file from the tracker.
     *
     * <p>NOTE: Only called after the WRITE lock is held and the file has been delete.
     */
    public void remove(@Nullable JobID jobId, BlobKey blobKey) {
        checkNotNull(blobKey);

        synchronized (lock) {
            remove(Tuple2.of(jobId, blobKey));
        }
    }

    /**
     * Update the last used index for the file so that the tracker can easily find out the last
     * recently used file.
     *
     * <p>NOTE: Only called after holding the READ lock.
     */
    public void update(@Nullable JobID jobId, BlobKey blobKey) {
        checkNotNull(blobKey);

        synchronized (lock) {
            caches.get(Tuple2.of(jobId, blobKey));
        }
    }
}

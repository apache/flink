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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * BlobCacheSizeTracker uses {@link LinkedHashMap} to maintain the LRU order for the files in the
 * cache. When new files are intended to be put into cache, {@code checkLimit} is called to query
 * the files should be removed. This tracker maintains a lock to avoid concurrent modification. To
 * avoid the inconsistency, make sure that hold the READ/WRITE lock in {@link PermanentBlobCache}
 * first and then hold the lock here.
 */
public class BlobCacheSizeTracker {

    private static final Logger LOG = LoggerFactory.getLogger(BlobCacheSizeTracker.class);

    private static final int INITIAL_SIZE = 10_000;

    private final Object lock = new Object();

    protected final long sizeLimit;

    @GuardedBy("lock")
    private long total;

    @GuardedBy("lock")
    private final LinkedHashMap<Tuple2<JobID, BlobKey>, Long> caches;

    @GuardedBy("lock")
    private final HashMap<JobID, Set<BlobKey>> blobKeyByJob;

    public BlobCacheSizeTracker(long sizeLimit) {
        checkArgument(sizeLimit > 0);

        this.sizeLimit = sizeLimit;
        this.total = 0L;
        this.caches = new LinkedHashMap<>(INITIAL_SIZE, 0.75F, true);
        this.blobKeyByJob = new HashMap<>();
    }

    /**
     * Check the size limit and return the BLOBs to delete.
     *
     * @param size size of the BLOB intended to put into the cache
     * @return list of BLOBs to delete before putting into the target BLOB
     */
    public List<Tuple2<JobID, BlobKey>> checkLimit(long size) {
        checkArgument(size >= 0);

        synchronized (lock) {
            List<Tuple2<JobID, BlobKey>> blobsToDelete = new ArrayList<>();

            long current = total;

            for (Map.Entry<Tuple2<JobID, BlobKey>, Long> entry : caches.entrySet()) {
                if (current + size > sizeLimit) {
                    blobsToDelete.add(entry.getKey());
                    current -= entry.getValue();
                }
            }

            return blobsToDelete;
        }
    }

    /** Register the BLOB to the tracker. */
    public void track(JobID jobId, BlobKey blobKey, long size) {
        checkNotNull(jobId);
        checkNotNull(blobKey);
        checkArgument(size >= 0);

        synchronized (lock) {
            if (caches.putIfAbsent(Tuple2.of(jobId, blobKey), size) == null) {
                blobKeyByJob.computeIfAbsent(jobId, ignore -> new HashSet<>()).add(blobKey);

                total += size;
                if (total > sizeLimit) {
                    LOG.warn(
                            "The overall size of BLOBs in the cache exceeds "
                                    + "the limit. Limit = [{}], Current: [{}], "
                                    + "The size of next BLOB: [{}].",
                            sizeLimit,
                            total,
                            size);
                }
            } else {
                LOG.warn(
                        "Attempt to track a duplicated BLOB. This may indicate a duplicate upload "
                                + "or a hash collision. Ignoring newest upload. "
                                + "JobID = [{}], BlobKey = [{}]",
                        jobId,
                        blobKey);
            }
        }
    }

    /** Remove the BLOB from the tracker. */
    public void untrack(Tuple2<JobID, BlobKey> key) {
        checkNotNull(key);
        checkNotNull(key.f0);
        checkNotNull(key.f1);

        synchronized (lock) {
            blobKeyByJob.computeIfAbsent(key.f0, ignore -> new HashSet<>()).remove(key.f1);

            Long size = caches.remove(key);
            if (size != null) {
                checkState(size >= 0);
                total -= size;
            }
        }
    }

    /** Remove the BLOB from the tracker. */
    private void untrack(JobID jobId, BlobKey blobKey) {
        checkNotNull(jobId);
        checkNotNull(blobKey);

        untrack(Tuple2.of(jobId, blobKey));
    }

    /**
     * Update the least used index for the BLOBs so that the tracker can easily find out the least
     * recently used BLOBs.
     */
    public void update(JobID jobId, BlobKey blobKey) {
        checkNotNull(jobId);
        checkNotNull(blobKey);

        synchronized (lock) {
            caches.get(Tuple2.of(jobId, blobKey));
        }
    }

    /** Unregister all the tracked BLOBs related to the given job. */
    public void untrackAll(JobID jobId) {
        checkNotNull(jobId);

        synchronized (lock) {
            Set<BlobKey> keysToRemove = blobKeyByJob.remove(jobId);
            if (keysToRemove != null) {
                for (BlobKey key : keysToRemove) {
                    untrack(jobId, key);
                }
            }
        }
    }

    @VisibleForTesting
    Long getSize(JobID jobId, BlobKey blobKey) {
        checkNotNull(jobId);
        checkNotNull(blobKey);

        synchronized (lock) {
            return caches.get(Tuple2.of(jobId, blobKey));
        }
    }

    @VisibleForTesting
    Set<BlobKey> getBlobKeysByJobId(JobID jobId) {
        checkNotNull(jobId);

        synchronized (lock) {
            return blobKeyByJob.getOrDefault(jobId, Collections.emptySet());
        }
    }
}

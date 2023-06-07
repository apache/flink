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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;

import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalNotification;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implement of {@link ShuffleDescriptorsCache}. Entries will be expired after timeout. */
public class DefaultShuffleDescriptorsCache implements ShuffleDescriptorsCache {
    private final Cache<PermanentBlobKey, ShuffleDescriptorCacheEntry> shuffleDescriptorsCache;
    private final Map<JobID, Set<PermanentBlobKey>> cachedBlobKeysPerJob;

    private DefaultShuffleDescriptorsCache(
            Duration expireTimeout, int cacheSizeLimit, Ticker ticker) {
        this.cachedBlobKeysPerJob = new HashMap<>();
        this.shuffleDescriptorsCache =
                CacheBuilder.newBuilder()
                        .concurrencyLevel(1)
                        .maximumSize(cacheSizeLimit)
                        .expireAfterAccess(expireTimeout)
                        .ticker(ticker)
                        .removalListener(this::onCacheRemoval)
                        .build();
    }

    @Override
    public void clear() {
        cachedBlobKeysPerJob.clear();
        shuffleDescriptorsCache.cleanUp();
    }

    @Override
    public ShuffleDescriptorGroup get(PermanentBlobKey blobKey) {
        ShuffleDescriptorCacheEntry entry = shuffleDescriptorsCache.getIfPresent(blobKey);
        return entry == null ? null : entry.getShuffleDescriptorGroup();
    }

    @Override
    public void put(
            JobID jobId, PermanentBlobKey blobKey, ShuffleDescriptorGroup shuffleDescriptorGroup) {
        shuffleDescriptorsCache.put(
                blobKey, new ShuffleDescriptorCacheEntry(shuffleDescriptorGroup, jobId));
        cachedBlobKeysPerJob.computeIfAbsent(jobId, ignore -> new HashSet<>()).add(blobKey);
    }

    @Override
    public void clearCacheForJob(JobID jobId) {
        Set<PermanentBlobKey> removed = cachedBlobKeysPerJob.remove(jobId);
        if (removed != null) {
            shuffleDescriptorsCache.invalidateAll(removed);
        }
    }

    /**
     * Removal listener that remove the index of serializedShuffleDescriptorsPerJob .
     *
     * @param removalNotification of removed element.
     */
    private void onCacheRemoval(
            RemovalNotification<PermanentBlobKey, ShuffleDescriptorCacheEntry>
                    removalNotification) {
        PermanentBlobKey blobKey = removalNotification.getKey();
        ShuffleDescriptorCacheEntry entry = removalNotification.getValue();
        if (blobKey != null && entry != null) {
            cachedBlobKeysPerJob.computeIfPresent(
                    entry.getJobId(),
                    (jobID, permanentBlobKeys) -> {
                        permanentBlobKeys.remove(blobKey);
                        if (permanentBlobKeys.isEmpty()) {
                            return null;
                        } else {
                            return permanentBlobKeys;
                        }
                    });
        }
    }

    private static class ShuffleDescriptorCacheEntry {
        private final ShuffleDescriptorGroup shuffleDescriptorGroup;
        private final JobID jobId;

        public ShuffleDescriptorCacheEntry(
                ShuffleDescriptorGroup shuffleDescriptorGroup, JobID jobId) {
            this.shuffleDescriptorGroup = checkNotNull(shuffleDescriptorGroup);
            this.jobId = checkNotNull(jobId);
        }

        public ShuffleDescriptorGroup getShuffleDescriptorGroup() {
            return shuffleDescriptorGroup;
        }

        public JobID getJobId() {
            return jobId;
        }
    }

    /** The Factory of {@link DefaultShuffleDescriptorsCache}. */
    public static class Factory {
        private static final Duration DEFAULT_CACHE_EXPIRE_TIMEOUT = Duration.ofSeconds(300);
        private static final int DEFAULT_CACHE_SIZE_LIMIT = 100;
        private static final Ticker DEFAULT_TICKER = Ticker.systemTicker();

        private final Duration cacheExpireTimeout;
        private final int cacheSizeLimit;
        private final Ticker ticker;

        public Factory() {
            this(DEFAULT_CACHE_EXPIRE_TIMEOUT, DEFAULT_CACHE_SIZE_LIMIT, DEFAULT_TICKER);
        }

        @VisibleForTesting
        public Factory(Duration cacheExpireTimeout, int cacheSizeLimit, Ticker ticker) {
            this.cacheExpireTimeout = cacheExpireTimeout;
            this.cacheSizeLimit = cacheSizeLimit;
            this.ticker = ticker;
        }

        public DefaultShuffleDescriptorsCache create() {
            return new DefaultShuffleDescriptorsCache(cacheExpireTimeout, cacheSizeLimit, ticker);
        }
    }
}

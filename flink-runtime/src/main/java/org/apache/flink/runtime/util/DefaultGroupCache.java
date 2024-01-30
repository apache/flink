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

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.shaded.guava31.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalNotification;

import javax.annotation.concurrent.NotThreadSafe;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Default implement of {@link GroupCache}. Entries will be expired after timeout. */
@NotThreadSafe
public class DefaultGroupCache<G, K, V> implements GroupCache<G, K, V> {
    private final Cache<CacheKey<G, K>, V> cache;
    private final Map<G, Set<CacheKey<G, K>>> cachedBlobKeysPerJob;

    private DefaultGroupCache(Duration expireTimeout, int cacheSizeLimit, Ticker ticker) {
        this.cachedBlobKeysPerJob = new HashMap<>();
        this.cache =
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
        cache.cleanUp();
    }

    @Override
    public V get(G group, K key) {
        return cache.getIfPresent(new CacheKey<>(group, key));
    }

    @Override
    public void put(G group, K key, V value) {
        CacheKey<G, K> cacheKey = new CacheKey<>(group, key);
        cache.put(cacheKey, value);
        cachedBlobKeysPerJob.computeIfAbsent(group, ignore -> new HashSet<>()).add(cacheKey);
    }

    @Override
    public void clearCacheForGroup(G group) {
        Set<CacheKey<G, K>> removed = cachedBlobKeysPerJob.remove(group);
        if (removed != null) {
            cache.invalidateAll(removed);
        }
    }

    /**
     * Removal listener that remove the cache key of this group .
     *
     * @param removalNotification of removed element.
     */
    private void onCacheRemoval(RemovalNotification<CacheKey<G, K>, V> removalNotification) {
        CacheKey<G, K> cacheKey = removalNotification.getKey();
        V value = removalNotification.getValue();
        if (cacheKey != null && value != null) {
            cachedBlobKeysPerJob.computeIfPresent(
                    cacheKey.getGroup(),
                    (group, keys) -> {
                        keys.remove(cacheKey);
                        if (keys.isEmpty()) {
                            return null;
                        } else {
                            return keys;
                        }
                    });
        }
    }

    private static class CacheKey<G, K> {
        private final G group;
        private final K key;

        public CacheKey(G group, K key) {
            this.group = group;
            this.key = key;
        }

        public G getGroup() {
            return group;
        }

        public K getKey() {
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey<?, ?> cacheKey = (CacheKey<?, ?>) o;
            return Objects.equals(group, cacheKey.group) && Objects.equals(key, cacheKey.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, key);
        }
    }

    /** The Factory of {@link DefaultGroupCache}. */
    public static class Factory<G, K, V> {
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

        public DefaultGroupCache<G, K, V> create() {
            return new DefaultGroupCache<>(cacheExpireTimeout, cacheSizeLimit, ticker);
        }
    }
}

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

package org.apache.flink.table.connector.source.lookup.cache;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.clock.Clock;

import org.apache.flink.shaded.guava30.com.google.common.base.Ticker;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;

import static org.apache.flink.table.connector.source.lookup.LookupOptions.CACHE_TYPE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType.PARTIAL;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE;
import static org.apache.flink.table.connector.source.lookup.LookupOptions.PARTIAL_CACHE_MAX_ROWS;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LookupCache}. */
@PublicEvolving
public class DefaultLookupCache implements LookupCache {
    private static final long serialVersionUID = 6839408984141411172L;

    // Configurations of the cache
    private final Duration expireAfterAccessDuration;
    private final Duration expireAfterWriteDuration;
    private final Long maximumSize;
    private final boolean cacheMissingKey;

    // The underlying Guava cache implementation
    private transient Cache<RowData, Collection<RowData>> guavaCache;

    // Guava Ticker for testing expiration
    private transient Ticker ticker;

    // For tracking cache metrics
    private transient Counter hitCounter;
    private transient Counter missCounter;

    private DefaultLookupCache(
            Duration expireAfterAccessDuration,
            Duration expireAfterWriteDuration,
            Long maximumSize,
            boolean cacheMissingKey) {
        this.expireAfterAccessDuration = expireAfterAccessDuration;
        this.expireAfterWriteDuration = expireAfterWriteDuration;
        this.maximumSize = maximumSize;
        this.cacheMissingKey = cacheMissingKey;
    }

    /** Creates a builder for the cache. */
    public static Builder newBuilder() {
        return new Builder();
    }

    public static DefaultLookupCache fromConfig(ReadableConfig config) {
        checkArgument(
                config.get(CACHE_TYPE).equals(PARTIAL),
                "'%s' should be '%s' in order to build a default lookup cache",
                CACHE_TYPE.key(),
                PARTIAL);
        return new DefaultLookupCache(
                config.get(PARTIAL_CACHE_EXPIRE_AFTER_ACCESS),
                config.get(PARTIAL_CACHE_EXPIRE_AFTER_WRITE),
                config.get(PARTIAL_CACHE_MAX_ROWS),
                config.get(PARTIAL_CACHE_CACHE_MISSING_KEY));
    }

    @Override
    public void open(CacheMetricGroup metricGroup) {
        synchronized (this) {
            // The cache should already been opened if guava cache is not null
            if (guavaCache != null) {
                return;
            }
            // Initialize Guava cache
            CacheBuilder<Object, Object> guavaCacheBuilder = CacheBuilder.newBuilder();
            if (expireAfterAccessDuration != null) {
                guavaCacheBuilder.expireAfterAccess(expireAfterAccessDuration);
            }
            if (expireAfterWriteDuration != null) {
                guavaCacheBuilder.expireAfterWrite(expireAfterWriteDuration);
            }
            if (maximumSize != null) {
                guavaCacheBuilder.maximumSize(maximumSize);
            }
            if (ticker != null) {
                guavaCacheBuilder.ticker(ticker);
            }
            guavaCache = guavaCacheBuilder.build();

            // Initialize and register metrics
            // Here we can't reuse Guava cache statistics because guavaCache#getIfPresent is not
            // counted in the stat
            hitCounter = new ThreadSafeSimpleCounter();
            missCounter = new ThreadSafeSimpleCounter();
            metricGroup.hitCounter(hitCounter);
            metricGroup.missCounter(missCounter);
            metricGroup.numCachedRecordsGauge(() -> guavaCache.size());
        }
    }

    @Nullable
    @Override
    public Collection<RowData> getIfPresent(RowData key) {
        Collection<RowData> value = guavaCache.getIfPresent(key);
        if (value != null) {
            hitCounter.inc();
        } else {
            missCounter.inc();
        }
        return value;
    }

    @Override
    public Collection<RowData> put(RowData key, Collection<RowData> value) {
        checkNotNull(key, "Cannot put an entry with null key into the cache");
        checkNotNull(value, "Cannot put an entry with null value into the cache");
        if (!value.isEmpty()) {
            guavaCache.put(key, value);
        } else {
            if (cacheMissingKey) {
                guavaCache.put(key, value);
            }
        }
        return value;
    }

    @Override
    public void invalidate(RowData key) {
        guavaCache.invalidate(key);
    }

    @Override
    public long size() {
        return guavaCache.size();
    }

    @Override
    public void close() throws Exception {
        if (guavaCache != null) {
            guavaCache.invalidateAll();
            guavaCache.cleanUp();
        }
    }

    @VisibleForTesting
    void withClock(Clock clock) {
        ticker =
                new Ticker() {
                    @Override
                    public long read() {
                        return clock.relativeTimeNanos();
                    }
                };
    }

    @VisibleForTesting
    Duration getExpireAfterAccessDuration() {
        return expireAfterAccessDuration;
    }

    @VisibleForTesting
    Duration getExpireAfterWriteDuration() {
        return expireAfterWriteDuration;
    }

    @VisibleForTesting
    Long getMaximumSize() {
        return maximumSize;
    }

    @VisibleForTesting
    boolean isCacheMissingKey() {
        return cacheMissingKey;
    }

    /** Builder for {@link DefaultLookupCache}. */
    @PublicEvolving
    public static class Builder {
        private Duration expireAfterAccessDuration;
        private Duration expireAfterWriteDuration;
        private Long maximumSize;
        private boolean cacheMissingKey = true;

        /**
         * Specifies the duration after an entry is last accessed that it should be automatically
         * removed.
         */
        public Builder expireAfterAccess(Duration duration) {
            expireAfterAccessDuration = duration;
            return this;
        }

        /**
         * Specifies the duration after an entry is created that it should be automatically removed.
         */
        public Builder expireAfterWrite(Duration duration) {
            expireAfterWriteDuration = duration;
            return this;
        }

        /** Specifies the maximum number of entries of the cache. */
        public Builder maximumSize(long maximumSize) {
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Specifies whether to cache empty value into the cache.
         *
         * <p>Please note that "empty" means a collection without any rows in it instead of null.
         * The cache will not accept any null key or value.
         */
        public Builder cacheMissingKey(boolean cacheMissingKey) {
            this.cacheMissingKey = cacheMissingKey;
            return this;
        }

        /** Creates the cache. */
        public DefaultLookupCache build() {
            sanityCheck();
            return new DefaultLookupCache(
                    expireAfterAccessDuration,
                    expireAfterWriteDuration,
                    maximumSize,
                    cacheMissingKey);
        }

        private void sanityCheck() {
            if (expireAfterWriteDuration == null
                    && expireAfterAccessDuration == null
                    && maximumSize == null) {
                throw new IllegalArgumentException(
                        "Expiration duration and maximum size are not set for the cache. "
                                + "This could lead to potential memory issues as the cache size "
                                + "may grow infinitely.");
            }
        }
    }
}

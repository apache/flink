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

package org.apache.flink.table.runtime.functions.table.lookup;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.FunctionContext;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper function around user-provided async lookup function with a cache layer.
 *
 * <p>This function will check the cache on lookup request and return entries directly on cache hit,
 * otherwise the function will invoke the actual lookup function, and store the entry into the cache
 * after lookup for later use.
 */
public class CachingAsyncLookupFunction extends AsyncLookupFunction {
    private static final long serialVersionUID = 1L;

    // Constants
    public static final String LOOKUP_CACHE_METRIC_GROUP_NAME = "cache";
    private static final long UNINITIALIZED = -1;

    // The actual user-provided lookup function
    private final AsyncLookupFunction delegate;

    private LookupCache cache;
    private transient String cacheIdentifier;

    // Cache metrics
    private transient CacheMetricGroup cacheMetricGroup;
    private transient Counter loadCounter;
    private transient Counter numLoadFailuresCounter;
    private volatile long latestLoadTime = UNINITIALIZED;

    public CachingAsyncLookupFunction(LookupCache cache, AsyncLookupFunction delegate) {
        this.cache = cache;
        this.delegate = delegate;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        // Get the shared cache from manager
        cacheIdentifier = functionIdentifier();
        cache = LookupCacheManager.getInstance().registerCacheIfAbsent(cacheIdentifier, cache);

        // Register metrics
        cacheMetricGroup =
                new InternalCacheMetricGroup(
                        context.getMetricGroup(), LOOKUP_CACHE_METRIC_GROUP_NAME);
        loadCounter = new ThreadSafeSimpleCounter();
        cacheMetricGroup.loadCounter(loadCounter);
        numLoadFailuresCounter = new ThreadSafeSimpleCounter();
        cacheMetricGroup.numLoadFailuresCounter(numLoadFailuresCounter);

        cache.open(cacheMetricGroup);
        delegate.open(context);
    }

    @Override
    public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
        Collection<RowData> cachedValues = cache.getIfPresent(keyRow);
        if (cachedValues != null) {
            return CompletableFuture.completedFuture(cachedValues);
        } else {
            long loadStartTime = System.currentTimeMillis();
            return delegate.asyncLookup(keyRow)
                    .whenComplete(
                            (lookupValues, throwable) -> {
                                if (throwable != null) {
                                    // TODO: Should implement retry on failure logic as proposed in
                                    // FLIP-234
                                    numLoadFailuresCounter.inc();
                                    throw new RuntimeException(
                                            String.format("Failed to lookup key '%s'", keyRow),
                                            throwable);
                                }
                                updateLatestLoadTime(System.currentTimeMillis() - loadStartTime);
                                loadCounter.inc();
                                Collection<RowData> cachingValues = lookupValues;
                                if (lookupValues == null || lookupValues.isEmpty()) {
                                    cachingValues = Collections.emptyList();
                                }
                                cache.put(keyRow, cachingValues);
                            });
        }
    }

    @Override
    public void close() throws Exception {
        delegate.close();
        if (cacheIdentifier != null) {
            LookupCacheManager.getInstance().unregisterCache(cacheIdentifier);
        }
    }

    @VisibleForTesting
    public LookupCache getCache() {
        return cache;
    }

    // --------------------------------- Helper functions ----------------------------
    private synchronized void updateLatestLoadTime(long loadTime) {
        if (latestLoadTime == UNINITIALIZED) {
            cacheMetricGroup.latestLoadTimeGauge(() -> latestLoadTime);
        }
        latestLoadTime = loadTime;
    }
}

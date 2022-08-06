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

public class CachingAsyncLookupFunction extends AsyncLookupFunction {

    // Constants
    public static final String LOOKUP_CACHE_METRIC_GROUP_NAME = "cache";
    private static final long UNINITIALIZED = -1;

    // The actual user-provided lookup function
    private final AsyncLookupFunction delegate;

    private String cacheIdentifier;
    private LookupCache cache;

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
            return delegate.asyncLookup(keyRow)
                    .whenComplete(
                            (lookupValues, throwable) -> {
                                if (throwable != null) {
                                    numLoadFailuresCounter.inc();
                                    throw new RuntimeException(
                                            String.format("Failed to lookup key '%s'", keyRow),
                                            throwable);
                                }
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
}

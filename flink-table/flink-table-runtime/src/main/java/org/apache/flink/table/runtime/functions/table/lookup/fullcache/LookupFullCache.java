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

package org.apache.flink.table.runtime.functions.table.lookup.fullcache;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.ThreadSafeSimpleCounter;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.connector.source.lookup.LookupOptions.LookupCacheType;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.connector.source.lookup.cache.trigger.CacheReloadTrigger;
import org.apache.flink.table.data.RowData;

import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Internal implementation of {@link LookupCache} for {@link LookupCacheType#FULL}. */
@Internal
public class LookupFullCache implements LookupCache {
    private static final long serialVersionUID = 1L;

    private final CacheLoader cacheLoader;
    private final CacheReloadTrigger reloadTrigger;

    private transient volatile ReloadTriggerContext reloadTriggerContext;
    private transient volatile Throwable reloadFailCause;

    // Cache metrics
    private transient Counter hitCounter; // equals to number of requests

    private transient ClassLoader userCodeClassLoader;

    public LookupFullCache(CacheLoader cacheLoader, CacheReloadTrigger reloadTrigger) {
        this.cacheLoader = checkNotNull(cacheLoader);
        this.reloadTrigger = checkNotNull(reloadTrigger);
    }

    public void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = userCodeClassLoader;
    }

    @Override
    public synchronized void open(CacheMetricGroup metricGroup) {
        if (hitCounter == null) {
            hitCounter = new ThreadSafeSimpleCounter();
        }
        metricGroup.hitCounter(hitCounter);
        metricGroup.missCounter(new SimpleCounter()); // always zero
        cacheLoader.initializeMetrics(metricGroup);

        if (reloadTriggerContext == null) {
            try {
                // TODO add Configuration into FunctionContext and pass in into LookupFullCache
                checkNotNull(
                        userCodeClassLoader,
                        "User code classloader must be initialized before opening full cache");
                cacheLoader.open(new Configuration(), userCodeClassLoader);
                reloadTriggerContext =
                        new ReloadTriggerContext(
                                cacheLoader::reloadAsync,
                                th -> {
                                    if (reloadFailCause == null) {
                                        reloadFailCause = th;
                                    } else {
                                        reloadFailCause.addSuppressed(th);
                                    }
                                });

                reloadTrigger.open(reloadTriggerContext);
                cacheLoader.awaitFirstLoad();
            } catch (Exception e) {
                throw new RuntimeException("Failed to open lookup 'FULL' cache.", e);
            }
        }
    }

    @Override
    public Collection<RowData> getIfPresent(RowData key) {
        if (reloadFailCause != null) {
            throw new RuntimeException(reloadFailCause);
        }
        Collection<RowData> result =
                cacheLoader.getCache().getOrDefault(key, Collections.emptyList());
        hitCounter.inc();
        return result;
    }

    @Override
    public Collection<RowData> put(RowData key, Collection<RowData> value) {
        throw new UnsupportedOperationException(
                "Lookup Full cache doesn't support public 'put' operation from the outside.");
    }

    @Override
    public void invalidate(RowData key) {
        throw new UnsupportedOperationException(
                "Lookup Full cache doesn't support public 'invalidate' operation from the outside.");
    }

    @Override
    public long size() {
        return cacheLoader.getCache().size();
    }

    @Override
    public void close() throws Exception {
        // stops scheduled thread pool that's responsible for scheduling cache updates
        reloadTrigger.close();
        // stops thread pool that's responsible for executing the actual cache update
        cacheLoader.close();
    }
}

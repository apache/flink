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

package org.apache.flink.table.runtime.operators.join.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.groups.CacheMetricGroup;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCacheManager;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** A helper class providing utils for accessing {@link LookupCache}. */
@Internal
public class LookupCacheHandler implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String tableIdentifier;

    // A projection which convert the input from left table to a row that only keeps the joining
    // keys. The cache should only store the joining keys as the cache key.
    private final GeneratedProjection generatedCacheKeyProjection;

    // Serializer for deep copy the key and value to be stored in the cache.
    // Make as nullable since copying is only required when object reuse is enabled.
    @Nullable private final RowDataSerializer cacheKeySerializer;
    @Nullable private final RowDataSerializer cacheValueSerializer;

    private LookupCache cache;
    private boolean initialized = false;

    // The runtime instance of the cache key projection
    private transient Projection<RowData, RowData> cacheKeyProjection;

    public LookupCacheHandler(
            LookupCache cache,
            String tableIdentifier,
            GeneratedProjection generatedCacheKeyProjection) {
        this(cache, tableIdentifier, generatedCacheKeyProjection, null, null);
    }

    public LookupCacheHandler(
            LookupCache cache,
            String tableIdentifier,
            GeneratedProjection generatedCacheKeyProjection,
            @Nullable RowDataSerializer cacheKeySerializer,
            @Nullable RowDataSerializer cacheValueSerializer) {
        this.cache = cache;
        this.tableIdentifier = tableIdentifier;
        this.generatedCacheKeyProjection = checkNotNull(generatedCacheKeyProjection);
        this.cacheKeySerializer = cacheKeySerializer;
        this.cacheValueSerializer = cacheValueSerializer;
    }

    @SuppressWarnings("unchecked")
    public void open(CacheMetricGroup metricGroup, ClassLoader classLoader) {
        // Try to register the holding cache into the manager, and the manager will return the
        // actual shared cache to use
        cache = LookupCacheManager.getInstance().registerCache(tableIdentifier, cache);
        cache.open(metricGroup);
        cacheKeyProjection = generatedCacheKeyProjection.newInstance(classLoader);
        initialized = true;
    }

    /** Project the input row from left table to get the key row. */
    public RowData getKeyRowFromInput(RowData in) {
        RowData keyRow = cacheKeyProjection.apply(in);
        keyRow.setRowKind(RowKind.INSERT);
        return keyRow;
    }

    /**
     * Put the key value pair into the cache. If object reuse is enabled, copy the key value pair
     * before storing into the cache.
     */
    public void maybeCopyThenPut(RowData key, Collection<RowData> value) {
        if (cacheKeySerializer != null && cacheValueSerializer != null) {
            RowData copiesKey = cacheKeySerializer.copy(key);
            Collection<RowData> copiedValues =
                    value.stream()
                            .map(cacheValueSerializer::copy)
                            .collect(Collectors.toCollection(ArrayList::new));
            getCache().put(copiesKey, copiedValues);
        } else {
            getCache().put(key, value);
        }
    }

    public LookupCache getCache() {
        checkInitialized();
        return cache;
    }

    private void checkInitialized() {
        checkState(initialized, "The cache handler is not initialized");
    }
}

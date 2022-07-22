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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.metrics.groups.InternalCacheMetricGroup;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.table.runtime.generated.GeneratedCollector;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The join runner to lookup the dimension table. */
public class LookupJoinRunner extends ProcessFunction<RowData, RowData> {
    private static final long serialVersionUID = -4521543015709964733L;

    public static final String LOOKUP_CACHE_METRIC_GROUP = "lookupCache";

    private final GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher;
    private final GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector;
    private final boolean isLeftOuterJoin;
    private final int tableFieldsCount;

    // For handling lookup caching, mark as nullable since caching might not be enabled
    @Nullable private final LookupCacheHandler cacheHandler;

    private transient FlatMapFunction<RowData, RowData> fetcher;
    protected transient TableFunctionCollector<RowData> collector;
    private transient GenericRowData nullRow;
    private transient JoinedRowData outRow;

    // For storing entries into the cache before collecting, mark as nullable since caching might
    // not be enabled
    @Nullable protected transient ResultCachingCollector cachingCollector;

    public LookupJoinRunner(
            GeneratedFunction<FlatMapFunction<RowData, RowData>> generatedFetcher,
            GeneratedCollector<TableFunctionCollector<RowData>> generatedCollector,
            boolean isLeftOuterJoin,
            int tableFieldsCount,
            @Nullable LookupCacheHandler cacheHandler) {
        this.generatedFetcher = generatedFetcher;
        this.generatedCollector = generatedCollector;
        this.isLeftOuterJoin = isLeftOuterJoin;
        this.tableFieldsCount = tableFieldsCount;
        this.cacheHandler = cacheHandler;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.fetcher = generatedFetcher.newInstance(getRuntimeContext().getUserCodeClassLoader());
        this.collector =
                generatedCollector.newInstance(getRuntimeContext().getUserCodeClassLoader());
        if (cacheHandler != null) {
            cachingCollector = new ResultCachingCollector(cacheHandler);
        }
        FunctionUtils.setFunctionRuntimeContext(fetcher, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(collector, getRuntimeContext());
        FunctionUtils.openFunction(fetcher, parameters);
        FunctionUtils.openFunction(collector, parameters);

        this.nullRow = new GenericRowData(tableFieldsCount);
        this.outRow = new JoinedRowData();
        maybeInitializeCacheHandler();
    }

    @Override
    public void processElement(RowData in, Context ctx, Collector<RowData> out) throws Exception {
        // Setup collector
        collector.setCollector(out);
        collector.setInput(in);
        collector.reset();

        if (cacheHandler != null) {
            lookupViaCache(in, out);
        } else {
            fetcher.flatMap(in, getFetcherCollector());
            maybeEmitNullForLeftOuterJoin(in, out);
        }
    }

    /**
     * Make a lookup via the cache.
     *
     * <p>This function checks the cache first and emits cached records to the collector directly on
     * cache hit, otherwise it will trigger a lookup in fetcher (user-provided logic) then store the
     * result into the cache.
     *
     * <p>Please notice that the key row stored in the cache will be the input from left table after
     * applying {@link LookupCacheHandler#getKeyRowFromInput}, and the value will be the raw value
     * returned by user's fetcher, which means no calculation and projection. For example:
     *
     * <p>- Input from left table (id, name): +I(1, Alice)
     *
     * <p>- Value return by user's fetcher (id, age, gender): +I(1, 18, female)
     *
     * <p>Then the entry stored in the cache would be: +I(1), +I(1, 18, female), even calculation
     * (age + 1) and projection (keep column "age" only) are applied.
     *
     * @param in input from left table
     * @param out final output collector, after applying projection on the lookup result (and
     *     calculating if using {@link LookupJoinWithCalcRunner}).
     */
    private void lookupViaCache(RowData in, Collector<RowData> out) throws Exception {
        assert cacheHandler != null;
        RowData keyRow = cacheHandler.getKeyRowFromInput(in);
        Collection<RowData> cachedValues = cacheHandler.getCache().getIfPresent(keyRow);
        if (cachedValues != null) {
            // Cache hit, collect rows stored in the cache directly
            cachedValues.forEach(getFetcherCollector()::collect);
        } else {
            assert cachingCollector != null;
            // Cache miss, trigger a lookup in fetcher and store the result in the cache
            cachingCollector.setDelegate(getFetcherCollector());
            cachingCollector.reset();
            fetcher.flatMap(in, cachingCollector);
            cachingCollector.storeInCache(keyRow);
        }
        maybeEmitNullForLeftOuterJoin(in, out);
    }

    public Collector<RowData> getFetcherCollector() {
        return collector;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (fetcher != null) {
            FunctionUtils.closeFunction(fetcher);
        }
        if (collector != null) {
            FunctionUtils.closeFunction(collector);
        }
        if (cacheHandler != null) {
            cacheHandler.getCache().close();
        }
    }

    @Nullable
    @VisibleForTesting
    public LookupCache getLookupCache() {
        return cacheHandler == null ? null : cacheHandler.getCache();
    }

    // ---------------------------- Helper methods --------------------------

    private void maybeEmitNullForLeftOuterJoin(RowData in, Collector<RowData> out) {
        if (!collector.isCollected() && isLeftOuterJoin) {
            outRow.replace(in, nullRow);
            outRow.setRowKind(in.getRowKind());
            out.collect(outRow);
        }
    }

    private void maybeInitializeCacheHandler() {
        if (cacheHandler == null) {
            return;
        }
        cacheHandler.open(
                new InternalCacheMetricGroup(
                        getRuntimeContext().getMetricGroup(), LOOKUP_CACHE_METRIC_GROUP),
                getRuntimeContext().getUserCodeClassLoader());
    }

    // ----------------------------- Helper classes --------------------------

    private static class ResultCachingCollector implements Collector<RowData> {

        private Collector<RowData> delegate;
        private final LookupCacheHandler cacheHandler;
        private List<RowData> currentValues = new ArrayList<>();

        public ResultCachingCollector(LookupCacheHandler cacheHandler) {
            this.cacheHandler = checkNotNull(cacheHandler);
        }

        public void setDelegate(Collector<RowData> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void collect(RowData record) {
            currentValues.add(record);
            delegate.collect(record);
        }

        public void storeInCache(RowData keyRow) {
            cacheHandler.maybeCopyThenPut(keyRow, currentValues);
        }

        public void reset() {
            currentValues = new ArrayList<>();
        }

        @Override
        public void close() {}
    }
}

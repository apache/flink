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

package org.apache.flink.table.runtime.operators.rank.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.rank.AbstractTopNFunction;
import org.apache.flink.table.runtime.operators.rank.FastTop1Function;
import org.apache.flink.table.runtime.operators.rank.TopNBufferCacheRemovalListener;
import org.apache.flink.table.runtime.operators.rank.async.AsyncStateFastTop1Function;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava32.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava32.com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A helper to help do the logic 'Top-1' in {@link FastTop1Function} and {@link
 * AsyncStateFastTop1Function}.
 */
public abstract class FastTop1Helper extends AbstractTopNFunction.AbstractTopNHelper {

    private static final Logger LOG = LoggerFactory.getLogger(FastTop1Helper.class);

    private final TypeSerializer<RowData> inputRowSer;

    // the kvMap stores mapping from partition key to its current top-1 value
    private final Cache<RowData, RowData> kvCache;

    private final long topNSize;

    public FastTop1Helper(
            AbstractTopNFunction topNFunction,
            TypeSerializer<RowData> inputRowSer,
            long cacheSize,
            long topNSize) {
        super(topNFunction);

        this.inputRowSer = inputRowSer;
        this.topNSize = topNSize;

        int lruCacheSize = Math.max(1, (int) (cacheSize / topNSize));
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (ttlConfig.isEnabled()) {
            cacheBuilder.expireAfterWrite(
                    ttlConfig.getTimeToLive().toMillis(), TimeUnit.MILLISECONDS);
        }

        this.kvCache =
                cacheBuilder
                        .maximumSize(lruCacheSize)
                        .removalListener(
                                new TopNBufferCacheRemovalListener<>(
                                        keyContext, this::flushBufferToState))
                        .build();

        LOG.info("Top-1 operator is using LRU caches key-size: {}", lruCacheSize);
    }

    @Nullable
    public RowData getPrevRowFromCache(RowData currentKey) {
        return kvCache.getIfPresent(currentKey);
    }

    public void processAsFirstRow(RowData input, RowData currentKey, Collector<RowData> out) {
        kvCache.put(currentKey, inputRowSer.copy(input));
        if (outputRankNumber) {
            // the rank end of top-1 is always 1L
            collectInsert(out, input, 1, 1);
        } else {
            collectInsert(out, input);
        }
    }

    public void processWithPrevRow(
            RowData input, RowData currentKey, RowData prevRow, Collector<RowData> out)
            throws Exception {
        RowData curSortKey = sortKeySelector.getKey(input);
        RowData oldSortKey = sortKeySelector.getKey(prevRow);
        int compare = sortKeyComparator.compare(curSortKey, oldSortKey);
        // current sort key is higher than old sort key
        if (compare < 0) {
            kvCache.put(currentKey, inputRowSer.copy(input));
            // Note: partition key is unique key if only top-1 is desired,
            //  thus emitting UB and UA here
            if (outputRankNumber) {
                // the rank end of top-1 is always 1L
                collectUpdateBefore(out, prevRow, 1, 1);
                collectUpdateAfter(out, input, 1, 1);
            } else {
                collectUpdateBefore(out, prevRow);
                collectUpdateAfter(out, input);
            }
        }
    }

    public void flushAllCacheToState() throws Exception {
        for (Map.Entry<RowData, RowData> entry : kvCache.asMap().entrySet()) {
            flushBufferToState(entry.getKey(), entry.getValue());
        }
    }

    public abstract void flushBufferToState(RowData currentKey, RowData value) throws Exception;

    public void registerMetric() {
        registerMetric(kvCache.size() * topNSize);
    }
}

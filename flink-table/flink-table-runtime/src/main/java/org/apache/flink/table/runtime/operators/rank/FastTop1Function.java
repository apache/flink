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

package org.apache.flink.table.runtime.operators.rank;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava31.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalCause;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalListener;
import org.apache.flink.shaded.guava31.com.google.common.cache.RemovalNotification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A more concise implementation for {@link AppendOnlyTopNFunction} and {@link
 * UpdatableTopNFunction} when only Top-1 is desired. This function can handle updating stream
 * because the RankProcessStrategy is inferred as UpdateFastStrategy, i.e., 1) the upsert key of
 * input steam contains partition key; 2) the sort field is updated monotonely under the upsert key.
 */
public class FastTop1Function extends AbstractTopNFunction implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(FastTop1Function.class);

    private final TypeSerializer<RowData> inputRowSer;
    private final long cacheSize;

    // a map state stores list of records
    private transient ValueState<RowData> dataState;

    // the kvMap stores mapping from partition key to its current top-1 value
    private transient Cache<RowData, RowData> kvCache;

    public FastTop1Function(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator generatedSortKeyComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            long cacheSize) {
        super(
                ttlConfig,
                inputRowType,
                generatedSortKeyComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);

        this.inputRowSer = inputRowType.createSerializer(new ExecutionConfig());
        this.cacheSize = cacheSize;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        int lruCacheSize = Math.max(1, (int) (cacheSize / getDefaultTopNSize()));
        CacheBuilder<Object, Object> cacheBuilder = CacheBuilder.newBuilder();
        if (ttlConfig.isEnabled()) {
            cacheBuilder.expireAfterWrite(
                    ttlConfig.getTtl().toMilliseconds(), TimeUnit.MILLISECONDS);
        }
        kvCache =
                cacheBuilder
                        .maximumSize(lruCacheSize)
                        .removalListener(new CacheRemovalListener())
                        .build();
        LOG.info("Top-1 operator is using LRU caches key-size: {}", lruCacheSize);

        ValueStateDescriptor<RowData> valueStateDescriptor =
                new ValueStateDescriptor<>("Top1-Rank-State", inputRowType);
        if (ttlConfig.isEnabled()) {
            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = getRuntimeContext().getState(valueStateDescriptor);

        // metrics
        registerMetric(kvCache.size() * getDefaultTopNSize());
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        requestCount += 1;
        // load state under current key if necessary
        RowData currentKey = (RowData) keyContext.getCurrentKey();
        RowData prevRow = kvCache.getIfPresent(currentKey);
        if (prevRow == null) {
            prevRow = dataState.value();
        } else {
            hitCount += 1;
        }

        // first row under current key.
        if (prevRow == null) {
            kvCache.put(currentKey, inputRowSer.copy(input));
            if (outputRankNumber) {
                collectInsert(out, input, 1);
            } else {
                collectInsert(out, input);
            }
            return;
        }

        RowData curSortKey = sortKeySelector.getKey(input);
        RowData oldSortKey = sortKeySelector.getKey(prevRow);
        int compare = sortKeyComparator.compare(curSortKey, oldSortKey);
        // current sort key is higher than old sort key
        if (compare < 0) {
            kvCache.put(currentKey, inputRowSer.copy(input));
            // Note: partition key is unique key if only top-1 is desired,
            //  thus emitting UB and UA here
            if (outputRankNumber) {
                collectUpdateBefore(out, prevRow, 1);
                collectUpdateAfter(out, input, 1);
            } else {
                collectUpdateBefore(out, prevRow);
                collectUpdateAfter(out, input);
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        for (Map.Entry<RowData, RowData> entry : kvCache.asMap().entrySet()) {
            keyContext.setCurrentKey(entry.getKey());
            dataState.update(entry.getValue());
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    private class CacheRemovalListener implements RemovalListener<RowData, RowData> {
        @Override
        public void onRemoval(RemovalNotification<RowData, RowData> notification) {
            if (notification.getCause() != RemovalCause.SIZE || notification.getValue() == null) {
                // Don't flush values to state if cause is ttl expired
                return;
            }

            RowData previousKey = (RowData) keyContext.getCurrentKey();
            RowData partitionKey = notification.getKey();
            keyContext.setCurrentKey(partitionKey);
            try {
                dataState.update(notification.getValue());
            } catch (Throwable e) {
                LOG.error("Fail to synchronize state!", e);
                throw new RuntimeException(e);
            } finally {
                keyContext.setCurrentKey(previousKey);
            }
        }
    }
}

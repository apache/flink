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

package org.apache.flink.table.runtime.operators.rank.async;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.rank.utils.AppendOnlyTopNHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A TopN function could handle insert-only stream.
 *
 * <p>The input stream should only contain INSERT messages.
 *
 * <p>Different with {@link AppendOnlyTopNFunction}, this function is used with async state api.
 */
public class AsyncStateAppendOnlyTopNFunction extends AbstractAsyncStateTopNFunction {

    private static final long serialVersionUID = 1L;

    private final InternalTypeInfo<RowData> sortKeyType;
    private final TypeSerializer<RowData> inputRowSer;
    private final long cacheSize;

    // a map state stores mapping from sort key to records list which is in topN
    private transient MapState<RowData, List<RowData>> dataState;

    private transient AsyncStateAppendOnlyTopNHelper helper;

    public AsyncStateAppendOnlyTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            GeneratedRecordComparator sortKeyGeneratedRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            long cacheSize) {
        super(
                ttlConfig,
                inputRowType,
                sortKeyGeneratedRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
        this.sortKeyType = sortKeySelector.getProducedType();
        this.inputRowSer = inputRowType.createSerializer(new SerializerConfigImpl());
        this.cacheSize = cacheSize;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        ListTypeInfo<RowData> valueTypeInfo = new ListTypeInfo<>(inputRowType);
        MapStateDescriptor<RowData, List<RowData>> mapStateDescriptor =
                new MapStateDescriptor<>("data-state-with-append", sortKeyType, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = getRuntimeContext().getMapState(mapStateDescriptor);

        helper = new AsyncStateAppendOnlyTopNHelper();

        helper.registerMetric();
    }

    @Override
    public void processElement(RowData input, Context context, Collector<RowData> out)
            throws Exception {
        StateFuture<TopNBuffer> topNBufferFuture = initHeapStates();
        StateFuture<Long> rankEndFuture = initRankEnd(input);

        RowData sortKey = sortKeySelector.getKey(input);
        topNBufferFuture.thenCombine(
                rankEndFuture,
                (buffer, rankEnd) -> {
                    if (checkSortKeyInBufferRange(sortKey, buffer)) {
                        // insert sort key into buffer
                        buffer.put(sortKey, inputRowSer.copy(input));
                        Collection<RowData> inputs = buffer.get(sortKey);

                        // update data state
                        // copy a new collection to avoid mutating state values, see
                        // CopyOnWriteStateMap,
                        // otherwise, the result might be corrupt.
                        // don't need to perform a deep copy, because RowData elements will not be
                        // updated
                        dataState
                                .asyncPut(sortKey, new ArrayList<>(inputs))
                                .thenAccept(
                                        VOID -> {
                                            if (outputRankNumber || hasOffset()) {
                                                // the without-number-algorithm can't handle topN
                                                // with offset,
                                                // so use the with-number-algorithm to handle offset
                                                helper.processElementWithRowNumber(
                                                        buffer, sortKey, input, rankEnd, out);
                                            } else {
                                                helper.processElementWithoutRowNumber(
                                                        buffer, input, rankEnd, out);
                                            }
                                        });
                    }

                    return null;
                });
    }

    private StateFuture<TopNBuffer> initHeapStates() {
        helper.accRequestCount();
        RowData currentKey = (RowData) keyContext.getCurrentKey();
        TopNBuffer bufferFromCache = helper.getTopNBufferFromCache(currentKey);
        if (bufferFromCache != null) {
            helper.accHitCount();
            return StateFutureUtils.completedFuture(bufferFromCache);
        }

        TopNBuffer bufferFromState = new TopNBuffer(sortKeyComparator, ArrayList::new);
        // restore buffer
        return dataState
                .asyncEntries()
                .thenCompose(
                        iter ->
                                iter.onNext(
                                        entry -> {
                                            RowData sortKey = entry.getKey();
                                            List<RowData> values = entry.getValue();
                                            // the order is preserved
                                            bufferFromState.putAll(sortKey, values);
                                        }))
                .thenApply(
                        VOID -> {
                            helper.saveTopNBufferToCache(currentKey, bufferFromState);
                            return bufferFromState;
                        });
    }

    private class AsyncStateAppendOnlyTopNHelper extends AppendOnlyTopNHelper {

        public AsyncStateAppendOnlyTopNHelper() {
            super(
                    AsyncStateAppendOnlyTopNFunction.this,
                    cacheSize,
                    AsyncStateAppendOnlyTopNFunction.this.getDefaultTopNSize());
        }

        @Override
        protected void removeFromState(RowData key) throws Exception {
            // no need to wait this async request to end
            dataState.asyncRemove(key);
        }

        @Override
        protected void updateState(RowData key, List<RowData> value) throws Exception {
            // no need to wait this async request to end
            dataState.asyncPut(key, value);
        }
    }
}

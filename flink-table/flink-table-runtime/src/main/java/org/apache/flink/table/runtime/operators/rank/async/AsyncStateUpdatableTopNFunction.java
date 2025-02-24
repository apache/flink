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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessingOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.TopNBuffer;
import org.apache.flink.table.runtime.operators.rank.utils.RankRow;
import org.apache.flink.table.runtime.operators.rank.utils.UpdatableTopNHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;

/**
 * A TopN function could handle updating stream. It is a fast version of {@link
 * RetractableTopNFunction} which only hold top n data in state, and keep sorted map in heap.
 * However, the function only works in some special scenarios: 1. sort field collation is ascending
 * and its mono is decreasing, or sort field collation is descending and its mono is increasing 2.
 * input data has unique keys and unique key must contain partition key 3. input stream could not
 * contain DELETE record or UPDATE_BEFORE record
 */
public class AsyncStateUpdatableTopNFunction extends AbstractAsyncStateTopNFunction
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final InternalTypeInfo<RowData> rowKeyType;
    private final long cacheSize;

    // a map state stores mapping from row key to record which is in topN
    // in tuple2, f0 is the record row, f1 is the index in the list of the same sort_key
    // the f1 is used to preserve the record order in the same sort_key
    private transient MapState<RowData, Tuple2<RowData, Integer>> dataState;

    // a HashMap stores mapping from rowKey to record, a heap mirror to dataState
    private transient Map<RowData, RankRow> rowKeyMap;

    private final TypeSerializer<RowData> inputRowSer;
    private final KeySelector<RowData, RowData> rowKeySelector;

    private transient AsyncStateUpdatableTopNHelper helper;

    public AsyncStateUpdatableTopNFunction(
            StateTtlConfig ttlConfig,
            InternalTypeInfo<RowData> inputRowType,
            RowDataKeySelector rowKeySelector,
            GeneratedRecordComparator generatedRecordComparator,
            RowDataKeySelector sortKeySelector,
            RankType rankType,
            RankRange rankRange,
            boolean generateUpdateBefore,
            boolean outputRankNumber,
            long cacheSize) {
        super(
                ttlConfig,
                inputRowType,
                generatedRecordComparator,
                sortKeySelector,
                rankType,
                rankRange,
                generateUpdateBefore,
                outputRankNumber);
        this.rowKeyType = rowKeySelector.getProducedType();
        this.cacheSize = cacheSize;
        this.inputRowSer = inputRowType.createSerializer(new SerializerConfigImpl());
        this.rowKeySelector = rowKeySelector;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                new TupleTypeInfo<>(inputRowType, Types.INT);
        MapStateDescriptor<RowData, Tuple2<RowData, Integer>> mapStateDescriptor =
                new MapStateDescriptor<>("data-state-with-update", rowKeyType, valueTypeInfo);
        if (ttlConfig.isEnabled()) {
            mapStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState = ((StreamingRuntimeContext) getRuntimeContext()).getMapState(mapStateDescriptor);

        helper = new AsyncStateUpdatableTopNHelper();

        // metrics
        helper.registerMetric();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    @Override
    public void processElement(RowData input, Context context, Collector<RowData> out)
            throws Exception {
        helper.initHeapStates();

        initRankEnd(input)
                .thenAccept(
                        VOID -> {
                            if (outputRankNumber || hasOffset()) {
                                // the without-number-algorithm can't handle topN with offset,
                                // so use the with-number-algorithm to handle offset
                                helper.processElementWithRowNumber(input, out);
                            } else {
                                helper.processElementWithoutRowNumber(input, out);
                            }
                        });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        helper.flushAllCacheToState();
    }

    private class AsyncStateUpdatableTopNHelper extends UpdatableTopNHelper {
        private AsyncStateUpdatableTopNHelper() {
            super(
                    AsyncStateUpdatableTopNFunction.this,
                    cacheSize,
                    getDefaultTopNSize(),
                    rowKeySelector,
                    inputRowSer,
                    inputRowType.getDataType(),
                    getRuntimeContext().getUserCodeClassLoader());
        }

        @Override
        protected void flushBufferToState(
                RowData currentKey, Tuple2<TopNBuffer, Map<RowData, RankRow>> bufferEntry)
                throws Exception {
            ((AsyncStateProcessingOperator) keyContext)
                    .asyncProcessWithKey(
                            currentKey,
                            () -> {
                                Map<RowData, RankRow> curRowKeyMap = bufferEntry.f1;
                                for (Map.Entry<RowData, RankRow> entry : curRowKeyMap.entrySet()) {
                                    RowData key = entry.getKey();
                                    RankRow rankRow = entry.getValue();
                                    if (rankRow.dirty) {
                                        // should update state
                                        dataState.put(
                                                key, Tuple2.of(rankRow.row, rankRow.innerRank));
                                        rankRow.dirty = false;
                                    }
                                }
                            });
        }

        @Override
        protected void removeDataState(RowData rowKey) throws Exception {
            // no need to wait for the future
            dataState.remove(rowKey);
        }

        @Override
        protected Iterator<Map.Entry<RowData, Tuple2<RowData, Integer>>> getDataStateIterator() {
            return dataState.iterator();
        }

        @Override
        protected boolean isInRankEnd(long rank) {
            return AsyncStateUpdatableTopNFunction.this.isInRankEnd(rank);
        }

        @Override
        protected long getRankEnd() {
            return rankEnd;
        }
    }
}

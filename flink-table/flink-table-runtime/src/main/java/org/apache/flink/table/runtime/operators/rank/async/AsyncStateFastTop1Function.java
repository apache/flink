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
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.operators.asyncprocessing.AsyncStateProcessingOperator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.rank.AppendOnlyTopNFunction;
import org.apache.flink.table.runtime.operators.rank.FastTop1Function;
import org.apache.flink.table.runtime.operators.rank.RankRange;
import org.apache.flink.table.runtime.operators.rank.RankType;
import org.apache.flink.table.runtime.operators.rank.UpdatableTopNFunction;
import org.apache.flink.table.runtime.operators.rank.utils.FastTop1Helper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

/**
 * A more concise implementation for {@link AppendOnlyTopNFunction} and {@link
 * UpdatableTopNFunction} when only Top-1 is desired. This function can handle updating stream
 * because the RankProcessStrategy is inferred as UpdateFastStrategy, i.e., 1) the upsert key of
 * input steam contains partition key; 2) the sort field is updated monotonely under the upsert key.
 *
 * <p>Different with {@link FastTop1Function}, this function is used with async state api.
 */
public class AsyncStateFastTop1Function extends AbstractAsyncStateTopNFunction
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<RowData> inputRowSer;
    private final long cacheSize;

    // a value state stores the latest record
    private transient ValueState<RowData> dataState;

    private transient AsyncStateFastTop1Helper helper;

    public AsyncStateFastTop1Function(
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

        this.inputRowSer = inputRowType.createSerializer(new SerializerConfigImpl());
        this.cacheSize = cacheSize;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        ValueStateDescriptor<RowData> valueStateDescriptor =
                new ValueStateDescriptor<>("Top1-Rank-State", inputRowType);
        if (ttlConfig.isEnabled()) {
            valueStateDescriptor.enableTimeToLive(ttlConfig);
        }
        dataState =
                ((StreamingRuntimeContext) getRuntimeContext()).getValueState(valueStateDescriptor);

        helper = new AsyncStateFastTop1Helper();

        helper.registerMetric();
    }

    @Override
    public void processElement(
            RowData input,
            KeyedProcessFunction<RowData, RowData, RowData>.Context ctx,
            Collector<RowData> out)
            throws Exception {
        helper.accRequestCount();

        // load state under current key if necessary
        RowData currentKey = (RowData) keyContext.getCurrentKey();

        RowData prevRowFromCache = helper.getPrevRowFromCache(currentKey);
        StateFuture<RowData> prevRowFuture;
        if (prevRowFromCache == null) {
            prevRowFuture = dataState.asyncValue();
        } else {
            helper.accHitCount();
            prevRowFuture = StateFutureUtils.completedFuture(prevRowFromCache);
        }

        prevRowFuture.thenAccept(
                prevRow -> {
                    // first row under current key.
                    if (prevRow == null) {
                        helper.processAsFirstRow(input, currentKey, out);
                    } else {
                        helper.processWithPrevRow(input, currentKey, prevRow, out);
                    }
                });
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        helper.flushAllCacheToState();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    private class AsyncStateFastTop1Helper extends FastTop1Helper {
        public AsyncStateFastTop1Helper() {
            super(
                    AsyncStateFastTop1Function.this,
                    inputRowSer,
                    cacheSize,
                    AsyncStateFastTop1Function.this.getDefaultTopNSize());
        }

        @Override
        public void flushBufferToState(RowData currentKey, RowData value) throws Exception {
            ((AsyncStateProcessingOperator) keyContext)
                    .asyncProcessWithKey(
                            currentKey,
                            () -> {
                                // no need to wait this async request to end
                                AsyncStateFastTop1Function.this.dataState.asyncUpdate(value);
                            });
        }
    }
}

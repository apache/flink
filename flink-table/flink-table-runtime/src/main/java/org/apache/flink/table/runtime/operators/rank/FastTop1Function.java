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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
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
import org.apache.flink.table.runtime.operators.rank.utils.FastTop1Helper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

/**
 * A more concise implementation for {@link AppendOnlyTopNFunction} and {@link
 * UpdatableTopNFunction} when only Top-1 is desired. This function can handle updating stream
 * because the RankProcessStrategy is inferred as UpdateFastStrategy, i.e., 1) the upsert key of
 * input stream contains partition key; 2) the sort field is updated monotonically under the upsert
 * key (See more at {@code FlinkRelMdModifiedMonotonicity}).
 */
public class FastTop1Function extends AbstractSyncStateTopNFunction
        implements CheckpointedFunction {

    private static final long serialVersionUID = 1L;

    private final TypeSerializer<RowData> inputRowSer;
    private final long cacheSize;

    // a value state stores the latest record
    private transient ValueState<RowData> dataState;

    private transient FastTop1Helper helper;

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
        dataState = getRuntimeContext().getState(valueStateDescriptor);

        helper = new SyncStateFastTop1Helper();

        helper.registerMetric();
    }

    @Override
    public void processElement(RowData input, Context ctx, Collector<RowData> out)
            throws Exception {
        helper.accRequestCount();

        // load state under current key if necessary
        RowData currentKey = (RowData) keyContext.getCurrentKey();
        RowData prevRow = helper.getPrevRowFromCache(currentKey);
        if (prevRow == null) {
            prevRow = dataState.value();
        } else {
            helper.accHitCount();
        }

        // first row under current key.
        if (prevRow == null) {
            helper.processAsFirstRow(input, currentKey, out);
        } else {
            helper.processWithPrevRow(input, currentKey, prevRow, out);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        helper.flushAllCacheToState();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // nothing to do
    }

    private class SyncStateFastTop1Helper extends FastTop1Helper {

        public SyncStateFastTop1Helper() {
            super(
                    FastTop1Function.this,
                    inputRowSer,
                    cacheSize,
                    FastTop1Function.this.getDefaultTopNSize());
        }

        @Override
        public void flushBufferToState(RowData currentKey, RowData value) throws Exception {
            keyContext.setCurrentKey(currentKey);
            FastTop1Function.this.dataState.update(value);
        }
    }
}

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

package org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.utils.OuterRecord;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.flink.table.runtime.util.StateConfigUtil.createTtlConfig;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utility to create a {@link JoinRecordAsyncStateView} depends on {@link JoinInputSideSpec}. */
public final class JoinRecordAsyncStateViews {

    /** Creates a {@link JoinRecordAsyncStateView} depends on {@link JoinInputSideSpec}. */
    public static JoinRecordAsyncStateView create(
            StreamingRuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new JoinKeyContainsUniqueKey(ctx, stateName, recordType, ttlConfig);
            } else {
                return new InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new InputSideHasNoUniqueKey(ctx, stateName, recordType, ttlConfig);
        }
    }

    // ------------------------------------------------------------------------------------

    private static final class JoinKeyContainsUniqueKey implements JoinRecordAsyncStateView {

        private final ValueState<RowData> recordState;
        private final List<OuterRecord> reusedList;

        private JoinKeyContainsUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            ValueStateDescriptor<RowData> recordStateDesc =
                    new ValueStateDescriptor<>(stateName, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getValueState(recordStateDesc);
            // the result records always not more than 1
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) {
            return recordState.asyncUpdate(record);
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) {
            return recordState.asyncClear();
        }

        @Override
        public StateFuture<List<OuterRecord>> findMatchedRecords(
                Function<RowData, Boolean> condition) {
            return recordState
                    .asyncValue()
                    .thenApply(
                            v -> {
                                reusedList.clear();
                                if (v != null) {
                                    if (condition.apply(v)) {
                                        reusedList.add(new OuterRecord(v));
                                    }
                                }
                                return reusedList;
                            });
        }
    }

    private static final class InputSideHasUniqueKey implements JoinRecordAsyncStateView {

        // stores record in the mapping <UK, Record>
        private final MapState<RowData, RowData> recordState;
        private final KeySelector<RowData, RowData> uniqueKeySelector;

        private InputSideHasUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                InternalTypeInfo<RowData> uniqueKeyType,
                KeySelector<RowData, RowData> uniqueKeySelector,
                StateTtlConfig ttlConfig) {
            checkNotNull(uniqueKeyType);
            checkNotNull(uniqueKeySelector);
            MapStateDescriptor<RowData, RowData> recordStateDesc =
                    new MapStateDescriptor<>(stateName, uniqueKeyType, recordType);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) {
            return StateFutureUtils.completedVoidFuture()
                    .thenCompose(
                            VOID -> {
                                RowData uniqueKey = uniqueKeySelector.getKey(record);
                                return recordState.asyncPut(uniqueKey, record);
                            });
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) {
            return StateFutureUtils.completedVoidFuture()
                    .thenCompose(
                            VOID -> {
                                RowData uniqueKey = uniqueKeySelector.getKey(record);
                                return recordState.asyncRemove(uniqueKey);
                            });
        }

        @Override
        public StateFuture<List<OuterRecord>> findMatchedRecords(
                Function<RowData, Boolean> condition) {
            List<OuterRecord> matchedRecords = new ArrayList<>();
            return recordState
                    .asyncValues()
                    .thenCompose(
                            it ->
                                    it.onNext(
                                            v -> {
                                                if (condition.apply(v)) {
                                                    matchedRecords.add(new OuterRecord(v));
                                                }
                                            }))
                    .thenApply(VOID -> matchedRecords);
        }
    }

    private static final class InputSideHasNoUniqueKey implements JoinRecordAsyncStateView {

        private final MapState<RowData, Integer> recordState;

        private InputSideHasNoUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            MapStateDescriptor<RowData, Integer> recordStateDesc =
                    new MapStateDescriptor<>(stateName, recordType, Types.INT);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record) {
            return recordState
                    .asyncGet(record)
                    .thenApply(
                            cnt -> {
                                if (cnt != null) {
                                    return cnt + 1;
                                } else {
                                    return 1;
                                }
                            })
                    .thenCompose(updateCnt -> recordState.asyncPut(record, updateCnt));
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) {
            return recordState
                    .asyncGet(record)
                    .thenCompose(
                            cnt -> {
                                if (cnt != null) {
                                    if (cnt > 1) {
                                        return recordState.asyncPut(record, cnt - 1);
                                    } else {
                                        return recordState.asyncRemove(record);
                                    }
                                }
                                // ignore cnt == null, which means state may be expired
                                return StateFutureUtils.completedVoidFuture();
                            });
        }

        @Override
        public StateFuture<List<OuterRecord>> findMatchedRecords(
                Function<RowData, Boolean> condition) {
            List<OuterRecord> matchedRecords = new ArrayList<>();
            return recordState
                    .asyncEntries()
                    .thenCompose(
                            it ->
                                    it.onNext(
                                            entry -> {
                                                RowData record = entry.getKey();
                                                int cnt = entry.getValue();
                                                if (condition.apply(record)) {
                                                    for (int i = 0; i < cnt; i++) {
                                                        matchedRecords.add(new OuterRecord(record));
                                                    }
                                                }
                                            }))
                    .thenApply(VOID -> matchedRecords);
        }
    }
}

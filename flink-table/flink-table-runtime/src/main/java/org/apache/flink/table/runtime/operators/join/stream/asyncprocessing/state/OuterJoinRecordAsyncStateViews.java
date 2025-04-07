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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
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

/**
 * Utility to create a {@link OuterJoinRecordAsyncStateViews} depends on {@link JoinInputSideSpec}.
 */
public final class OuterJoinRecordAsyncStateViews {

    /** Creates a {@link OuterJoinRecordAsyncStateView} depends on {@link JoinInputSideSpec}. */
    public static OuterJoinRecordAsyncStateView create(
            StreamingRuntimeContext ctx,
            String stateName,
            JoinInputSideSpec inputSideSpec,
            InternalTypeInfo<RowData> recordType,
            long retentionTime) {
        StateTtlConfig ttlConfig = createTtlConfig(retentionTime);
        if (inputSideSpec.hasUniqueKey()) {
            if (inputSideSpec.joinKeyContainsUniqueKey()) {
                return new OuterJoinRecordAsyncStateViews.JoinKeyContainsUniqueKey(
                        ctx, stateName, recordType, ttlConfig);
            } else {
                return new OuterJoinRecordAsyncStateViews.InputSideHasUniqueKey(
                        ctx,
                        stateName,
                        recordType,
                        inputSideSpec.getUniqueKeyType(),
                        inputSideSpec.getUniqueKeySelector(),
                        ttlConfig);
            }
        } else {
            return new OuterJoinRecordAsyncStateViews.InputSideHasNoUniqueKey(
                    ctx, stateName, recordType, ttlConfig);
        }
    }

    // ------------------------------------------------------------------------------------------

    private static final class JoinKeyContainsUniqueKey implements OuterJoinRecordAsyncStateView {

        private final ValueState<Tuple2<RowData, Integer>> recordState;
        private final List<OuterRecord> reusedList;

        private JoinKeyContainsUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(recordType, Types.INT);
            ValueStateDescriptor<Tuple2<RowData, Integer>> recordStateDesc =
                    new ValueStateDescriptor<>(stateName, valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getValueState(recordStateDesc);
            // the result records always not more than 1
            this.reusedList = new ArrayList<>(1);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) {
            return recordState.asyncUpdate(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations) {
            return recordState.asyncUpdate(Tuple2.of(record, numOfAssociations));
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) {
            return recordState.asyncClear();
        }

        @Override
        public StateFuture<List<OuterRecord>> findMatchedRecordsAndNumOfAssociations(
                Function<RowData, Boolean> condition) {
            return recordState
                    .asyncValue()
                    .thenCompose(
                            tuple -> {
                                reusedList.clear();
                                if (tuple != null) {
                                    if (condition.apply(tuple.f0)) {
                                        reusedList.add(new OuterRecord(tuple.f0, tuple.f1));
                                    }
                                }
                                return StateFutureUtils.completedFuture(reusedList);
                            });
        }
    }

    private static final class InputSideHasUniqueKey implements OuterJoinRecordAsyncStateView {

        // stores record in the mapping <UK, <Record, associated-num>>
        private final MapState<RowData, Tuple2<RowData, Integer>> recordState;
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
            TupleTypeInfo<Tuple2<RowData, Integer>> valueTypeInfo =
                    new TupleTypeInfo<>(recordType, Types.INT);
            MapStateDescriptor<RowData, Tuple2<RowData, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, uniqueKeyType, valueTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
            this.uniqueKeySelector = uniqueKeySelector;
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) {
            return StateFutureUtils.completedVoidFuture()
                    .thenCompose(
                            VOID -> {
                                RowData uniqueKey = uniqueKeySelector.getKey(record);
                                return recordState.asyncPut(
                                        uniqueKey, Tuple2.of(record, numOfAssociations));
                            });
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations) {
            return StateFutureUtils.completedVoidFuture()
                    .thenCompose(
                            VOID -> {
                                RowData uniqueKey = uniqueKeySelector.getKey(record);
                                return recordState.asyncPut(
                                        uniqueKey, Tuple2.of(record, numOfAssociations));
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
        public StateFuture<List<OuterRecord>> findMatchedRecordsAndNumOfAssociations(
                Function<RowData, Boolean> condition) {
            List<OuterRecord> matchedRecords = new ArrayList<>();
            return recordState
                    .asyncValues()
                    .thenCompose(
                            it ->
                                    it.onNext(
                                            v -> {
                                                if (condition.apply(v.f0)) {
                                                    matchedRecords.add(new OuterRecord(v.f0, v.f1));
                                                }
                                            }))
                    .thenApply(VOID -> matchedRecords);
        }
    }

    private static final class InputSideHasNoUniqueKey implements OuterJoinRecordAsyncStateView {

        // stores record in the mapping <Record, <appear-times, associated-num>>
        private final MapState<RowData, Tuple2<Integer, Integer>> recordState;

        private InputSideHasNoUniqueKey(
                StreamingRuntimeContext ctx,
                String stateName,
                InternalTypeInfo<RowData> recordType,
                StateTtlConfig ttlConfig) {
            TupleTypeInfo<Tuple2<Integer, Integer>> tupleTypeInfo =
                    new TupleTypeInfo<>(Types.INT, Types.INT);
            MapStateDescriptor<RowData, Tuple2<Integer, Integer>> recordStateDesc =
                    new MapStateDescriptor<>(stateName, recordType, tupleTypeInfo);
            if (ttlConfig.isEnabled()) {
                recordStateDesc.enableTimeToLive(ttlConfig);
            }
            this.recordState = ctx.getMapState(recordStateDesc);
        }

        @Override
        public StateFuture<Void> addRecord(RowData record, int numOfAssociations) {
            return recordState
                    .asyncGet(record)
                    .thenApply(
                            tuple -> {
                                if (tuple != null) {
                                    tuple.f0 = tuple.f0 + 1;
                                    tuple.f1 = numOfAssociations;
                                    return tuple;
                                } else {
                                    return Tuple2.of(1, numOfAssociations);
                                }
                            })
                    .thenCompose(updatedTuple -> recordState.asyncPut(record, updatedTuple));
        }

        @Override
        public StateFuture<Void> updateNumOfAssociations(RowData record, int numOfAssociations) {
            return recordState
                    .asyncGet(record)
                    .thenApply(
                            tuple -> {
                                if (tuple != null) {
                                    tuple.f1 = numOfAssociations;
                                    return tuple;
                                } else {
                                    // compatible for state ttl
                                    return Tuple2.of(1, numOfAssociations);
                                }
                            })
                    .thenCompose(updatedTuple -> recordState.asyncPut(record, updatedTuple));
        }

        @Override
        public StateFuture<Void> retractRecord(RowData record) {
            return recordState
                    .asyncGet(record)
                    .thenCompose(
                            tuple -> {
                                if (tuple != null) {
                                    if (tuple.f0 > 1) {
                                        tuple.f0 = tuple.f0 - 1;
                                        return recordState.asyncPut(record, tuple);
                                    } else {
                                        return recordState.asyncRemove(record);
                                    }
                                }
                                return StateFutureUtils.completedVoidFuture();
                            });
        }

        @Override
        public StateFuture<List<OuterRecord>> findMatchedRecordsAndNumOfAssociations(
                Function<RowData, Boolean> condition) {
            List<OuterRecord> matchedRecords = new ArrayList<>();
            return recordState
                    .asyncEntries()
                    .thenCompose(
                            it ->
                                    it.onNext(
                                            entry -> {
                                                RowData record = entry.getKey();
                                                int appearTimes = entry.getValue().f0;
                                                int associatedNum = entry.getValue().f1;

                                                if (condition.apply(record)) {
                                                    for (int i = 0; i < appearTimes; i++) {
                                                        matchedRecords.add(
                                                                new OuterRecord(
                                                                        record, associatedNum));
                                                    }
                                                }
                                            }))
                    .thenApply(VOID -> matchedRecords);
        }
    }
}

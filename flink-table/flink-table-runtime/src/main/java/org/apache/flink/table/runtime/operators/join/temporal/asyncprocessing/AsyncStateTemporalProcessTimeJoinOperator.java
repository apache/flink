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

package org.apache.flink.table.runtime.operators.join.temporal.asyncprocessing;

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.generated.JoinCondition;
import org.apache.flink.table.runtime.operators.join.temporal.utils.TemporalProcessTimeJoinHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

/**
 * The operator to temporal join a stream on processing time in async state.
 *
 * <p>For temporal TableFunction join (LATERAL TemporalTableFunction(o.proctime)) and temporal table
 * join (FOR SYSTEM_TIME AS OF), they can reuse same processing-time operator implementation, the
 * differences between them are: (1) The temporal TableFunction join only supports single column in
 * primary key but temporal table join supports arbitrary columns in primary key. (2) The temporal
 * TableFunction join only supports inner join, temporal table join supports both inner join and
 * left outer join.
 */
public class AsyncStateTemporalProcessTimeJoinOperator
        extends BaseTwoInputAsyncStateStreamOperatorWithStateRetention {

    private static final long serialVersionUID = 1L;

    private final boolean isLeftOuterJoin;
    private final InternalTypeInfo<RowData> rightType;
    private final GeneratedJoinCondition generatedJoinCondition;

    private transient ValueState<RowData> rightState;
    private transient JoinCondition joinCondition;

    private transient JoinedRowData outRow;
    private transient GenericRowData rightNullRow;
    private transient TimestampedCollector<RowData> collector;

    private transient AsyncStateTemporalProcessTimeJoinHelper temporalProcessTimeJoinHelper;

    public AsyncStateTemporalProcessTimeJoinOperator(
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            long minRetentionTime,
            long maxRetentionTime,
            boolean isLeftOuterJoin) {
        super(minRetentionTime, maxRetentionTime);
        this.rightType = rightType;
        this.generatedJoinCondition = generatedJoinCondition;
        this.isLeftOuterJoin = isLeftOuterJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.joinCondition =
                generatedJoinCondition.newInstance(getRuntimeContext().getUserCodeClassLoader());
        FunctionUtils.setFunctionRuntimeContext(joinCondition, getRuntimeContext());
        FunctionUtils.openFunction(joinCondition, DefaultOpenContext.INSTANCE);

        ValueStateDescriptor<RowData> rightStateDesc =
                new ValueStateDescriptor<>("right", rightType);
        this.rightState = getRuntimeContext().getValueState(rightStateDesc);
        this.collector = new TimestampedCollector<>(output);
        this.outRow = new JoinedRowData();
        this.rightNullRow = new GenericRowData(rightType.toRowSize());
        // consider watermark from left stream only.
        super.processWatermark2(Watermark.MAX_WATERMARK);
        this.temporalProcessTimeJoinHelper = new AsyncStateTemporalProcessTimeJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData leftSideRow = element.getValue();
        // RowData rightSideRow = rightState.value();
        StateFuture<RowData> rightSideRowFuture = rightState.asyncValue();

        rightSideRowFuture.thenAccept(
                rightSideRow -> {
                    temporalProcessTimeJoinHelper.processElement1(leftSideRow, rightSideRow);
                    if (rightSideRow != null) {
                        // register a cleanup timer only if the rightSideRow is not null
                        registerProcessingCleanupTimer();
                    }
                });
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        if (RowDataUtil.isAccumulateMsg(element.getValue())) {
            StateFuture<Void> updateFuture = rightState.asyncUpdate(element.getValue());
            updateFuture.thenAccept(VOID -> registerProcessingCleanupTimer());
        } else {
            StateFuture<Void> clearFuture = rightState.asyncClear();
            clearFuture.thenAccept(VOID -> cleanupLastTimer());
        }
    }

    @Override
    public void close() throws Exception {
        FunctionUtils.closeFunction(joinCondition);
        super.close();
    }

    /**
     * The method to be called when a cleanup timer fires.
     *
     * @param time The timestamp of the fired timer.
     */
    @Override
    public StateFuture<Void> cleanupState(long time) {
        return rightState.asyncClear();
    }

    /** Invoked when an event-time timer fires. */
    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {}

    private class AsyncStateTemporalProcessTimeJoinHelper extends TemporalProcessTimeJoinHelper {
        public AsyncStateTemporalProcessTimeJoinHelper() {
            super(isLeftOuterJoin, joinCondition, outRow, rightNullRow, collector);
        }
    }
}

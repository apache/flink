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

package org.apache.flink.table.runtime.operators.join.stream.asyncprocessing;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.JoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.JoinRecordAsyncStateViews;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.OuterJoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.OuterJoinRecordAsyncStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.AssociatedRecords;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.utils.SemiAntiJoinHelper;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/** Streaming unbounded Join operator which supports SEMI/ANTI JOIN. */
public class AsyncStateStreamingSemiAntiJoinOperator
        extends AbstractAsyncStateStreamingJoinOperator {

    private static final long serialVersionUID = 1L;

    // true if it is anti join, otherwise is semi join
    private final boolean isAntiJoin;

    // left join state
    private transient OuterJoinRecordAsyncStateView leftRecordAsyncStateView;
    // right join state
    private transient JoinRecordAsyncStateView rightRecordAsyncStateView;

    private transient AsyncStateSemiAntiJoinHelper semiAntiJoinHelper;

    public AsyncStateStreamingSemiAntiJoinOperator(
            boolean isAntiJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTIme) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTIme);
        this.isAntiJoin = isAntiJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.leftRecordAsyncStateView =
                OuterJoinRecordAsyncStateViews.create(
                        getRuntimeContext(),
                        LEFT_RECORDS_STATE_NAME,
                        leftInputSideSpec,
                        leftType,
                        leftStateRetentionTime);

        this.rightRecordAsyncStateView =
                JoinRecordAsyncStateViews.create(
                        getRuntimeContext(),
                        RIGHT_RECORDS_STATE_NAME,
                        rightInputSideSpec,
                        rightType,
                        rightStateRetentionTime);

        this.semiAntiJoinHelper = new AsyncStateSemiAntiJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        StateFuture<AssociatedRecords> associatedRecordsFuture =
                AssociatedRecords.fromAsyncStateView(
                        input, true, rightRecordAsyncStateView, joinCondition);
        associatedRecordsFuture.thenAccept(
                associatedRecords -> {
                    semiAntiJoinHelper.processLeftJoin(
                            associatedRecords, input, leftRecordAsyncStateView);
                });
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        StateFuture<AssociatedRecords> associatedRecordsFuture =
                AssociatedRecords.fromAsyncStateView(
                        input, false, leftRecordAsyncStateView, joinCondition);
        associatedRecordsFuture.thenAccept(
                associatedRecords -> {
                    semiAntiJoinHelper.processRightJoin(
                            isAccumulateMsg,
                            leftRecordAsyncStateView,
                            rightRecordAsyncStateView,
                            input,
                            associatedRecords,
                            inputRowKind);
                });
    }

    private class AsyncStateSemiAntiJoinHelper
            extends SemiAntiJoinHelper<JoinRecordAsyncStateView, OuterJoinRecordAsyncStateView> {

        public AsyncStateSemiAntiJoinHelper() {
            super(isAntiJoin, collector);
        }

        @Override
        public void addRecord(JoinRecordAsyncStateView stateView, RowData record) throws Exception {
            // no need to wait for the future
            stateView.addRecord(record);
        }

        @Override
        public void retractRecord(JoinRecordAsyncStateView stateView, RowData record)
                throws Exception {
            // no need to wait for the future
            stateView.retractRecord(record);
        }

        @Override
        public void addRecordInOuterSide(
                OuterJoinRecordAsyncStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            // no need to wait for the future
            stateView.addRecord(record, numOfAssociations);
        }

        @Override
        public void retractRecordInOuterSide(
                OuterJoinRecordAsyncStateView stateView, RowData record) throws Exception {
            // no need to wait for the future
            stateView.retractRecord(record);
        }

        @Override
        public void updateNumOfAssociationsInOuterSide(
                OuterJoinRecordAsyncStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            // no need to wait for the future
            stateView.updateNumOfAssociations(record, numOfAssociations);
        }
    }
}

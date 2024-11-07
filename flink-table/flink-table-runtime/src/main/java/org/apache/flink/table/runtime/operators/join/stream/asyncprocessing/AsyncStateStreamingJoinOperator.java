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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.JoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.JoinRecordAsyncStateViews;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.OuterJoinRecordAsyncStateView;
import org.apache.flink.table.runtime.operators.join.stream.asyncprocessing.state.OuterJoinRecordAsyncStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.AssociatedRecords;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinHelper;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/**
 * Streaming unbounded Join operator based on async state api, which supports INNER/LEFT/RIGHT/FULL
 * JOIN.
 */
public class AsyncStateStreamingJoinOperator extends AbstractAsyncStateStreamingJoinOperator {

    private static final long serialVersionUID = 1L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    private final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    private final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    private transient JoinRecordAsyncStateView leftRecordAsyncStateView;
    // right join state
    private transient JoinRecordAsyncStateView rightRecordAsyncStateView;

    private transient AsyncStateJoinHelper joinHelper;

    public AsyncStateStreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTime);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());

        // initialize states
        if (leftIsOuter) {
            this.leftRecordAsyncStateView =
                    OuterJoinRecordAsyncStateViews.create(
                            getRuntimeContext(),
                            LEFT_RECORDS_STATE_NAME,
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        } else {
            this.leftRecordAsyncStateView =
                    JoinRecordAsyncStateViews.create(
                            getRuntimeContext(),
                            LEFT_RECORDS_STATE_NAME,
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordAsyncStateView =
                    OuterJoinRecordAsyncStateViews.create(
                            getRuntimeContext(),
                            RIGHT_RECORDS_STATE_NAME,
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        } else {
            this.rightRecordAsyncStateView =
                    JoinRecordAsyncStateViews.create(
                            getRuntimeContext(),
                            RIGHT_RECORDS_STATE_NAME,
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        }

        this.joinHelper = new AsyncStateJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        doProcessElement(
                element.getValue(), leftRecordAsyncStateView, rightRecordAsyncStateView, true);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        doProcessElement(
                element.getValue(), rightRecordAsyncStateView, leftRecordAsyncStateView, false);
    }

    private void doProcessElement(
            RowData input,
            JoinRecordAsyncStateView inputSideAsyncStateView,
            JoinRecordAsyncStateView otherSideAsyncStateView,
            boolean inputIsLeft)
            throws Exception {
        RowKind originalRowKind = input.getRowKind();
        // erase RowKind for later state updating
        input.setRowKind(RowKind.INSERT);

        StateFuture<AssociatedRecords> associatedRecordsFuture =
                AssociatedRecords.fromAsyncStateView(
                        input, inputIsLeft, otherSideAsyncStateView, joinCondition);
        // set back RowKind
        input.setRowKind(originalRowKind);
        associatedRecordsFuture.thenAccept(
                associatedRecords ->
                        joinHelper.processJoin(
                                input,
                                inputSideAsyncStateView,
                                otherSideAsyncStateView,
                                inputIsLeft,
                                associatedRecords,
                                false));
    }

    private class AsyncStateJoinHelper
            extends JoinHelper<JoinRecordAsyncStateView, OuterJoinRecordAsyncStateView> {

        public AsyncStateJoinHelper() {
            super(leftIsOuter, rightIsOuter, outRow, leftNullRow, rightNullRow, collector);
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
        public void updateNumOfAssociationsInOuterSide(
                OuterJoinRecordAsyncStateView outerJoinRecordAsyncStateView,
                RowData record,
                int numOfAssociations)
                throws Exception {
            // no need to wait for the future
            outerJoinRecordAsyncStateView.updateNumOfAssociations(record, numOfAssociations);
        }
    }
}

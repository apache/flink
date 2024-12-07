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

package org.apache.flink.table.runtime.operators.join.stream;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.AssociatedRecords;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinHelper;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

/** Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    protected final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    protected final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    protected transient JoinRecordStateView leftRecordStateView;
    // right join state
    protected transient JoinRecordStateView rightRecordStateView;

    private transient SyncStateJoinHelper joinHelper;

    public StreamingJoinOperator(
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
            this.leftRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        } else {
            this.leftRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            leftStateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        } else {
            this.rightRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            rightStateRetentionTime);
        }
        this.joinHelper = new SyncStateJoinHelper();
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true, false);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false, false);
    }

    protected void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft,
            boolean isSuppress)
            throws Exception {
        RowKind originalRowKind = input.getRowKind();
        // erase RowKind for later state updating
        input.setRowKind(RowKind.INSERT);

        AssociatedRecords associatedRecords =
                AssociatedRecords.fromSyncStateView(
                        input, inputIsLeft, otherSideStateView, joinCondition);
        // set back RowKind
        input.setRowKind(originalRowKind);
        joinHelper.processJoin(
                input,
                inputSideStateView,
                otherSideStateView,
                inputIsLeft,
                associatedRecords,
                isSuppress);
    }

    private class SyncStateJoinHelper
            extends JoinHelper<JoinRecordStateView, OuterJoinRecordStateView> {
        public SyncStateJoinHelper() {
            super(leftIsOuter, rightIsOuter, outRow, leftNullRow, rightNullRow, collector);
        }

        @Override
        public void addRecord(JoinRecordStateView stateView, RowData record) throws Exception {
            stateView.addRecord(record);
        }

        @Override
        public void retractRecord(JoinRecordStateView stateView, RowData record) throws Exception {
            stateView.retractRecord(record);
        }

        @Override
        public void addRecordInOuterSide(
                OuterJoinRecordStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            stateView.addRecord(record, numOfAssociations);
        }

        @Override
        public void updateNumOfAssociationsInOuterSide(
                OuterJoinRecordStateView stateView, RowData record, int numOfAssociations)
                throws Exception {
            stateView.updateNumOfAssociations(record, numOfAssociations);
        }
    }
}

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
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJoinOperator.class);

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    private final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    private final boolean rightIsOuter;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    private transient JoinRecordStateView leftRecordStateView;
    // right join state
    private transient JoinRecordStateView rightRecordStateView;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long stateRetentionTime) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                stateRetentionTime);
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
                            stateRetentionTime);
        } else {
            this.leftRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "left-records",
                            leftInputSideSpec,
                            leftType,
                            stateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordStateView =
                    OuterJoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            stateRetentionTime);
        } else {
            this.rightRecordStateView =
                    JoinRecordStateViews.create(
                            getRuntimeContext(),
                            "right-records",
                            rightInputSideSpec,
                            rightType,
                            stateRetentionTime);
        }
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method. The logic of this
     * method is too complex, so we provide the pseudo code to help understand the logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind in all join kinds and,
     * additionally, for outer join explicitly set -U & +U to retract / emit rows with null other
     * side. See {@code FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if there are matched rows on the other side
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, send -U[null+other]s and +U[record+other]s
     * |  |  |  if the matched num in the matched rows > 0, send +I/+U[record+other]s (using input RowKind)
     * |  |  |  otherState.update(other, old + 1)
     * |  |  endif
     * |  |  if other side is not outer, send +I/+U[record+other]s (using input RowKind)
     * |  endif
     * |  if there are no matched rows on the other side
     * |  |  if input side is outer, send +I/+U[record+null] (using input RowKind)
     * |  |  if input side is not outer, skip
     * |  endif
     * |  if input side is outer, state.add(record, other.size)
     * |  if input side is not outer, state.add(record)
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there are matched rows on the other side
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send -U[record+other]s and +U[null+other]s
     * |  |  |  if the matched num in the matched rows > 1, send -D/-U[record+other]s (using input RowKind)
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  |  if other side is not outer, send -D/-U[record+other]s (using input RowKind)
     * |  endif
     * |  if there are no matched rows on the other side
     * |  |  if input side is outer, send -D/-U[record+other] (using input RowKind)
     * |  |  if input side is not outer, skip
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     */
    private void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        AssociatedRecords associatedRecords =
                AssociatedRecords.of(input, inputIsLeft, otherSideStateView, joinCondition);
        if (isAccumulateMsg) { // record is accumulate
            if (!associatedRecords.isEmpty()) { // there are matched rows on the other side
                if (otherIsOuter) { // other side is outer
                    OuterJoinRecordStateView otherSideOuterStateView =
                            (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        RowData other = outerRecord.record;
                        // retract null row if there are no associations
                        if (outerRecord.numOfAssociations == 0) {
                            // send -U[null+other] and +U[record+other]
                            outRow.setRowKind(RowKind.UPDATE_BEFORE);
                            outputNullPadding(other, !inputIsLeft);
                            outRow.setRowKind(RowKind.UPDATE_AFTER);
                            output(input, other, inputIsLeft);
                        } else {
                            // send +I/+U[record+other] (using input RowKind)
                            outRow.setRowKind(inputRowKind);
                            output(input, other, inputIsLeft);
                        }
                        // otherState.update(other, old + 1)
                        otherSideOuterStateView.updateNumOfAssociations(
                                other, outerRecord.numOfAssociations + 1);
                    }
                } else {
                    // send +I/+U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
            } else { // there are no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send +I/+U[record+null] (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            }
            if (inputIsOuter) { // input side is outer
                OuterJoinRecordStateView inputSideOuterStateView =
                        (OuterJoinRecordStateView) inputSideStateView;
                // state.add(record, other.size)
                inputSideOuterStateView.addRecord(input, associatedRecords.size());
            } else { // input side not outer
                // state.add(record)
                inputSideStateView.addRecord(input);
            }
        } else { // input record is retract
            // state.retract(record)
            inputSideStateView.retractRecord(input);
            if (!associatedRecords.isEmpty()) { // there are matched rows on the other side
                if (otherIsOuter) {
                    OuterJoinRecordStateView otherSideOuterStateView =
                            (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 0) {
                            LOG.warn(
                                    "Stream join encountered {} record {} without preceding INSERT/UPDATE_AFTER "
                                            + "records by corresponding join key being stored in state. "
                                            + "Probably, there are data disorder in stream, or these records "
                                            + "was expired from state. This will lead to incorrect result.",
                                    inputRowKind,
                                    input);
                        }
                        RowData other = outerRecord.record;
                        // emit null row if there are only one association
                        if (outerRecord.numOfAssociations == 1) {
                            // send -U[record+other] and +U[null+other]
                            outRow.setRowKind(RowKind.UPDATE_BEFORE);
                            output(input, other, inputIsLeft);
                            outRow.setRowKind(RowKind.UPDATE_AFTER);
                            outputNullPadding(other, !inputIsLeft);
                        } else {
                            // send -D/-U[record+other] (using input RowKind)
                            outRow.setRowKind(inputRowKind);
                            output(input, other, inputIsLeft);
                        }
                        // otherState.update(other, old - 1)
                        otherSideOuterStateView.updateNumOfAssociations(
                                other, outerRecord.numOfAssociations - 1);
                    }
                } else {
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
            } else { // there are no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D/-U[record+other] (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            }
        }
    }

    // -------------------------------------------------------------------------------------

    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        collector.collect(outRow);
    }

    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}

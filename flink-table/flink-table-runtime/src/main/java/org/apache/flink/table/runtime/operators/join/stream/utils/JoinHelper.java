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

package org.apache.flink.table.runtime.operators.join.stream.utils;

import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.types.RowKind;

/** A helper to do the logic of streaming join. */
public abstract class JoinHelper<STATE_VIEW, OUTER_STATE_VIEW extends STATE_VIEW> {

    private final boolean leftIsOuter;
    private final boolean rightIsOuter;
    private final JoinedRowData outRow;
    private final RowData leftNullRow;
    private final RowData rightNullRow;

    private final TimestampedCollector<RowData> collector;

    public JoinHelper(
            boolean leftIsOuter,
            boolean rightIsOuter,
            JoinedRowData outRow,
            RowData leftNullRow,
            RowData rightNullRow,
            TimestampedCollector<RowData> collector) {
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
        this.outRow = outRow;
        this.leftNullRow = leftNullRow;
        this.rightNullRow = rightNullRow;
        this.collector = collector;
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
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D & +I for now for
     * simplification. See {@code
     * FlinkChangelogModeInferenceProgram.SatisfyModifyKindSetTraitVisitor}.
     *
     * <pre>
     * if input record is accumulate
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other]
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record)
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideAsyncStateView state of input side
     * @param otherSideAsyncStateView state of other side
     * @param inputIsLeft whether input side is left side
     * @param otherSideAssociatedRecords associated records in the state of the other side
     */
    public void processJoin(
            RowData input,
            STATE_VIEW inputSideAsyncStateView,
            STATE_VIEW otherSideAsyncStateView,
            boolean inputIsLeft,
            AssociatedRecords otherSideAssociatedRecords,
            boolean isSuppress)
            throws Exception {
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        if (isAccumulateMsg) { // record is accumulate
            if (inputIsOuter) { // input side is outer
                OUTER_STATE_VIEW inputSideOuterStateView =
                        (OUTER_STATE_VIEW) inputSideAsyncStateView;
                if (otherSideAssociatedRecords
                        .isEmpty()) { // there is no matched rows on the other side
                    // send +I[record+null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    addRecordInOuterSide(inputSideOuterStateView, input, 0);
                } else { // there are matched rows on the other side
                    if (otherIsOuter) { // other side is outer
                        OUTER_STATE_VIEW otherSideOuterStateView =
                                (OUTER_STATE_VIEW) otherSideAsyncStateView;
                        for (OuterRecord outerRecord :
                                otherSideAssociatedRecords.getOuterRecords()) {
                            RowData other = outerRecord.record;
                            // if the matched num in the matched rows == 0
                            if (outerRecord.numOfAssociations == 0 && !isSuppress) {
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0
                            // otherState.update(other, old + 1)
                            updateNumOfAssociationsInOuterSide(
                                    otherSideOuterStateView,
                                    other,
                                    outerRecord.numOfAssociations + 1);
                        }
                    }
                    // send +I[record+other]s
                    outRow.setRowKind(RowKind.INSERT);
                    for (RowData other : otherSideAssociatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                    // state.add(record, other.size)
                    addRecordInOuterSide(
                            inputSideOuterStateView, input, otherSideAssociatedRecords.size());
                }
            } else { // input side not outer
                // state.add(record)
                addRecord(inputSideAsyncStateView, input);
                if (!otherSideAssociatedRecords
                        .isEmpty()) { // if there are matched rows on the other side
                    if (otherIsOuter) { // if other side is outer
                        OUTER_STATE_VIEW otherSideOuterStateView =
                                (OUTER_STATE_VIEW) otherSideAsyncStateView;
                        for (OuterRecord outerRecord :
                                otherSideAssociatedRecords.getOuterRecords()) {
                            if (outerRecord.numOfAssociations == 0
                                    && !isSuppress) { // if the matched num in the matched rows == 0
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            updateNumOfAssociationsInOuterSide(
                                    otherSideOuterStateView,
                                    outerRecord.record,
                                    outerRecord.numOfAssociations + 1);
                        }
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    for (RowData other : otherSideAssociatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            // state.retract(record)
            if (!isSuppress) {
                retractRecord(inputSideAsyncStateView, input);
            }
            if (otherSideAssociatedRecords
                    .isEmpty()) { // there is no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            } else { // there are matched rows on the other side
                if (inputIsOuter) {
                    // send -D[record+other]s
                    outRow.setRowKind(RowKind.DELETE);
                } else {
                    // send -D/-U[record+other]s (using input RowKind)
                    outRow.setRowKind(inputRowKind);
                }
                for (RowData other : otherSideAssociatedRecords.getRecords()) {
                    output(input, other, inputIsLeft);
                }
                // if other side is outer
                if (otherIsOuter) {
                    OUTER_STATE_VIEW otherSideOuterStateView =
                            (OUTER_STATE_VIEW) otherSideAsyncStateView;
                    for (OuterRecord outerRecord : otherSideAssociatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 1 && !isSuppress) {
                            // send +I[null+other]
                            outRow.setRowKind(RowKind.INSERT);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // nothing else to do when number of associations > 1
                        // otherState.update(other, old - 1)
                        updateNumOfAssociationsInOuterSide(
                                otherSideOuterStateView,
                                outerRecord.record,
                                outerRecord.numOfAssociations - 1);
                    }
                }
            }
        }
    }

    // ---------- common methods for accessing the state ----------

    public abstract void addRecord(STATE_VIEW stateView, RowData record) throws Exception;

    public abstract void retractRecord(STATE_VIEW stateView, RowData record) throws Exception;

    // ---------- unique methods for accessing the state of the outer side ----------
    public abstract void addRecordInOuterSide(
            OUTER_STATE_VIEW stateView, RowData record, int numOfAssociations) throws Exception;

    public abstract void updateNumOfAssociationsInOuterSide(
            OUTER_STATE_VIEW stateView, RowData record, int numOfAssociations) throws Exception;

    // ----------------------------------------------------------

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

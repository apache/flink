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
import org.apache.flink.types.RowKind;

/** A helper to do the logic of streaming semi anti join. */
public abstract class SemiAntiJoinHelper<STATE_VIEW, OUTER_STATE_VIEW extends STATE_VIEW> {

    // true if it is anti join, otherwise is semi join
    private final boolean isAntiJoin;

    private final TimestampedCollector<RowData> collector;

    public SemiAntiJoinHelper(boolean isAntiJoin, TimestampedCollector<RowData> collector) {
        this.isAntiJoin = isAntiJoin;
        this.collector = collector;
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method.
     *
     * <pre>
     * if there is no matched rows on the other side
     *   if anti join, send input record
     * if there are matched rows on the other side
     *   if semi join, send input record
     * if the input record is accumulate, state.add(record, matched size)
     * if the input record is retract, state.retract(record)
     * </pre>
     */
    public void processLeftJoin(
            AssociatedRecords associatedRecords,
            RowData input,
            OUTER_STATE_VIEW leftRecordAsyncStateView)
            throws Exception {
        output(associatedRecords, input);
        if (RowDataUtil.isAccumulateMsg(input)) {
            // erase RowKind for state updating
            input.setRowKind(RowKind.INSERT);
            addRecordInOuterSide(leftRecordAsyncStateView, input, associatedRecords.size());
        } else { // input is retract
            // erase RowKind for state updating
            input.setRowKind(RowKind.INSERT);
            retractRecordInOuterSide(leftRecordAsyncStateView, input);
        }
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method.
     *
     * <p>Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE".
     *
     * <pre>
     * if input record is accumulate
     * | state.add(record)
     * | if there is no matched rows on the other side, skip
     * | if there are matched rows on the other side
     * | | if the matched num in the matched rows == 0
     * | |   if anti join, send -D[other]s
     * | |   if semi join, send +I/+U[other]s (using input RowKind)
     * | | if the matched num in the matched rows > 0, skip
     * | | otherState.update(other, old+1)
     * | endif
     * endif
     * if input record is retract
     * | state.retract(record)
     * | if there is no matched rows on the other side, skip
     * | if there are matched rows on the other side
     * | | if the matched num in the matched rows == 0, this should never happen!
     * | | if the matched num in the matched rows == 1
     * | |   if semi join, send -D/-U[other] (using input RowKind)
     * | |   if anti join, send +I[other]
     * | | if the matched num in the matched rows > 1, skip
     * | | otherState.update(other, old-1)
     * | endif
     * endif
     * </pre>
     */
    public void processRightJoin(
            boolean isAccumulateMsg,
            OUTER_STATE_VIEW leftRecordAsyncStateView,
            STATE_VIEW rightRecordAsyncStateView,
            RowData input,
            AssociatedRecords associatedRecords,
            RowKind inputRowKind)
            throws Exception {
        if (isAccumulateMsg) { // record is accumulate
            addRecord(rightRecordAsyncStateView, input);
            if (!associatedRecords.isEmpty()) {
                // there are matched rows on the other side
                for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                    RowData other = outerRecord.record;
                    if (outerRecord.numOfAssociations == 0) {
                        if (isAntiJoin) {
                            // send -D[other]
                            other.setRowKind(RowKind.DELETE);
                        } else {
                            // send +I/+U[other] (using input RowKind)
                            other.setRowKind(inputRowKind);
                        }
                        collector.collect(other);
                        // set header back to INSERT, because we will update the other row to state
                        other.setRowKind(RowKind.INSERT);
                    } // ignore when number > 0
                    updateNumOfAssociationsInOuterSide(
                            leftRecordAsyncStateView, other, outerRecord.numOfAssociations + 1);
                }
            } // ignore when associated number == 0
        } else { // retract input
            retractRecord(rightRecordAsyncStateView, input);
            if (!associatedRecords.isEmpty()) {
                // there are matched rows on the other side
                for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                    RowData other = outerRecord.record;
                    if (outerRecord.numOfAssociations == 1) {
                        if (!isAntiJoin) {
                            // send -D/-U[other] (using input RowKind)
                            other.setRowKind(inputRowKind);
                        } else {
                            // send +I[other]
                            other.setRowKind(RowKind.INSERT);
                        }
                        collector.collect(other);
                        // set RowKind back, because we will update the other row to state
                        other.setRowKind(RowKind.INSERT);
                    } // ignore when number > 0
                    updateNumOfAssociationsInOuterSide(
                            leftRecordAsyncStateView, other, outerRecord.numOfAssociations - 1);
                }
            } // ignore when associated number == 0
        }
    }

    // ---------- common methods for accessing the state ----------

    public abstract void addRecord(STATE_VIEW stateView, RowData record) throws Exception;

    public abstract void retractRecord(STATE_VIEW stateView, RowData record) throws Exception;

    // ---------- unique methods for accessing the state of the outer side ----------
    public abstract void addRecordInOuterSide(
            OUTER_STATE_VIEW stateView, RowData record, int numOfAssociations) throws Exception;

    public abstract void retractRecordInOuterSide(OUTER_STATE_VIEW stateView, RowData record)
            throws Exception;

    public abstract void updateNumOfAssociationsInOuterSide(
            OUTER_STATE_VIEW stateView, RowData record, int numOfAssociations) throws Exception;

    // ----------------------------------------------------------

    private void output(AssociatedRecords associatedRecords, RowData input) {
        if (associatedRecords.isEmpty()) {
            if (isAntiJoin) {
                collector.collect(input);
            }
        } else { // there are matched rows on the other side
            if (!isAntiJoin) {
                collector.collect(input);
            }
        }
    }
}

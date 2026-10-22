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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import javax.annotation.Nullable;

import java.util.Iterator;

/** Streaming unbounded Join operator which supports SEMI/ANTI JOIN. */
public class StreamingSemiAntiJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -3135772379944924519L;

    // true if it is anti join, otherwise is semi join
    private final boolean isAntiJoin;

    // left join state
    private transient OuterJoinRecordStateView leftRecordStateView;
    // right join state
    private transient JoinRecordStateView rightRecordStateView;

    public StreamingSemiAntiJoinOperator(
            boolean isAntiJoin,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            @Nullable RuntimeChangelogMode leftInputChangelogMode,
            @Nullable RuntimeChangelogMode rightInputChangelogMode,
            boolean hasNonEquiCondition,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTIme) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                leftInputChangelogMode,
                rightInputChangelogMode,
                hasNonEquiCondition,
                filterNullKeys,
                leftStateRetentionTime,
                rightStateRetentionTIme);
        this.isAntiJoin = isAntiJoin;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.leftRecordStateView =
                OuterJoinRecordStateViews.create(
                        getRuntimeContext(),
                        LEFT_RECORDS_STATE_NAME,
                        leftInputSideSpec,
                        leftType,
                        leftStateRetentionTime);

        this.rightRecordStateView =
                JoinRecordStateViews.create(
                        getRuntimeContext(),
                        RIGHT_RECORDS_STATE_NAME,
                        rightInputSideSpec,
                        rightType,
                        rightStateRetentionTime);
    }

    /**
     * Process an input element and output incremental joined records, retraction messages will be
     * sent in some scenarios.
     *
     * <p>Following is the pseudo code to describe the core logic of this method.
     *
     * <pre>
     * if record is a DELETE that may contain only the unique key and the condition is non-equi
     *   record = the stored record with the same unique key, if there is one
     * if record replaces a stored record (upsert) and the condition is non-equi
     *   if the replaced record is sent but the record is not, send -D[replaced]
     * if there is no matched rows on the other side
     *   if anti join, send input record
     * if there are matched rows on the other side
     *   if semi join, send input record
     * if the input record is accumulate, state.add(record, matched size), unless the join key can
     * never match
     * if the input record is retract, state.retract(record)
     * </pre>
     */
    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        final RowData record =
                isAccumulateMsg
                        ? input
                        : retractedRecord(input, inputRowKind, leftRecordStateView, true);
        Iterator<OuterRecord> associatedRecords =
                AbstractStreamingJoinOperator.iterator(
                        record, true, rightRecordStateView, joinCondition);
        final boolean recordIsSent = isAntiJoin != associatedRecords.hasNext();
        // count before the state of the other side is read again, which may reuse the iterable
        final int numOfAssociations = isAccumulateMsg ? Iterators.size(associatedRecords) : 0;
        if (isAccumulateMsg && !recordIsSent) {
            final RowData replacedRecord = replacedRecord(input, leftRecordStateView, true, false);
            if (replacedRecord != null && isSent(replacedRecord)) {
                // send -D[replaced]
                collect(replacedRecord, RowKind.DELETE);
            }
        }
        if (recordIsSent) {
            collect(record, inputRowKind);
        }
        if (isAccumulateMsg) {
            // do not store a record that can never match
            if (!joinCondition.currentKeyCannotMatch()) {
                leftRecordStateView.addRecord(input, numOfAssociations);
            }
        } else {
            leftRecordStateView.retractRecord(record);
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
     * | if record replaces a stored record (upsert) and the condition is non-equi
     * | | for each other that the replaced record matches but the record does not
     * | | | if the matched num in the matched rows == 1
     * | | |   if semi join, send -D[other]
     * | | |   if anti join, send +I[other]
     * | | | otherState.update(other, old-1)
     * | | endfor
     * | endif
     * | state.add(record), unless the join key can never match
     * | if there is no matched rows on the other side, skip
     * | if there are matched rows on the other side
     * | | if the matched num in the matched rows == 0
     * | |   if anti join, send -D[other]s
     * | |   if semi join, send +I/+U[other]s (using input RowKind)
     * | | if the matched num in the matched rows > 0, skip
     * | | if the replaced record did not match other, otherState.update(other, old+1)
     * | endif
     * endif
     * if input record is retract
     * | if record is a DELETE that may contain only the unique key and the condition is non-equi
     * | | record = the stored record with the same unique key, if there is one
     * | endif
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
    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData input = element.getValue();
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        if (isAccumulateMsg) { // record is accumulate
            final RowData replacedRecord = replacedRecord(input, rightRecordStateView, false, true);
            if (replacedRecord != null && hasNonEquiCondition) {
                removeLostMatches(input, replacedRecord);
            }
            // do not store a record that can never match
            if (!joinCondition.currentKeyCannotMatch()) {
                rightRecordStateView.addRecord(input);
            }
            Iterator<OuterRecord> associatedRecords =
                    AbstractStreamingJoinOperator.iterator(
                            input, false, leftRecordStateView, joinCondition);
            while (associatedRecords.hasNext()) {
                OuterRecord outerRecord = associatedRecords.next();
                RowData other = outerRecord.record;
                if (outerRecord.numOfAssociations == 0) {
                    // send -D[other] if anti join, +I/+U[other] (using input RowKind) if semi join
                    collect(other, isAntiJoin ? RowKind.DELETE : inputRowKind);
                } // ignore when number > 0
                if (isNewMatch(outerRecord, replacedRecord, false)) {
                    leftRecordStateView.updateNumOfAssociations(
                            other, outerRecord.numOfAssociations + 1);
                }
            }
        } else { // retract input
            final RowData retractedRecord =
                    retractedRecord(input, inputRowKind, rightRecordStateView, false);
            rightRecordStateView.retractRecord(retractedRecord);
            Iterator<OuterRecord> associatedRecords =
                    AbstractStreamingJoinOperator.iterator(
                            retractedRecord, false, leftRecordStateView, joinCondition);
            while (associatedRecords.hasNext()) {
                removeMatch(associatedRecords.next(), inputRowKind);
            }
        }
    }

    /** Removes the matches of the left records that only the replaced right record matched. */
    private void removeLostMatches(RowData record, RowData replacedRecord) throws Exception {
        final Iterator<OuterRecord> matchesOfReplacedRecord =
                AbstractStreamingJoinOperator.iterator(
                        replacedRecord, false, leftRecordStateView, joinCondition);
        while (matchesOfReplacedRecord.hasNext()) {
            final OuterRecord other = matchesOfReplacedRecord.next();
            if (!matches(record, other.record, false)) {
                removeMatch(other, RowKind.DELETE);
            }
        }
    }

    /**
     * Removes a match of the left record. If it was the last one, a semi join retracts the record
     * with the given RowKind and an anti join sends it.
     */
    private void removeMatch(OuterRecord other, RowKind retractionKind) throws Exception {
        if (other.numOfAssociations == 1) {
            // send -D/-U[other] if semi join, +I[other] if anti join
            collect(other.record, isAntiJoin ? RowKind.INSERT : retractionKind);
        } // ignore when number > 1
        leftRecordStateView.updateNumOfAssociations(other.record, other.numOfAssociations - 1);
    }

    /** Returns whether the left record is sent, i.e. it has matches in a semi join. */
    private boolean isSent(RowData left) throws Exception {
        return isAntiJoin
                != AbstractStreamingJoinOperator.iterator(
                                left, true, rightRecordStateView, joinCondition)
                        .hasNext();
    }

    /** Sends the left record with the given RowKind. */
    private void collect(RowData left, RowKind rowKind) {
        left.setRowKind(rowKind);
        collector.collect(left);
        // set RowKind back, because the record may be the one in state
        left.setRowKind(RowKind.INSERT);
    }
}

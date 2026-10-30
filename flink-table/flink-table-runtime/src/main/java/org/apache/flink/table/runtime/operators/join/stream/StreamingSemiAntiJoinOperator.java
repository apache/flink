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

import org.apache.flink.api.java.tuple.Tuple2;
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
     *   record = the stored record with the same unique key, if any
     * if record replaces a stored record (upsert) and the condition is non-equi
     *   if the replaced record is sent (by its matched num) but the record is not, send -D[replaced]
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

        if (isAccumulateMsg) {
            final int numOfAssociations =
                    Iterators.size(
                            AbstractStreamingJoinOperator.iterator(
                                    input, true, rightRecordStateView, joinCondition));
            if (isSent(numOfAssociations)) {
                collect(input, inputRowKind);
            } else if (needsReplacedRecord(true, false)) {
                final Tuple2<RowData, Integer> replaced =
                        leftRecordStateView.getRecordAndNumOfAssociations(input);
                if (replaced != null && isSent(replaced.f1)) {
                    // send -D[replaced]
                    collect(replaced.f0, RowKind.DELETE);
                }
            }
            // do not store a record that can never match
            if (!joinCondition.currentKeyCannotMatch()) {
                leftRecordStateView.addRecord(input, numOfAssociations);
            }
        } else {
            final RowData retractedRecord =
                    retractedRecord(input, inputRowKind, leftRecordStateView, true);
            final boolean hasMatches =
                    AbstractStreamingJoinOperator.iterator(
                                    retractedRecord, true, rightRecordStateView, joinCondition)
                            .hasNext();
            if (isAntiJoin != hasMatches) {
                collect(retractedRecord, inputRowKind);
            }
            leftRecordStateView.retractRecord(retractedRecord);
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
     * | state.add(record), unless the join key can never match
     * | for each other on the other side
     * | | if record matches other
     * | | | if the matched num of other <= 0
     * | | |   if anti join, send -D[other]
     * | | |   if semi join, send +I/+U[other] (using input RowKind)
     * | | |   otherState.update(other, 1)
     * | | | else if the record replaces a stored record (upsert) that did not match other
     * | | |   otherState.update(other, old+1)
     * | | else if the record replaces a stored record (upsert) that matched other
     * | | | remove the match of other
     * | endfor
     * endif
     * if input record is retract
     * | if record is a DELETE that may contain only the unique key and the condition is non-equi
     * | | record = the stored record with the same unique key, if any
     * | endif
     * | state.retract(record)
     * | for each other on the other side that record matches
     * | | remove the match of other
     * | endfor
     * endif
     *
     * remove the match of other:
     * if the matched num of other <= 0, skip, as the record was not counted
     * if the matched num of other == 1
     *   if semi join, send -D/-U[other] (using input RowKind)
     *   if anti join, send +I[other]
     * otherState.update(other, old-1)
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
            // do not store a record that can never match
            if (!joinCondition.currentKeyCannotMatch()) {
                rightRecordStateView.addRecord(input);
            }
            // a single pass for the matches that the record gains and the replaced record loses
            for (Tuple2<RowData, Integer> left :
                    leftRecordStateView.getRecordsAndNumOfAssociations()) {
                final RowData other = left.f0;
                final int numOfAssociations = left.f1;
                final boolean replacedRecordMatches =
                        replacedRecord != null && matches(replacedRecord, other, false);
                if (matches(input, other, false)) {
                    if (numOfAssociations <= 0) {
                        // send -D[other] if anti join, +I/+U[other] (using input RowKind) if semi
                        collect(other, isAntiJoin ? RowKind.DELETE : inputRowKind);
                        leftRecordStateView.updateNumOfAssociations(other, 1);
                    } else if (!replacedRecordMatches && !joinKeyContainsUniqueKey(false)) {
                        // with at most one record per join key, the existing match is the
                        // replaced record
                        leftRecordStateView.updateNumOfAssociations(other, numOfAssociations + 1);
                    }
                } else if (replacedRecordMatches) {
                    removeMatch(other, numOfAssociations, RowKind.DELETE);
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
                final OuterRecord outerRecord = associatedRecords.next();
                removeMatch(outerRecord.record, outerRecord.numOfAssociations, inputRowKind);
            }
        }
    }

    /**
     * Removes a match of the left record. If it was the last one, a semi join retracts the record
     * with the given RowKind and an anti join sends it.
     */
    private void removeMatch(RowData other, int numOfAssociations, RowKind retractionKind)
            throws Exception {
        if (numOfAssociations <= 0) {
            // the retracted record was not counted, e.g. it had expired when the left record was
            // stored
            return;
        }
        if (numOfAssociations == 1) {
            // send -D/-U[other] if semi join, +I[other] if anti join
            collect(other, isAntiJoin ? RowKind.INSERT : retractionKind);
        } // ignore when number > 1
        leftRecordStateView.updateNumOfAssociations(other, numOfAssociations - 1);
    }

    /** Returns whether a left record with the given number of matches is sent. */
    private boolean isSent(int numOfAssociations) {
        return isAntiJoin != (numOfAssociations > 0);
    }

    /** Sends the left record with the given RowKind. */
    private void collect(RowData left, RowKind rowKind) {
        left.setRowKind(rowKind);
        collector.collect(left);
        // set RowKind back, because the record may be the one in state
        left.setRowKind(RowKind.INSERT);
    }
}

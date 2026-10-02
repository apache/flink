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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.utils.JoinInputSideSpec;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.RuntimeChangelogMode;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Iterator;

/** Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN. */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when LEFT OUTER JOIN
    protected final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when RIGHT OUTER JOIN
    protected final boolean rightIsOuter;
    // changelog mode of the left input, null for compiled plans before Flink 2.4
    @Nullable protected final RuntimeChangelogMode leftInputChangelogMode;
    // changelog mode of the right input, null for compiled plans before Flink 2.4
    @Nullable protected final RuntimeChangelogMode rightInputChangelogMode;
    // whether the input may update a record without retracting it first (upsert), so that the
    // record it replaces has to be looked up in state
    private final boolean leftIsUpsertOrUnknown;
    private final boolean rightIsUpsertOrUnknown;
    // whether the join condition has a non-equi part, e.g. A JOIN B ON A.k = B.k AND A.v > B.v
    protected final boolean hasNonEquiCondition;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    protected transient JoinRecordStateView leftRecordStateView;
    // right join state
    protected transient JoinRecordStateView rightRecordStateView;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            @Nullable RuntimeChangelogMode leftInputChangelogMode,
            @Nullable RuntimeChangelogMode rightInputChangelogMode,
            boolean hasNonEquiCondition,
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
        this.leftInputChangelogMode = leftInputChangelogMode;
        this.rightInputChangelogMode = rightInputChangelogMode;
        this.leftIsUpsertOrUnknown = isUpsertOrUnknown(leftInputChangelogMode);
        this.rightIsUpsertOrUnknown = isUpsertOrUnknown(rightInputChangelogMode);
        this.hasNonEquiCondition = hasNonEquiCondition;
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
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true, null);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false, null);
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
     * |  if record replaces a stored record (upsert) and the condition is non-equi
     * |  |  for each other that the replaced record matches but the record does not
     * |  |  |  send -D[replaced+other]
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 1, send +I[null+other]
     * |  |  |  |  otherState.update(other, old - 1)
     * |  |  |  endif
     * |  |  endfor
     * |  endif
     * |  if input side is outer
     * |  |  if there is no matched rows on the other side, send +I[record+null], state.add(record, 0)
     * |  |  if there are matched rows on the other side
     * |  |  | if the replaced record was null padded, send -D[replaced+null]
     * |  |  | if other side is outer
     * |  |  | |  if the matched num in the matched rows == 0, send -D[null+other] unless the paired
     * |  |  | |  record matches other
     * |  |  | |  if the matched num in the matched rows > 0, skip
     * |  |  | |  if the replaced record did not match other, otherState.update(other, old + 1)
     * |  |  | endif
     * |  |  | send +I[record+other]s, state.add(record, other.size)
     * |  |  endif
     * |  endif
     * |  if input side not outer
     * |  |  state.add(record)
     * |  |  if there is no matched rows on the other side, skip
     * |  |  if there are matched rows on the other side
     * |  |  |  if other side is outer
     * |  |  |  |  if the matched num in the matched rows == 0, send -D[null+other] unless the
     * |  |  |  |  paired record matches other
     * |  |  |  |  if the matched num in the matched rows > 0, skip
     * |  |  |  |  if the replaced record did not match other, otherState.update(other, old + 1)
     * |  |  |  |  send +I[record+other]s
     * |  |  |  else
     * |  |  |  |  send +I/+U[record+other]s (using input RowKind)
     * |  |  |  endif
     * |  |  endif
     * |  endif
     * endif
     *
     * if input record is retract
     * |  state.retract(record) if there is no paired record
     * |  if there is no matched rows on the other side
     * |  | if input side is outer, send -D[record+null]
     * |  endif
     * |  if there are matched rows on the other side, send -D[record+other]s if outer, send -D/-U[record+other]s if inner.
     * |  |  if other side is outer
     * |  |  |  if the matched num in the matched rows == 0, this should never happen!
     * |  |  |  if the matched num in the matched rows == 1, send +I[null+other] unless the paired
     * |  |  |  record matches other
     * |  |  |  if the matched num in the matched rows > 1, skip
     * |  |  |  otherState.update(other, old - 1)
     * |  |  endif
     * |  endif
     * endif
     * </pre>
     *
     * @param input the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft whether input side is left side
     * @param pairedRecord the other record of a -U/+U pair in mini-batch mode, or null. Null
     *     padding changes that the pair cancels out are suppressed.
     */
    protected void processElement(
            RowData input,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft,
            @Nullable RowData pairedRecord)
            throws Exception {
        final boolean isSuppress = pairedRecord != null;
        boolean inputIsOuter = inputIsLeft ? leftIsOuter : rightIsOuter;
        boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        boolean isAccumulateMsg = RowDataUtil.isAccumulateMsg(input);
        RowKind inputRowKind = input.getRowKind();
        input.setRowKind(RowKind.INSERT); // erase RowKind for later state updating

        if (isAccumulateMsg) { // record is accumulate
            final RowData replacedRecord =
                    replacedRecord(input, inputSideStateView, inputIsLeft, isSuppress);
            boolean replacedRecordHadNoMatches = false;
            if (replacedRecord != null && hasNonEquiCondition) {
                replacedRecordHadNoMatches =
                        !retractLostMatches(input, replacedRecord, otherSideStateView, inputIsLeft);
            }
            if (inputIsOuter) { // input side is outer
                Iterator<OuterRecord> associatedRecords =
                        AbstractStreamingJoinOperator.iterator(
                                input, inputIsLeft, otherSideStateView, joinCondition);
                if (!associatedRecords.hasNext()) { // there is no matched rows on the other side
                    // send +I[record+null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    ((OuterJoinRecordStateView) inputSideStateView).addRecord(input, 0);
                    return;
                } else { // there are matched rows on the other side
                    if (replacedRecordHadNoMatches) {
                        // the replaced record was null padded, send -D[replaced+null]
                        outRow.setRowKind(RowKind.DELETE);
                        outputNullPadding(replacedRecord, inputIsLeft);
                    }
                    int numAssociations = 0;
                    while (associatedRecords.hasNext()) {
                        OuterRecord outerRecord = associatedRecords.next();
                        RowData other = outerRecord.record;
                        if (otherIsOuter) { // other side is outer
                            // if the matched num in the matched rows == 0
                            if (outerRecord.numOfAssociations == 0
                                    && !pairCancelsNullPadding(pairedRecord, other, inputIsLeft)) {
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0
                            // otherState.update(other, old + 1)
                            if (isNewMatch(outerRecord, replacedRecord, inputIsLeft)) {
                                ((OuterJoinRecordStateView) otherSideStateView)
                                        .updateNumOfAssociations(
                                                other, outerRecord.numOfAssociations + 1);
                            }
                        }
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                        output(input, other, inputIsLeft);
                        numAssociations++;
                    }
                    // state.add(record, other.size)
                    ((OuterJoinRecordStateView) inputSideStateView)
                            .addRecord(input, numAssociations);
                }
            } else { // input side not outer
                // state.add(record)
                inputSideStateView.addRecord(input);
                Iterator<OuterRecord> associatedRecords =
                        AbstractStreamingJoinOperator.iterator(
                                input, inputIsLeft, otherSideStateView, joinCondition);
                if (associatedRecords.hasNext()) {
                    if (otherIsOuter) { // if other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        while (associatedRecords.hasNext()) {
                            OuterRecord outerRecord = associatedRecords.next();
                            if (outerRecord.numOfAssociations == 0
                                    && !pairCancelsNullPadding(
                                            pairedRecord, outerRecord.record, inputIsLeft)) {
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            if (isNewMatch(outerRecord, replacedRecord, inputIsLeft)) {
                                otherSideOuterStateView.updateNumOfAssociations(
                                        outerRecord.record, outerRecord.numOfAssociations + 1);
                            }
                            // send +I[record+other]s
                            outRow.setRowKind(RowKind.INSERT);
                            output(input, outerRecord.record, inputIsLeft);
                        }
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                        while (associatedRecords.hasNext()) {
                            OuterRecord other = associatedRecords.next();
                            output(input, other.record, inputIsLeft);
                        }
                    }
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            // state.retract(record)
            if (!isSuppress) {
                inputSideStateView.retractRecord(input);
            }
            Iterator<OuterRecord> associatedRecords =
                    AbstractStreamingJoinOperator.iterator(
                            input, inputIsLeft, otherSideStateView, joinCondition);
            if (!associatedRecords.hasNext()) { // there is no matched rows on the other side
                if (inputIsOuter) { // input side is outer
                    // send -D[record+null]
                    outRow.setRowKind(RowKind.DELETE);
                    outputNullPadding(input, inputIsLeft);
                }
                // nothing to do when input side is not outer
            } else { // there are matched rows on the other side
                while (associatedRecords.hasNext()) {
                    if (inputIsOuter) {
                        // send -D[record+other]s
                        outRow.setRowKind(RowKind.DELETE);
                    } else {
                        // send -D/-U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    OuterRecord outerRecord = associatedRecords.next();
                    output(input, outerRecord.record, inputIsLeft);
                    // if other side is outer
                    if (otherIsOuter) {
                        OuterJoinRecordStateView otherSideOuterStateView =
                                (OuterJoinRecordStateView) otherSideStateView;
                        if (outerRecord.numOfAssociations == 1
                                && !pairCancelsNullPadding(
                                        pairedRecord, outerRecord.record, inputIsLeft)) {
                            // send +I[null+other]
                            outRow.setRowKind(RowKind.INSERT);
                            outputNullPadding(outerRecord.record, !inputIsLeft);
                        } // nothing else to do when number of associations > 1
                        // otherState.update(other, old - 1)
                        otherSideOuterStateView.updateNumOfAssociations(
                                outerRecord.record, outerRecord.numOfAssociations - 1);
                    }
                }
            }
        }
    }

    /**
     * Returns the stored record that the given record replaces, or null if there is none. An upsert
     * input updates a record by sending only the new version, without retracting the old one.
     *
     * <p>The lookup only happens if it can change the result. In a mini-batch pair, the retraction
     * already handled the old version.
     */
    private @Nullable RowData replacedRecord(
            RowData record, JoinRecordStateView stateView, boolean isLeft, boolean isSuppress)
            throws Exception {
        final boolean inputIsUpsertOrUnknown =
                isLeft ? leftIsUpsertOrUnknown : rightIsUpsertOrUnknown;
        final boolean otherIsOuter = isLeft ? rightIsOuter : leftIsOuter;
        // with at most one record per join key, the count of the other side is right without it
        final boolean mayCountTwice = otherIsOuter && !joinKeyContainsUniqueKey(isLeft);
        final boolean mayMatchDifferently = hasNonEquiCondition;
        if (isSuppress || !inputIsUpsertOrUnknown || !(mayCountTwice || mayMatchDifferently)) {
            return null;
        }
        return stateView.getRecord(record);
    }

    /**
     * Retracts the joined rows that only the replaced record produced, i.e. with the rows of the
     * other side that it matched but the new record does not. Returns whether the replaced record
     * matched any row.
     */
    private boolean retractLostMatches(
            RowData record,
            RowData replacedRecord,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        final boolean otherIsOuter = inputIsLeft ? rightIsOuter : leftIsOuter;
        final Iterator<OuterRecord> matchesOfReplacedRecord =
                AbstractStreamingJoinOperator.iterator(
                        replacedRecord, inputIsLeft, otherSideStateView, joinCondition);
        final boolean replacedRecordHadMatches = matchesOfReplacedRecord.hasNext();
        while (matchesOfReplacedRecord.hasNext()) {
            final OuterRecord other = matchesOfReplacedRecord.next();
            final boolean bothOldAndNewVersionsMatch = matches(record, other.record, inputIsLeft);
            if (bothOldAndNewVersionsMatch) {
                continue;
            }
            // send -D[replaced+other]
            outRow.setRowKind(RowKind.DELETE);
            output(replacedRecord, other.record, inputIsLeft);
            if (otherIsOuter) {
                final boolean wasLastMatch = other.numOfAssociations == 1;
                if (wasLastMatch) {
                    // send +I[null+other]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(other.record, !inputIsLeft);
                }
                // otherState.update(other, old - 1)
                ((OuterJoinRecordStateView) otherSideStateView)
                        .updateNumOfAssociations(other.record, other.numOfAssociations - 1);
            }
        }
        return replacedRecordHadMatches;
    }

    /**
     * Returns whether the other record gains a match, so that its number of associations increases.
     * It does not if the replaced record matched it already.
     */
    private boolean isNewMatch(
            OuterRecord other, @Nullable RowData replacedRecord, boolean inputIsLeft) {
        if (other.numOfAssociations == 0) {
            return true;
        }
        // with at most one record per join key, the existing match is the replaced record
        if (joinKeyContainsUniqueKey(inputIsLeft)) {
            return false;
        }
        return replacedRecord == null || !matches(replacedRecord, other.record, inputIsLeft);
    }

    /**
     * Returns whether the paired record of a -U/+U pair cancels out the null padding change of the
     * other record, which is the case if both records of the pair match it.
     */
    private boolean pairCancelsNullPadding(
            @Nullable RowData pairedRecord, RowData other, boolean inputIsLeft) {
        return pairedRecord != null
                && (!hasNonEquiCondition || matches(pairedRecord, other, inputIsLeft));
    }

    /** Returns whether the input may update a record without retracting it first. */
    private static boolean isUpsertOrUnknown(@Nullable RuntimeChangelogMode inputChangelogMode) {
        // compiled plans before Flink 2.4 do not contain the changelog mode, and handling
        // their inputs as upsert is always correct
        if (inputChangelogMode == null) {
            return true;
        }
        final ChangelogMode changelogMode = inputChangelogMode.deserialize();
        return changelogMode.contains(RowKind.UPDATE_AFTER)
                && !changelogMode.contains(RowKind.UPDATE_BEFORE);
    }

    private boolean joinKeyContainsUniqueKey(boolean isLeft) {
        return (isLeft ? leftInputSideSpec : rightInputSideSpec).joinKeyContainsUniqueKey();
    }

    private boolean matches(RowData inputRecord, RowData otherRecord, boolean inputIsLeft) {
        return inputIsLeft
                ? joinCondition.apply(inputRecord, otherRecord)
                : joinCondition.apply(otherRecord, inputRecord);
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

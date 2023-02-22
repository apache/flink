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

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.join.stream.minibatch.MiniBatchJoinBuffer;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateViews;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateView;
import org.apache.flink.table.runtime.operators.join.stream.state.OuterJoinRecordStateViews;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.runtime.util.RowDataStringSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * Streaming unbounded Join operator which supports INNER/LEFT/RIGHT/FULL JOIN.
 */
public class StreamingJoinOperator extends AbstractStreamingJoinOperator {

    private static final long serialVersionUID = -376944622236540545L;

    // whether left side is outer side, e.g. left is outer but right is not when
    // LEFT OUTER JOIN
    private final boolean leftIsOuter;
    // whether right side is outer side, e.g. right is outer but left is not when
    // RIGHT OUTER JOIN
    private final boolean rightIsOuter;

    private final boolean isMinibatchEnabled;
    private final int maxMinibatchSize;

    private transient JoinedRowData outRow;
    private transient RowData leftNullRow;
    private transient RowData rightNullRow;

    // left join state
    private transient JoinRecordStateView leftRecordStateView;
    private transient MiniBatchJoinBuffer leftRecordStateBuffer;

    // right join state
    private transient JoinRecordStateView rightRecordStateView;
    private transient MiniBatchJoinBuffer rightRecordStateBuffer;

    private transient Counter leftInputCount;
    private transient Counter rightInputCount;
    private transient Counter leftInputNullKeyCount;
    private transient Counter rightInputNullKeyCount;
    private transient Counter leftInputDroppedNullKeyCount;
    private transient Counter rightInputDroppedNullKeyCount;

    public StreamingJoinOperator(
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long stateRetentionTime,
            boolean isBatchBackfillEnabled,
            boolean isMinibatchEnabled,
            int maxMinibatchSize) {
        super(
                leftType,
                rightType,
                generatedJoinCondition,
                leftInputSideSpec,
                rightInputSideSpec,
                filterNullKeys,
                stateRetentionTime,
                isBatchBackfillEnabled);
        this.leftIsOuter = leftIsOuter;
        this.rightIsOuter = rightIsOuter;
        this.isMinibatchEnabled = isMinibatchEnabled;
        this.maxMinibatchSize = maxMinibatchSize;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.outRow = new JoinedRowData();
        this.leftNullRow = new GenericRowData(leftType.toRowSize());
        this.rightNullRow = new GenericRowData(rightType.toRowSize());

        this.leftInputCount = getRuntimeContext().getMetricGroup().counter("join.leftInputCount");
        this.rightInputCount = getRuntimeContext().getMetricGroup().counter("join.rightInputCount");
        this.leftInputNullKeyCount = getRuntimeContext().getMetricGroup().counter("join.leftInputNullKeyCount");
        this.rightInputNullKeyCount = getRuntimeContext().getMetricGroup().counter("join.rightInputNullKeyCount");
        this.leftInputDroppedNullKeyCount = getRuntimeContext().getMetricGroup().counter("join.leftInputDroppedNullKeyCount");
        this.rightInputDroppedNullKeyCount = getRuntimeContext().getMetricGroup().counter("join.rightInputDroppedNullKeyCount");

        // initialize states
        if (leftIsOuter) {
            this.leftRecordStateView = OuterJoinRecordStateViews.create(
                    getRuntimeContext(),
                    "left-records",
                    leftInputSideSpec,
                    leftType,
                    rightType,
                    stateRetentionTime);
        } else {
            this.leftRecordStateView = JoinRecordStateViews.create(
                    getRuntimeContext(),
                    "left-records",
                    leftInputSideSpec,
                    leftType,
                    stateRetentionTime);
        }

        if (rightIsOuter) {
            this.rightRecordStateView = OuterJoinRecordStateViews.create(
                    getRuntimeContext(),
                    "right-records",
                    rightInputSideSpec,
                    rightType,
                    leftType,
                    stateRetentionTime);
        } else {
            this.rightRecordStateView = JoinRecordStateViews.create(
                    getRuntimeContext(),
                    "right-records",
                    rightInputSideSpec,
                    rightType,
                    stateRetentionTime);
        }

        // initialize minibatch buffer states
        if (isMinibatchEnabled) {
            leftRecordStateBuffer = new MiniBatchJoinBuffer(
                getOperatorName() + " - LEFT input",
                leftType, (KeySelector<RowData, RowData>) stateKeySelector1, maxMinibatchSize);
            rightRecordStateBuffer = new MiniBatchJoinBuffer(
                getOperatorName() + " - RIGHT input",
                rightType, (KeySelector<RowData, RowData>) stateKeySelector2, maxMinibatchSize);
        }
    }

    private boolean isKeyAnyNulls() {
        RowData key = (RowData) getCurrentKey();
        for (int i = 0; i < key.getArity(); i++) {
            if (key.isNullAt(i)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        leftInputCount.inc();
        if (isKeyAnyNulls()) {
            leftInputNullKeyCount.inc();
            if (!leftIsOuter) {
                // performance optimization: if the input key is null and it's
                // not an outer side, we can simply ignore the input row
                RowDataStringSerializer rowStringSerializer = new RowDataStringSerializer(leftType);
                LOG.debug("dropping LEFT input {}", rowStringSerializer.asString(element.getValue()));
                leftInputDroppedNullKeyCount.inc();
                return;
            }
        }
        if (isMinibatchEnabled && !isBatchMode()) {
            RowData input = element.getValue();
            leftRecordStateBuffer.addRecordToBatch(input, this.shouldLogInput());
            if (leftRecordStateBuffer.batchNeedsFlush()) {
                flushLeftMinibatch();
            }
        } else {
            processElement(element.getValue(), leftRecordStateView, rightRecordStateView, true);
        }
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        rightInputCount.inc();
        if (isKeyAnyNulls()) {
            rightInputNullKeyCount.inc();
            if (!rightIsOuter) {
                // performance optimization: if the input key is null and it's
                // not an outer side, we can simply ignore the input row
                RowDataStringSerializer rowStringSerializer = new RowDataStringSerializer(rightType);
                LOG.debug("dropping RIGHT input {}", rowStringSerializer.asString(element.getValue()));
                rightInputDroppedNullKeyCount.inc();
                return;
            }
        }
        if (isMinibatchEnabled && !isBatchMode()) {
            RowData input = element.getValue();
            rightRecordStateBuffer.addRecordToBatch(input, this.shouldLogInput());
            if (rightRecordStateBuffer.batchNeedsFlush()) {
                flushRighMinibatch();
            }
        } else {
            processElement(element.getValue(), rightRecordStateView, leftRecordStateView, false);
        }
    }

    private void flushLeftMinibatch() throws Exception {
        if (isMinibatchEnabled) {
            leftRecordStateBuffer.processBatch(getKeyedStateBackend(), record -> {
                processElement(record, leftRecordStateView, rightRecordStateView, true);
            });
        }
    }

    private void flushRighMinibatch() throws Exception {
        if (isMinibatchEnabled) {
            rightRecordStateBuffer.processBatch(getKeyedStateBackend(), record -> {
                processElement(record, rightRecordStateView, leftRecordStateView, false);
            });
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        if (!isBatchMode()) {
            if (this.shouldLogInput()){
                LOG.info("MINIBATCH WATERMARK in streaming mode {}", mark);
            } else {
                LOG.debug("MINIBATCH WATERMARK in streaming mode {}", mark);
            }
            flushRighMinibatch();
            flushLeftMinibatch();
        }
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        flushRighMinibatch();
        flushLeftMinibatch();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    protected boolean isHybridStreamBatchCapable() {
        return true;
    }

    @Override
    protected void emitStateAndSwitchToStreaming() throws Exception {
        LOG.info("{} emit and switch to streaming", getPrintableName());
        if (leftIsOuter) {
            leftRecordStateView.emitCompleteState(getKeyedStateBackend(), this.collector,
                    rightRecordStateView, joinCondition, false, true /* inputIsLeft */);
            if (rightIsOuter) {
                // FULL JOIN condition, we want to emit the following
                // leftState -> emitComplete
                // rightState -> emitAntiJoin (everything in right that doesn't match left)

                OuterJoinRecordStateView rightView = (OuterJoinRecordStateView) rightRecordStateView;
                rightView.emitAntiJoinState(getKeyedStateBackend(), this.collector,
                        leftRecordStateView, joinCondition, false, false /* inputIsLeft */);
            }
        } else if (rightIsOuter) {
            // RIGHT OUTER JOIN
            rightRecordStateView.emitCompleteState(getKeyedStateBackend(), this.collector,
                    leftRecordStateView, joinCondition, false, false /* inputIsLeft */);
        } else {
            // standard inner join
            leftRecordStateView.emitCompleteState(getKeyedStateBackend(), this.collector,
                    rightRecordStateView, joinCondition, false, true /* inputIsLeft */);
        }

        setStreamMode(true);
    }

    /**
     * Process an input element and output incremental joined records, retraction
     * messages will be
     * sent in some scenarios.
     *
     * <p>
     * Following is the pseudo code to describe the core logic of this method. The
     * logic of this
     * method is too complex, so we provide the pseudo code to help understand the
     * logic. We should
     * keep sync the following pseudo code with the real logic of the method.
     *
     * <p>
     * Note: "+I" represents "INSERT", "-D" represents "DELETE", "+U" represents
     * "UPDATE_AFTER",
     * "-U" represents "UPDATE_BEFORE". We forward input RowKind if it is inner
     * join, otherwise, we
     * always send insert and delete for simplification. We can optimize this to
     * send -U & +U
     * instead of D & I in the future (see FLINK-17337). They are equivalent in this
     * join case. It
     * may need some refactoring if we want to send -U & +U, so we still keep -D &
     * +I for now for
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
     * @param input              the input element
     * @param inputSideStateView state of input side
     * @param otherSideStateView state of other side
     * @param inputIsLeft        whether input side is left side
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

        // Pass in leftType, rightType, OperatorName, which are used to log expensive
        // joins.
        AssociatedRecords associatedRecords = AssociatedRecords.of(input, inputIsLeft, this.leftType, this.rightType,
                getOperatorName(), otherSideStateView, joinCondition);
        if (this.shouldLogInput()) {
            RowDataStringSerializer rowStringSerializer = new RowDataStringSerializer(
                    inputIsLeft ? leftType : rightType);
            LOG.info("Processing input row: " + rowStringSerializer.asString(input) + " (original RowKind: " + inputRowKind + ")");
        }
        if (isAccumulateMsg) { // record is accumulate
            if (inputIsOuter) { // input side is outer
                OuterJoinRecordStateView inputSideOuterStateView = (OuterJoinRecordStateView) inputSideStateView;
                if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
                    // send +I[record+null]
                    outRow.setRowKind(RowKind.INSERT);
                    outputNullPadding(input, inputIsLeft);
                    // state.add(record, 0)
                    inputSideOuterStateView.addRecord(input, 0);
                } else { // there are matched rows on the other side
                    if (otherIsOuter) { // other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            RowData other = outerRecord.record;
                            // if the matched num in the matched rows == 0
                            if (outerRecord.numOfAssociations == 0) {
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(other, !inputIsLeft);
                            } // ignore matched number > 0
                              // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    other, outerRecord.numOfAssociations + 1);
                        }
                    }
                    // send +I[record+other]s
                    if (!isBatchMode()) {
                        // do not even retrieve records since the results are only used
                        // to emit and not mutate state, we do not want to waste cycles
                        outRow.setRowKind(RowKind.INSERT);
                        for (RowData other : associatedRecords.getRecords()) {
                            output(input, other, inputIsLeft);
                        }
                    }
                    // state.add(record, other.size)
                    inputSideOuterStateView.addRecord(input, associatedRecords.size());
                }
            } else { // input side not outer
                // state.add(record)
                inputSideStateView.addRecord(input);
                if (!associatedRecords.isEmpty()) { // if there are matched rows on the other side
                    if (otherIsOuter) { // if other side is outer
                        OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
                        for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                            if (outerRecord.numOfAssociations == 0) { // if the matched num in the matched rows == 0
                                // send -D[null+other]
                                outRow.setRowKind(RowKind.DELETE);
                                outputNullPadding(outerRecord.record, !inputIsLeft);
                            }
                            // otherState.update(other, old + 1)
                            otherSideOuterStateView.updateNumOfAssociations(
                                    outerRecord.record, outerRecord.numOfAssociations + 1);
                        }
                        // send +I[record+other]s
                        outRow.setRowKind(RowKind.INSERT);
                    } else {
                        // send +I/+U[record+other]s (using input RowKind)
                        outRow.setRowKind(inputRowKind);
                    }
                    if (!isBatchMode()) {
                        // do not even retrieve records since the results are only used
                        // to emit and not mutate state, we do not want to waste cycles
                        for (RowData other : associatedRecords.getRecords()) {
                            output(input, other, inputIsLeft);
                        }
                    }
                }
                // skip when there is no matched rows on the other side
            }
        } else { // input record is retract
            // state.retract(record)
            inputSideStateView.retractRecord(input);
            if (associatedRecords.isEmpty()) { // there is no matched rows on the other side
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
                if (!isBatchMode()) {
                    // do not even retrieve records since the results are only used
                    // to emit and not mutate state, we do not want to waste cycles
                    for (RowData other : associatedRecords.getRecords()) {
                        output(input, other, inputIsLeft);
                    }
                }
                // if other side is outer
                if (otherIsOuter) {
                    OuterJoinRecordStateView otherSideOuterStateView = (OuterJoinRecordStateView) otherSideStateView;
                    for (OuterRecord outerRecord : associatedRecords.getOuterRecords()) {
                        if (outerRecord.numOfAssociations == 1) {
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

    // -------------------------------------------------------------------------------------

    private void output(RowData inputRow, RowData otherRow, boolean inputIsLeft) {
        if (isBatchMode()) {
            return;
        }
        if (inputIsLeft) {
            outRow.replace(inputRow, otherRow);
        } else {
            outRow.replace(otherRow, inputRow);
        }
        collector.collect(outRow);
    }

    private void outputNullPadding(RowData row, boolean isLeft) {
        if (isBatchMode()) {
            return;
        }
        if (isLeft) {
            outRow.replace(row, rightNullRow);
        } else {
            outRow.replace(leftNullRow, row);
        }
        collector.collect(outRow);
    }
}

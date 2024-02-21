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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.operators.bundle.trigger.CoBundleTrigger;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.runtime.operators.join.stream.bundle.BufferBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasNoUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.InputSideHasUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.bundle.JoinKeyContainsUniqueKeyBundle;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec;
import org.apache.flink.table.runtime.operators.join.stream.state.JoinRecordStateView;
import org.apache.flink.table.runtime.operators.metrics.SimpleGauge;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/** Streaming unbounded Join base operator which support mini-batch join. */
public abstract class MiniBatchStreamingJoinOperator extends StreamingJoinOperator
        implements BundleTriggerCallback {

    private static final long serialVersionUID = -1106342589994963997L;

    private final CoBundleTrigger<RowData, RowData> coBundleTrigger;

    private transient BufferBundle<?> leftBuffer;
    private transient BufferBundle<?> rightBuffer;
    private transient SimpleGauge<Integer> leftBundleReducedSizeGauge;
    private transient SimpleGauge<Integer> rightBundleReducedSizeGauge;
    private transient TypeSerializer<RowData> leftSerializer;
    private transient TypeSerializer<RowData> rightSerializer;

    public MiniBatchStreamingJoinOperator(MiniBatchStreamingJoinParameter parameter) {
        super(
                parameter.leftType,
                parameter.rightType,
                parameter.generatedJoinCondition,
                parameter.leftInputSideSpec,
                parameter.rightInputSideSpec,
                parameter.leftIsOuter,
                parameter.rightIsOuter,
                parameter.filterNullKeys,
                parameter.leftStateRetentionTime,
                parameter.rightStateRetentionTime);

        this.coBundleTrigger = parameter.coBundleTrigger;
    }

    @Override
    public void open() throws Exception {
        super.open();

        this.leftBuffer = initialBuffer(leftInputSideSpec);
        this.rightBuffer = initialBuffer(rightInputSideSpec);

        coBundleTrigger.registerCallback(this);
        coBundleTrigger.reset();
        LOG.info("Initialize MiniBatchStreamingJoinOperator successfully.");

        this.leftSerializer = leftType.createSerializer(getExecutionConfig());
        this.rightSerializer = rightType.createSerializer(getExecutionConfig());

        // register metrics
        leftBundleReducedSizeGauge = new SimpleGauge<>(0);
        rightBundleReducedSizeGauge = new SimpleGauge<>(0);

        getRuntimeContext()
                .getMetricGroup()
                .gauge("leftBundleReducedSize", leftBundleReducedSizeGauge);
        getRuntimeContext()
                .getMetricGroup()
                .gauge("rightBundleReducedSize", rightBundleReducedSizeGauge);
    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {
        RowData record = leftSerializer.copy(element.getValue());
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = null;
        if (leftInputSideSpec.getUniqueKeySelector() != null) {
            uniqueKey = leftInputSideSpec.getUniqueKeySelector().getKey(record);
        }
        leftBuffer.addRecord(joinKey, uniqueKey, record);
        coBundleTrigger.onElement1(record);
    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {
        RowData record = rightSerializer.copy(element.getValue());
        RowData joinKey = (RowData) getCurrentKey();
        RowData uniqueKey = null;
        if (rightInputSideSpec.getUniqueKeySelector() != null) {
            uniqueKey = rightInputSideSpec.getUniqueKeySelector().getKey(record);
        }
        rightBuffer.addRecord(joinKey, uniqueKey, record);
        coBundleTrigger.onElement2(record);
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        finishBundle();
        super.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        finishBundle();
        super.processWatermark2(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        finishBundle();
    }

    @Override
    public void finish() throws Exception {
        finishBundle();
        super.finish();
    }

    @Override
    public void close() throws Exception {
        super.close();
        this.leftBuffer.clear();
        this.rightBuffer.clear();
    }

    @Override
    public void finishBundle() throws Exception {
        if (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {
            // update metrics value
            leftBundleReducedSizeGauge.update(leftBuffer.reducedSize());
            rightBundleReducedSizeGauge.update(rightBuffer.reducedSize());

            this.processBundles(leftBuffer, rightBuffer);
            leftBuffer.clear();
            rightBuffer.clear();
        }
        coBundleTrigger.reset();
    }

    protected abstract void processBundles(BufferBundle<?> leftBuffer, BufferBundle<?> rightBuffer)
            throws Exception;

    private BufferBundle<?> initialBuffer(JoinInputSideSpec inputSideSpec) {
        if (inputSideSpec.joinKeyContainsUniqueKey()) {
            return new JoinKeyContainsUniqueKeyBundle();
        }
        if (inputSideSpec.hasUniqueKey()) {
            return new InputSideHasUniqueKeyBundle();
        }
        return new InputSideHasNoUniqueKeyBundle();
    }

    private void processElementWithSuppress(
            Iterator<RowData> iter,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        RowData pre = null; // always retractMsg if not null
        while (iter.hasNext()) {
            RowData current = iter.next();
            boolean isSuppress = false;
            if (RowDataUtil.isRetractMsg(current) && iter.hasNext()) {
                RowData next = iter.next();
                if (RowDataUtil.isAccumulateMsg(next)) {
                    isSuppress = true;
                } else {
                    // retract + retract
                    pre = next;
                }
                processElement(
                        current, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                if (isSuppress) {
                    processElement(
                            next, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                }
            } else {
                // 1. current is accumulateMsg 2. current is retractMsg and no next row
                if (pre != null) {
                    if (RowDataUtil.isAccumulateMsg(current)) {
                        isSuppress = true;
                    }
                    processElement(
                            pre, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
                    pre = null;
                }
                processElement(
                        current, inputSideStateView, otherSideStateView, inputIsLeft, isSuppress);
            }
        }
    }

    /**
     * RetractMsg+accumulatingMsg would be optimized which would keep sending retractMsg but do not
     * deal with state.
     */
    protected void processSingleSideBundles(
            BufferBundle<?> inputBuffer,
            JoinRecordStateView inputSideStateView,
            JoinRecordStateView otherSideStateView,
            boolean inputIsLeft)
            throws Exception {
        if (inputBuffer instanceof InputSideHasNoUniqueKeyBundle) {
            // -U/+U pair is already folded in the buffer so it is no need to go to
            // processElementByPair function
            for (Map.Entry<RowData, List<RowData>> entry : inputBuffer.getRecords().entrySet()) {
                // set state key first
                setCurrentKey(entry.getKey());
                for (RowData rowData : entry.getValue()) {
                    processElement(
                            rowData, inputSideStateView, otherSideStateView, inputIsLeft, false);
                }
            }
        } else if (inputBuffer instanceof JoinKeyContainsUniqueKeyBundle) {
            for (Map.Entry<RowData, List<RowData>> entry : inputBuffer.getRecords().entrySet()) {
                // set state key first
                setCurrentKey(entry.getKey());
                Iterator<RowData> iter = entry.getValue().iterator();
                processElementWithSuppress(
                        iter, inputSideStateView, otherSideStateView, inputIsLeft);
            }
        } else if (inputBuffer instanceof InputSideHasUniqueKeyBundle) {
            for (RowData joinKey : inputBuffer.getJoinKeys()) {
                // set state key first
                setCurrentKey(joinKey);
                for (Map.Entry<RowData, List<RowData>> entry :
                        inputBuffer.getRecordsWithJoinKey(joinKey).entrySet()) {
                    Iterator<RowData> iter = entry.getValue().iterator();
                    processElementWithSuppress(
                            iter, inputSideStateView, otherSideStateView, inputIsLeft);
                }
            }
        }
    }

    public static MiniBatchStreamingJoinOperator newMiniBatchStreamJoinOperator(
            FlinkJoinType joinType,
            InternalTypeInfo<RowData> leftType,
            InternalTypeInfo<RowData> rightType,
            GeneratedJoinCondition generatedJoinCondition,
            JoinInputSideSpec leftInputSideSpec,
            JoinInputSideSpec rightInputSideSpec,
            boolean leftIsOuter,
            boolean rightIsOuter,
            boolean[] filterNullKeys,
            long leftStateRetentionTime,
            long rightStateRetentionTime,
            CoBundleTrigger<RowData, RowData> coBundleTrigger) {
        MiniBatchStreamingJoinParameter parameter =
                new MiniBatchStreamingJoinParameter(
                        leftType,
                        rightType,
                        generatedJoinCondition,
                        leftInputSideSpec,
                        rightInputSideSpec,
                        leftIsOuter,
                        rightIsOuter,
                        filterNullKeys,
                        leftStateRetentionTime,
                        rightStateRetentionTime,
                        coBundleTrigger);
        switch (joinType) {
            case INNER:
                return new MiniBatchInnerJoinStreamOperator(parameter);
            case LEFT:
                return new MiniBatchLeftOuterJoinStreamOperator(parameter);
            case RIGHT:
                return new MiniBatchRightOuterJoinStreamOperator(parameter);
            case FULL:
                return new MiniBatchFullOuterJoinStreamOperator(parameter);
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
    }

    static class MiniBatchStreamingJoinParameter implements Serializable {
        InternalTypeInfo<RowData> leftType;
        InternalTypeInfo<RowData> rightType;
        GeneratedJoinCondition generatedJoinCondition;
        JoinInputSideSpec leftInputSideSpec;
        JoinInputSideSpec rightInputSideSpec;
        boolean leftIsOuter;
        boolean rightIsOuter;
        boolean[] filterNullKeys;
        long leftStateRetentionTime;
        long rightStateRetentionTime;

        CoBundleTrigger<RowData, RowData> coBundleTrigger;

        MiniBatchStreamingJoinParameter(
                InternalTypeInfo<RowData> leftType,
                InternalTypeInfo<RowData> rightType,
                GeneratedJoinCondition generatedJoinCondition,
                JoinInputSideSpec leftInputSideSpec,
                JoinInputSideSpec rightInputSideSpec,
                boolean leftIsOuter,
                boolean rightIsOuter,
                boolean[] filterNullKeys,
                long leftStateRetentionTime,
                long rightStateRetentionTime,
                CoBundleTrigger<RowData, RowData> coBundleTrigger) {
            this.leftType = leftType;
            this.rightType = rightType;
            this.generatedJoinCondition = generatedJoinCondition;
            this.leftInputSideSpec = leftInputSideSpec;
            this.rightInputSideSpec = rightInputSideSpec;
            this.leftIsOuter = leftIsOuter;
            this.rightIsOuter = rightIsOuter;
            this.filterNullKeys = filterNullKeys;
            this.leftStateRetentionTime = leftStateRetentionTime;
            this.rightStateRetentionTime = rightStateRetentionTime;
            this.coBundleTrigger = coBundleTrigger;
        }
    }

    /** Inner MiniBatch Join operator. */
    private static final class MiniBatchInnerJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchInnerJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected void processBundles(BufferBundle<?> leftBuffer, BufferBundle<?> rightBuffer)
                throws Exception {
            // process right
            this.processSingleSideBundles(
                    rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            this.processSingleSideBundles(
                    leftBuffer, leftRecordStateView, rightRecordStateView, true);
        }
    }

    /** MiniBatch Left outer join operator. */
    private static final class MiniBatchLeftOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchLeftOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected void processBundles(BufferBundle<?> leftBuffer, BufferBundle<?> rightBuffer)
                throws Exception {
            // more efficient to process right first for left out join, i.e, some retractions can be
            // avoided for timing-dependent left and right stream
            // process right
            this.processSingleSideBundles(
                    rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            this.processSingleSideBundles(
                    leftBuffer, leftRecordStateView, rightRecordStateView, true);
        }
    }

    /** MiniBatch Right outer join operator. */
    private static final class MiniBatchRightOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchRightOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected void processBundles(BufferBundle<?> leftBuffer, BufferBundle<?> rightBuffer)
                throws Exception {

            // more efficient to process left first for right out join, i.e, some retractions can be
            // avoided for timing-dependent left and right stream
            // process left
            this.processSingleSideBundles(
                    leftBuffer, leftRecordStateView, rightRecordStateView, true);

            // process right
            this.processSingleSideBundles(
                    rightBuffer, rightRecordStateView, leftRecordStateView, false);
        }
    }

    /** MiniBatch Full outer Join operator. */
    private static final class MiniBatchFullOuterJoinStreamOperator
            extends MiniBatchStreamingJoinOperator {

        public MiniBatchFullOuterJoinStreamOperator(MiniBatchStreamingJoinParameter parameter) {
            super(parameter);
        }

        @Override
        protected void processBundles(BufferBundle<?> leftBuffer, BufferBundle<?> rightBuffer)
                throws Exception {
            // process right
            this.processSingleSideBundles(
                    rightBuffer, rightRecordStateView, leftRecordStateView, false);
            // process left
            this.processSingleSideBundles(
                    leftBuffer, leftRecordStateView, rightRecordStateView, true);
        }
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import static org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup;

/** A bounded one-input or two-input stream operator for test. */
public class TestFinishedOnRestoreStreamOperator
        implements OneInputStreamOperator<String, String>,
                TwoInputStreamOperator<String, String, String>,
                BoundedOneInput,
                BoundedMultiInput {
    private static final long serialVersionUID = 1L;

    protected static final String MESSAGE = "This should never be called";

    @Override
    public void open() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processElement(StreamRecord<String> element) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermark(Watermark mark) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void endInput() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void finish() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        // TODO: Should this be allowed to access for finished tasks?
        return createUnregisteredOperatorMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void setCurrentKey(Object key) {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Object getCurrentKey() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processElement1(StreamRecord<String> element) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processElement2(StreamRecord<String> element) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermarkStatus1(WatermarkStatus watermarkStatus) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void processWatermarkStatus2(WatermarkStatus watermarkStatus) throws Exception {
        throw new IllegalStateException(MESSAGE);
    }
}

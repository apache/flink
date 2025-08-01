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

package org.apache.flink.state.api.output.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.state.api.output.SnapshotUtils;
import org.apache.flink.state.api.output.TaggedOperatorSubtaskState;
import org.apache.flink.state.api.runtime.NeverFireProcessingTimeService;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.OutputTag;

/** Wraps an existing operator so it can be bootstrapped. */
@Internal
@SuppressWarnings({"unchecked", "deprecation", "rawtypes"})
public final class StateBootstrapWrapperOperator<
                IN, OUT, OP extends AbstractStreamOperator<OUT> & OneInputStreamOperator<IN, OUT>>
        implements OneInputStreamOperator<IN, TaggedOperatorSubtaskState>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final long checkpointId;

    private final long timestamp;

    private final Path savepointPath;

    private Output<StreamRecord<TaggedOperatorSubtaskState>> output;

    private final WindowOperator<?, IN, ?, ?, ?> operator;

    public StateBootstrapWrapperOperator(
            long checkpointId,
            long timestamp,
            Path savepointPath,
            WindowOperator<?, IN, ?, ?, ?> operator) {
        this.checkpointId = checkpointId;
        this.timestamp = timestamp;
        this.savepointPath = savepointPath;
        this.operator = operator;
    }

    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<TaggedOperatorSubtaskState>> output) {
        operator.setup(containingTask, config, new VoidOutput<>());
        operator.setProcessingTimeService(new NeverFireProcessingTimeService());
        this.output = output;
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        operator.processElement(element);
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        operator.processWatermark(mark);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        operator.processLatencyMarker(latencyMarker);
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        operator.processWatermarkStatus(watermarkStatus);
    }

    @Override
    public void open() throws Exception {
        operator.open();
    }

    @Override
    public void finish() throws Exception {
        operator.finish();
    }

    @Override
    public void close() throws Exception {
        operator.close();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        operator.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public OperatorSnapshotFutures snapshotState(
            long checkpointId,
            long timestamp,
            CheckpointOptions checkpointOptions,
            CheckpointStreamFactory storageLocation)
            throws Exception {
        return operator.snapshotState(checkpointId, timestamp, checkpointOptions, storageLocation);
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager)
            throws Exception {
        operator.initializeState(streamTaskStateManager);
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) throws Exception {
        operator.setKeyContextElement1(record);
    }

    @Override
    public void setKeyContextElement2(StreamRecord<?> record) throws Exception {
        operator.setKeyContextElement2(record);
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
        return operator.getMetricGroup();
    }

    @Override
    public OperatorID getOperatorID() {
        return operator.getOperatorID();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        operator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void setCurrentKey(Object key) {
        operator.setCurrentKey(key);
    }

    @Override
    public Object getCurrentKey() {
        return operator.getCurrentKey();
    }

    @Override
    public void endInput() throws Exception {
        Configuration jobConf = operator.getContainingTask().getJobConfiguration();
        TaggedOperatorSubtaskState state =
                SnapshotUtils.snapshot(
                        checkpointId,
                        this,
                        operator.getContainingTask()
                                .getEnvironment()
                                .getTaskInfo()
                                .getIndexOfThisSubtask(),
                        timestamp,
                        CheckpointingOptions.getCheckpointingMode(jobConf),
                        CheckpointingOptions.isUnalignedCheckpointEnabled(jobConf),
                        operator.getContainingTask().getConfiguration().getConfiguration(),
                        savepointPath);

        output.collect(new StreamRecord<>(state));
    }

    private static class VoidOutput<T> implements Output<T> {

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void emitWatermark(WatermarkEvent watermark) {}

        @Override
        public void collect(T record) {}

        @Override
        public void close() {}
    }
}

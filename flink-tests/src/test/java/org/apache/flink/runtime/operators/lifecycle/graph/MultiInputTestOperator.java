/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle.graph;

import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.InputEndedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent.LastReceivedVertexDataInfo;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link MultipleInputStreamOperator} that emits {@link TestEvent}s and reacts to {@link
 * TestCommand}s. It processes the data by passing it downstream replacing the relevant fields.
 */
@SuppressWarnings("rawtypes") // Input type parameter not allowed by interface
class MultiInputTestOperator extends AbstractStreamOperatorV2<TestDataElement>
        implements MultipleInputStreamOperator<TestDataElement>,
                ProcessingTimeCallback,
                BoundedMultiInput {
    private final String operatorId;
    private final List<Input> inputs;
    private final TestEventQueue eventQueue;
    // atomic only to update from different static classes, not for // concurrency
    private final AtomicLong lastDataSent;
    private final Map<String, OperatorFinishedEvent.LastVertexDataInfo> lastDataReceived;

    public MultiInputTestOperator(
            int numInputs,
            StreamOperatorParameters<TestDataElement> params,
            TestEventQueue eventQueue,
            String operatorId) {
        super(params, numInputs);
        this.lastDataReceived = new HashMap<>();
        this.lastDataSent = new AtomicLong(0L);
        this.inputs =
                IntStream.rangeClosed(1, numInputs)
                        .mapToObj(
                                id ->
                                        new TestEventInput(
                                                id,
                                                eventQueue,
                                                output,
                                                operatorId,
                                                getRuntimeContext().getIndexOfThisSubtask(),
                                                getRuntimeContext().getAttemptNumber(),
                                                lastDataReceived,
                                                lastDataSent))
                        .collect(Collectors.toList());
        this.eventQueue = eventQueue;
        this.operatorId = operatorId;
    }

    @Override
    public void open() throws Exception {
        super.open();
        registerTimer();
        this.eventQueue.add(
                new OperatorStartedEvent(
                        operatorId,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber()));
    }

    @Override
    public List<Input> getInputs() {
        return inputs;
    }

    @Override
    public void finish() throws Exception {
        eventQueue.add(
                new OperatorFinishedEvent(
                        operatorId,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        lastDataSent.get(),
                        new LastReceivedVertexDataInfo(lastDataReceived)));
        super.finish();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        eventQueue.add(
                new CheckpointStartedEvent(
                        operatorId,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        context.getCheckpointId()));
        super.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        eventQueue.add(
                new CheckpointCompletedEvent(
                        operatorId,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        checkpointId));
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void onProcessingTime(long timestamp) {
        registerTimer();
    }

    private void registerTimer() {
        getProcessingTimeService()
                .registerTimer(getProcessingTimeService().getCurrentProcessingTime() + 1, this);
    }

    @Override
    public void endInput(int inputId) throws Exception {
        eventQueue.add(
                new InputEndedEvent(
                        operatorId,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        inputId));
    }

    private static class TestEventInput implements Input<TestDataElement> {
        private final int id;
        private final TestEventQueue eventQueue;
        private final Output<StreamRecord<TestDataElement>> output;
        private final String operatorId;
        private final int subtaskIndex;
        private final Map<String, OperatorFinishedEvent.LastVertexDataInfo> lastDataReceived;
        private final AtomicLong lastDataSent;
        private final int attemptNumber;

        public TestEventInput(
                int id,
                TestEventQueue eventQueue,
                Output<StreamRecord<TestDataElement>> output,
                String operatorId,
                int subtaskIndex,
                int attemptNumber,
                Map<String, OperatorFinishedEvent.LastVertexDataInfo> lastDataReceived,
                AtomicLong lastDataSent) {
            this.id = id;
            this.eventQueue = eventQueue;
            this.output = output;
            this.operatorId = operatorId;
            this.subtaskIndex = subtaskIndex;
            this.lastDataReceived = lastDataReceived;
            this.lastDataSent = lastDataSent;
            this.attemptNumber = attemptNumber;
        }

        @Override
        public void processElement(StreamRecord<TestDataElement> element) throws Exception {
            TestDataElement e = element.getValue();
            lastDataReceived
                    .computeIfAbsent(
                            e.operatorId, ign -> new OperatorFinishedEvent.LastVertexDataInfo())
                    .bySubtask
                    .put(e.subtaskIndex, e.seq);
            output.collect(
                    new StreamRecord<>(
                            new TestDataElement(
                                    operatorId, subtaskIndex, lastDataSent.incrementAndGet())));
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            eventQueue.add(
                    new WatermarkReceivedEvent(
                            operatorId, subtaskIndex, attemptNumber, mark.getTimestamp(), id));
            output.emitWatermark(mark);
        }

        @Override
        public void processWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void processLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void setKeyContextElement(StreamRecord<TestDataElement> record) {}
    }
}

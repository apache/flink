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
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link TwoInputStreamOperator} that emits {@link TestEvent}s and reacts to {@link TestCommand}s.
 * It processes the data by passing it downstream replacing the relevant fields.
 */
class TwoInputTestStreamOperator extends AbstractStreamOperator<TestDataElement>
        implements TwoInputStreamOperator<TestDataElement, TestDataElement, TestDataElement>,
                BoundedMultiInput,
                ProcessingTimeCallback {
    private final String operatorID;
    private long lastDataSent;
    private final Map<String, OperatorFinishedEvent.LastVertexDataInfo> lastDataReceived =
            new HashMap<>();
    private boolean timerRegistered;
    private final TestEventQueue eventQueue;

    TwoInputTestStreamOperator(String operatorID, TestEventQueue eventQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.eventQueue.add(
                new OperatorStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber()));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        collectEvent(
                new CheckpointStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        context.getCheckpointId()));
        super.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        collectEvent(
                new CheckpointCompletedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        checkpointId));
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void finish() throws Exception {
        collectEvent(
                new OperatorFinishedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        lastDataSent,
                        new LastReceivedVertexDataInfo(lastDataReceived)));
        super.finish();
    }

    private void processElement(StreamRecord<TestDataElement> element) {
        TestDataElement e = element.getValue();
        lastDataReceived
                .computeIfAbsent(
                        e.operatorId, ign -> new OperatorFinishedEvent.LastVertexDataInfo())
                .bySubtask
                .put(e.subtaskIndex, e.seq);
        output.collect(
                new StreamRecord<>(
                        new TestDataElement(
                                operatorID,
                                getRuntimeContext().getIndexOfThisSubtask(),
                                ++lastDataSent)));
        if (!timerRegistered) {
            registerTimer();
            timerRegistered = true;
        }
    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {
        collectEvent(
                new WatermarkReceivedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        mark.getTimestamp(),
                        1));
        super.processWatermark1(mark);
    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {
        collectEvent(
                new WatermarkReceivedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        mark.getTimestamp(),
                        2));
        super.processWatermark2(mark);
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
        collectEvent(
                new InputEndedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        inputId));
    }

    @Override
    public void processElement1(StreamRecord element) {
        processElement(element);
    }

    @Override
    public void processElement2(StreamRecord element) {
        processElement(element);
    }

    private void collectEvent(TestEvent e) {
        eventQueue.add(e);
    }
}

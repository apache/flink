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

import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.InputEndedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent.LastVertexDataInfo;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestCommandAckEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.DELAY_SNAPSHOT;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL_SNAPSHOT;

/**
 * {@link OneInputStreamOperator} that emits {@link TestEvent}s and reacts to {@link TestCommand}s.
 * It processes the data by passing it downstream replacing the relevant fields.
 */
class OneInputTestStreamOperator extends AbstractStreamOperator<TestDataElement>
        implements OneInputStreamOperator<TestDataElement, TestDataElement>,
                BoundedOneInput,
                ProcessingTimeCallback {
    private final String operatorID;
    private long lastDataSent;
    private final Map<String, LastVertexDataInfo> lastDataReceived = new HashMap<>();
    private boolean timerRegistered;
    private final TestEventQueue eventQueue;
    private final Set<TestCommand> receivedCommands;
    private final TestCommandDispatcher dispatcher;
    private transient ListState<String> state;

    OneInputTestStreamOperator(
            String operatorID, TestEventQueue eventQueue, TestCommandDispatcher dispatcher) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
        this.receivedCommands = new HashSet<>();
        this.dispatcher = dispatcher;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.eventQueue.add(
                new OperatorStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber()));
        this.dispatcher.subscribe(receivedCommands::add, operatorID);
        this.state =
                getKeyedStateBackend() != null
                        ? getRuntimeContext()
                                .getListState(new ListStateDescriptor<>("test", String.class))
                        : getOperatorStateBackend()
                                .getListState(new ListStateDescriptor<>("test", String.class));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        if (receivedCommands.remove(DELAY_SNAPSHOT)) {
            Thread.sleep(10);
        }
        if (receivedCommands.remove(FAIL_SNAPSHOT)) {
            ackAndFail(FAIL_SNAPSHOT);
        }
        eventQueue.add(
                new CheckpointStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        context.getCheckpointId()));
        super.snapshotState(context);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        eventQueue.add(
                new CheckpointCompletedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        checkpointId));
        super.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void finish() throws Exception {
        eventQueue.add(
                new OperatorFinishedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        lastDataSent,
                        new OperatorFinishedEvent.LastReceivedVertexDataInfo(lastDataReceived)));
        super.finish();
    }

    @Override
    public void processElement(StreamRecord<TestDataElement> element) throws Exception {
        if (receivedCommands.remove(FAIL)) {
            ackAndFail(FAIL);
        }
        TestDataElement e = element.getValue();
        state.update(singletonList(String.valueOf(e.seq)));
        lastDataReceived
                .computeIfAbsent(e.operatorId, ign -> new LastVertexDataInfo())
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
    public void processWatermark(Watermark mark) throws Exception {
        eventQueue.add(
                new WatermarkReceivedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        mark.getTimestamp(),
                        1));
        super.processWatermark(mark);
    }

    @Override
    public void endInput() throws Exception {
        eventQueue.add(
                new InputEndedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        1));
    }

    @Override
    public void onProcessingTime(long timestamp) {
        registerTimer();
    }

    private void registerTimer() {
        getProcessingTimeService()
                .registerTimer(getProcessingTimeService().getCurrentProcessingTime() + 1, this);
    }

    private void ack(TestCommand cmd) {
        LOG.info("Executed command: {}", cmd);
        eventQueue.add(
                new TestCommandAckEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        cmd));
    }

    private void ackAndFail(TestCommand failSnapshot) {
        ack(failSnapshot);
        throw new RuntimeException("requested to fail");
    }
}

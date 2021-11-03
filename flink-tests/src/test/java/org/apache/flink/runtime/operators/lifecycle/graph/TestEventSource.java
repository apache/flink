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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent.LastReceivedVertexDataInfo;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestCommandAckEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Collections.emptyMap;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;

/**
 * {@link SourceFunction} that emits {@link TestEvent}s and reacts to {@link TestCommand}s. It emits
 * {@link TestDataElement} to its output.
 */
class TestEventSource extends RichSourceFunction<TestDataElement>
        implements ParallelSourceFunction<TestDataElement> {
    private final String operatorID;
    private final TestCommandDispatcher commandQueue;
    private transient Queue<TestCommand> scheduledCommands;
    private transient volatile boolean isRunning = true;
    private final TestEventQueue eventQueue;

    TestEventSource(
            String operatorID, TestEventQueue eventQueue, TestCommandDispatcher commandQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
        this.commandQueue = commandQueue;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.isRunning = true;
        this.scheduledCommands = new LinkedBlockingQueue<>();
        this.commandQueue.subscribe(cmd -> scheduledCommands.add(cmd), operatorID);
        this.eventQueue.add(
                new OperatorStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber()));
    }

    @Override
    public void run(SourceContext<TestDataElement> ctx) {
        long lastSent = 0;
        while (isRunning) {
            // Don't finish the source if it has not sent at least one value.
            TestCommand cmd = lastSent == 0 ? null : scheduledCommands.poll();
            if (cmd == FINISH_SOURCES) {
                ack(cmd);
                isRunning = false;
            } else if (cmd == FAIL) {
                ack(cmd);
                throw new RuntimeException("requested to fail");
            } else if (cmd == null) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(
                            new TestDataElement(
                                    operatorID,
                                    getRuntimeContext().getIndexOfThisSubtask(),
                                    ++lastSent));
                }
            } else {
                throw new RuntimeException("unknown command " + cmd);
            }
        }
        // note: this only gets collected with FLIP-147 changes
        synchronized (ctx.getCheckpointLock()) {
            eventQueue.add(
                    new OperatorFinishedEvent(
                            operatorID,
                            getRuntimeContext().getIndexOfThisSubtask(),
                            getRuntimeContext().getAttemptNumber(),
                            lastSent,
                            new LastReceivedVertexDataInfo(emptyMap())));
        }
    }

    private void ack(TestCommand cmd) {
        eventQueue.add(
                new TestCommandAckEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber(),
                        cmd));
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

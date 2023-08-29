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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommand;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher.CommandExecutor;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestCommandAckEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FAIL;
import static org.apache.flink.runtime.operators.lifecycle.command.TestCommand.FINISH_SOURCES;

/**
 * {@link SourceFunction} that emits {@link TestEvent}s and reacts to {@link TestCommand}s. It emits
 * {@link TestDataElement} to its output.
 */
public class TestEventSource extends RichSourceFunction<TestDataElement>
        implements ParallelSourceFunction<TestDataElement> {
    private static final Logger LOG = LoggerFactory.getLogger(TestEventSource.class);
    private final String operatorID;
    private final TestCommandDispatcher commandQueue;
    private transient volatile Queue<TestCommand> scheduledCommands;
    private transient volatile boolean isRunning = true;
    private final TestEventQueue eventQueue;
    private transient volatile CommandExecutor commandExecutor;

    public TestEventSource(
            String operatorID, TestEventQueue eventQueue, TestCommandDispatcher commandQueue) {
        this.operatorID = operatorID;
        this.eventQueue = eventQueue;
        this.commandQueue = commandQueue;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        this.isRunning = true;
        this.scheduledCommands = new LinkedBlockingQueue<>();
        this.commandExecutor = cmd -> scheduledCommands.add(cmd);
        this.commandQueue.subscribe(commandExecutor, operatorID);
        this.eventQueue.add(
                new OperatorStartedEvent(
                        operatorID,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getAttemptNumber()));
    }

    @Override
    public void run(SourceContext<TestDataElement> ctx) {
        long lastSent = 0;
        while (isRunning || !scheduledCommands.isEmpty()) {
            // Don't finish the source if it has not sent at least one value.
            TestCommand cmd = lastSent == 0 ? null : scheduledCommands.poll();
            if (cmd == FINISH_SOURCES) {
                ack(cmd);
                stop();
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
        stop();
    }

    private void stop() {
        commandQueue.unsubscribe(operatorID, commandExecutor);
        isRunning = false;
        if (!scheduledCommands.isEmpty()) {
            LOG.info("Unsubscribed with remaining commands: {}", scheduledCommands);
        }
    }
}

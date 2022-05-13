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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;

import java.util.Queue;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test harness for testing a {@link StreamTask}.
 *
 * <p>This mock Invokable provides the task with a basic runtime context and allows pushing elements
 * and watermarks into the task. {@link #getOutput()} can be used to get the emitted elements and
 * events. You are free to modify the retrieved list.
 */
public class StreamTaskMailboxTestHarness<OUT> implements AutoCloseable {
    protected final StreamTask<OUT, ?> streamTask;
    protected final StreamMockEnvironment streamMockEnvironment;
    protected final TestTaskStateManager taskStateManager;
    protected final Queue<Object> outputList;
    protected final StreamTestSingleInputGate[] inputGates;
    protected final boolean[] inputGateEnded;

    private boolean autoProcess = true;

    StreamTaskMailboxTestHarness(
            StreamTask<OUT, ?> streamTask,
            Queue<Object> outputList,
            StreamTestSingleInputGate[] inputGates,
            StreamMockEnvironment streamMockEnvironment) {
        this.streamTask = checkNotNull(streamTask);
        this.taskStateManager = (TestTaskStateManager) streamMockEnvironment.getTaskStateManager();
        this.inputGates = checkNotNull(inputGates);
        this.outputList = checkNotNull(outputList);
        this.streamMockEnvironment = checkNotNull(streamMockEnvironment);
        this.inputGateEnded = new boolean[inputGates.length];
    }

    public TestTaskStateManager getTaskStateManager() {
        return taskStateManager;
    }

    public StreamTask<OUT, ?> getStreamTask() {
        return streamTask;
    }

    public TimerService getTimerService() {
        return streamTask.getTimerService();
    }

    /**
     * Get all the output from the task. This contains StreamRecords and Events interleaved. Use
     * {@link
     * org.apache.flink.streaming.util.TestHarnessUtil#getRawElementsFromOutput(java.util.Queue)}}
     * to extract only the StreamRecords.
     */
    public Queue<Object> getOutput() {
        return outputList;
    }

    @SuppressWarnings("unchecked")
    public void processElement(Object element) throws Exception {
        processElement(element, 0);
    }

    @SuppressWarnings("unchecked")
    public void processElement(Object element, int inputGate) throws Exception {
        processElement(element, inputGate, 0);
    }

    @SuppressWarnings("unchecked")
    public void processElement(Object element, int inputGate, int channel) throws Exception {
        inputGates[inputGate].sendElement(element, channel);
        maybeProcess();
    }

    public void processEvent(AbstractEvent event) throws Exception {
        processEvent(event, 0);
    }

    public void processEvent(AbstractEvent event, int inputGate) throws Exception {
        processEvent(event, inputGate, 0);
    }

    public void processEvent(AbstractEvent event, int inputGate, int channel) throws Exception {
        inputGates[inputGate].sendEvent(event, channel);
        maybeProcess();
    }

    private void maybeProcess() throws Exception {
        if (autoProcess) {
            processAll();
        }
    }

    /** Process until {@code condition} is met. */
    public void processUntil(Supplier<Boolean> condition) throws Exception {
        while (!condition.get()) {
            processAll();
        }
    }

    public void processAll() throws Exception {
        while (processSingleStep()) {}
    }

    public boolean processSingleStep() throws Exception {
        if (streamTask.mailboxProcessor.isMailboxLoopRunning()) {
            return streamTask.runMailboxStep();
        }
        return false;
    }

    public MailboxExecutor getExecutor(int priority) {
        return streamTask.getMailboxExecutorFactory().createExecutor(priority);
    }

    public void endInput() {
        endInput(true);
    }

    public void endInput(boolean allDataProcessed) {
        for (int i = 0; i < inputGates.length; i++) {
            endInput(i, allDataProcessed);
        }
    }

    public void endInput(int inputIndex, boolean emitEndOfData) {
        if (!inputGateEnded[inputIndex]) {
            inputGates[inputIndex].endInput(emitEndOfData);
            inputGateEnded[inputIndex] = true;
        }
    }

    public void waitForTaskCompletion() throws Exception {
        endInput();
        processAll();
    }

    public void finishProcessing() throws Exception {
        streamTask.afterInvoke();
        streamTask.cleanUp(null);
    }

    @Override
    public void close() throws Exception {
        if (streamTask.isRunning()) {
            streamTask.cancel();
            finishProcessing();
        }

        streamMockEnvironment.getIOManager().close();
        MemoryManager memMan = this.streamMockEnvironment.getMemoryManager();
        if (memMan != null) {
            assertTrue(
                    "Memory Manager managed memory was not completely freed.",
                    memMan.verifyEmpty());
            memMan.shutdown();
        }
    }

    public void setAutoProcess(boolean autoProcess) {
        this.autoProcess = autoProcess;
    }

    public TestCheckpointResponder getCheckpointResponder() {
        return (TestCheckpointResponder) taskStateManager.getCheckpointResponder();
    }
}

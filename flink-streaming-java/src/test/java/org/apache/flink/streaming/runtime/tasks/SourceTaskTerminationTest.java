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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Queue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test verifying the termination process (synchronous checkpoint and task termination) at the
 * {@link SourceStreamTask}.
 */
class SourceTaskTerminationTest {

    private static OneShotLatch ready;
    private static MultiShotLatch runLoopStart;
    private static MultiShotLatch runLoopEnd;

    @BeforeEach
    void initialize() {
        ready = new OneShotLatch();
        runLoopStart = new MultiShotLatch();
        runLoopEnd = new MultiShotLatch();
    }

    @Test
    void testStopWithSavepointWithMaxWatermark() throws Exception {
        stopWithSavepointStreamTaskTestHelper(true);
    }

    @Test
    void testStopWithSavepointWithoutMaxWatermark() throws Exception {
        stopWithSavepointStreamTaskTestHelper(false);
    }

    private void stopWithSavepointStreamTaskTestHelper(final boolean shouldTerminate)
            throws Exception {
        final long syncSavepointId = 34L;

        try (StreamTaskMailboxTestHarness<Long> srcTaskTestHarness =
                getSourceStreamTaskTestHarness()) {
            final StreamTask<Long, ?> srcTask = srcTaskTestHarness.getStreamTask();
            srcTaskTestHarness.processAll();

            // step by step let the source thread emit elements
            emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 1L);
            emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 2L);

            srcTaskTestHarness.processUntil(
                    srcTask.triggerCheckpointAsync(
                                    new CheckpointMetaData(31L, 900),
                                    CheckpointOptions.forCheckpointWithDefaultLocation())
                            ::isDone);

            verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), 31L);

            emitAndVerifyWatermarkAndElement(srcTaskTestHarness, 3L);

            srcTaskTestHarness.processUntil(
                    srcTask.triggerCheckpointAsync(
                                    new CheckpointMetaData(syncSavepointId, 900),
                                    new CheckpointOptions(
                                            shouldTerminate
                                                    ? SavepointType.terminate(
                                                            SavepointFormatType.CANONICAL)
                                                    : SavepointType.suspend(
                                                            SavepointFormatType.CANONICAL),
                                            CheckpointStorageLocationReference.getDefault()))
                            ::isDone);

            if (shouldTerminate) {
                // if we are in TERMINATE mode, we expect the source task
                // to emit MAX_WM before the SYNC_SAVEPOINT barrier.
                verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
            }

            verifyEvent(
                    srcTaskTestHarness.getOutput(),
                    new EndOfData(shouldTerminate ? StopMode.DRAIN : StopMode.NO_DRAIN));
            verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

            waitForSynchronousSavepointIdToBeSet(srcTask);

            assertThat(srcTask.getSynchronousSavepointId()).isPresent();

            srcTaskTestHarness.processUntil(
                    srcTask.notifyCheckpointCompleteAsync(syncSavepointId)::isDone);

            srcTaskTestHarness.waitForTaskCompletion();
        }
    }

    private StreamTaskMailboxTestHarness<Long> getSourceStreamTaskTestHarness() throws Exception {
        StreamTaskMailboxTestHarness<Long> testHarness =
                new StreamTaskMailboxTestHarnessBuilder<>(
                                SourceStreamTask::new, BasicTypeInfo.LONG_TYPE_INFO)
                        .setCollectNetworkEvents()
                        .modifyExecutionConfig((config) -> config.setLatencyTrackingInterval(-1))
                        .setupOutputForSingletonOperatorChain(
                                new StreamSource<>(new LockStepSourceWithOneWmPerElement()))
                        .build();
        return testHarness;
    }

    private void waitForSynchronousSavepointIdToBeSet(final StreamTask streamTaskUnderTest)
            throws InterruptedException {
        while (!streamTaskUnderTest.getSynchronousSavepointId().isPresent()) {
            Thread.sleep(10L);
        }
    }

    private void emitAndVerifyWatermarkAndElement(
            final StreamTaskMailboxTestHarness<Long> srcTaskTestHarness, final long expectedElement)
            throws Exception {

        runLoopStart.trigger();
        runLoopEnd.await();
        srcTaskTestHarness.processAll();
        verifyWatermark(srcTaskTestHarness.getOutput(), new Watermark(expectedElement));
        verifyNextElement(srcTaskTestHarness.getOutput(), expectedElement);
    }

    private void verifyNextElement(Queue<Object> output, long expectedElement) {
        Object next = output.remove();
        assertThat(next).as("next element is not an event").isInstanceOf(StreamRecord.class);
        assertThat(((StreamRecord<Long>) next).getValue())
                .as("wrong event")
                .isEqualTo(expectedElement);
    }

    private void verifyWatermark(Queue<Object> output, Watermark expectedWatermark) {
        Object next = output.remove();
        assertThat(next).as("next element is not an event").isInstanceOf(Watermark.class);
        assertThat(next).as("wrong watermark").isEqualTo(expectedWatermark);
    }

    private void verifyEvent(Queue<Object> output, AbstractEvent expectedEvent) {
        Object next = output.remove();
        assertThat(next).isInstanceOf(expectedEvent.getClass()).isEqualTo(expectedEvent);
    }

    private void verifyCheckpointBarrier(Queue<Object> output, long checkpointId) {
        Object next = output.remove();
        assertThat(next)
                .as("next element is not a checkpoint barrier")
                .isInstanceOf(CheckpointBarrier.class);
        assertThat(((CheckpointBarrier) next).getId())
                .as("wrong checkpoint id")
                .isEqualTo(checkpointId);
    }

    private static class LockStepSourceWithOneWmPerElement implements SourceFunction<Long> {

        private volatile boolean isRunning;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            long element = 1L;
            isRunning = true;

            ready.trigger();

            while (isRunning) {
                runLoopStart.await();
                if (isRunning) {
                    ctx.emitWatermark(new Watermark(element));
                    ctx.collect(element++);
                }
                runLoopEnd.trigger();
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
            runLoopStart.trigger();
        }
    }
}

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
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A test verifying the termination process (synchronous checkpoint and task termination) at the
 * {@link SourceStreamTask}.
 */
public class SourceTaskTerminationTest extends TestLogger {

    private static OneShotLatch ready;
    private static MultiShotLatch runLoopStart;
    private static MultiShotLatch runLoopEnd;

    @Before
    public void initialize() {
        ready = new OneShotLatch();
        runLoopStart = new MultiShotLatch();
        runLoopEnd = new MultiShotLatch();
    }

    @Test
    public void testStopWithSavepointWithMaxWatermark() throws Exception {
        stopWithSavepointStreamTaskTestHelper(true);
    }

    @Test
    public void testStopWithSavepointWithoutMaxWatermark() throws Exception {
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
                                                    ? CheckpointType.SAVEPOINT_TERMINATE
                                                    : CheckpointType.SAVEPOINT_SUSPEND,
                                            CheckpointStorageLocationReference.getDefault()))
                            ::isDone);

            if (shouldTerminate) {
                // if we are in TERMINATE mode, we expect the source task
                // to emit MAX_WM before the SYNC_SAVEPOINT barrier.
                verifyWatermark(srcTaskTestHarness.getOutput(), Watermark.MAX_WATERMARK);
                verifyEvent(srcTaskTestHarness.getOutput(), EndOfData.INSTANCE);
            }

            verifyCheckpointBarrier(srcTaskTestHarness.getOutput(), syncSavepointId);

            waitForSynchronousSavepointIdToBeSet(srcTask);

            assertTrue(srcTask.getSynchronousSavepointId().isPresent());

            srcTaskTestHarness.processUntil(
                    srcTask.notifyCheckpointCompleteAsync(syncSavepointId)::isDone);
            if (!shouldTerminate) {
                assertFalse(srcTask.getSynchronousSavepointId().isPresent());
            }

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
        assertTrue("next element is not an event", next instanceof StreamRecord);
        assertEquals(
                "wrong event", expectedElement, ((StreamRecord<Long>) next).getValue().longValue());
    }

    private void verifyWatermark(Queue<Object> output, Watermark expectedWatermark) {
        Object next = output.remove();
        assertTrue("next element is not a watermark", next instanceof Watermark);
        assertEquals("wrong watermark", expectedWatermark, next);
    }

    private void verifyEvent(Queue<Object> output, AbstractEvent expectedEvent) {
        Object next = output.remove();
        assertTrue(expectedEvent.getClass().isInstance(next));
        assertEquals(expectedEvent, next);
    }

    private void verifyCheckpointBarrier(Queue<Object> output, long checkpointId) {
        Object next = output.remove();
        assertTrue("next element is not a checkpoint barrier", next instanceof CheckpointBarrier);
        assertEquals("wrong checkpoint id", checkpointId, ((CheckpointBarrier) next).getId());
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

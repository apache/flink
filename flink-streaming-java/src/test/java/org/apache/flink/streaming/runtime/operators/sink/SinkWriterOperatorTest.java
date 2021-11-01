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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.fromOutput;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test stateful and stateless {@link SinkWriterStateHandler} in {@link SinkOperator}. */
@RunWith(Parameterized.class)
public class SinkWriterOperatorTest extends TestLogger {

    @Parameterized.Parameters(name = "Stateful: {0}")
    public static Collection<Object> data() {
        return Arrays.asList(true, false);
    }

    @Parameterized.Parameter() public boolean stateful;

    @Test
    public void nonBufferingWriterEmitsWithoutFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(new TestSink.DefaultSinkWriter<>());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);

        assertThat(
                fromOutput(testHarness.getOutput()),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void nonBufferingWriterEmitsOnFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(new TestSink.DefaultSinkWriter<>());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.endInput();

        assertThat(
                fromOutput(testHarness.getOutput()),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void bufferingWriterDoesNotEmitWithoutFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(new BufferingSinkWriter());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        testHarness.snapshot(1L, 1L);

        assertThat(fromOutput(testHarness.getOutput()), contains(new Watermark(initialTime)));
    }

    @Test
    public void bufferingWriterEmitsOnFlush() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(new BufferingSinkWriter());
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.endInput();

        assertThat(
                fromOutput(testHarness.getOutput()),
                contains(
                        new Watermark(initialTime),
                        new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()),
                        new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString())));
    }

    @Test
    public void timeBasedBufferingSinkWriter() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(new TimeBasedBufferingSinkWriter());
        testHarness.open();

        testHarness.setProcessingTime(0L);

        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);

        assertThat(testHarness.getOutput().size(), equalTo(0));

        testHarness.getProcessingTimeService().setCurrentTime(2001);

        testHarness.prepareSnapshotPreBarrier(2L);
        testHarness.endInput();

        assertThat(
                fromOutput(testHarness.getOutput()),
                contains(
                        new StreamRecord<>(
                                Tuple3.of(1, initialTime + 1, Long.MIN_VALUE).toString()),
                        new StreamRecord<>(
                                Tuple3.of(2, initialTime + 2, Long.MIN_VALUE).toString())));
    }

    @Test
    public void watermarkPropagatedToSinkWriter() throws Exception {
        final long initialTime = 0;

        final TestSink.DefaultSinkWriter<Integer> writer = new TestSink.DefaultSinkWriter<>();
        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(writer);
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processWatermark(initialTime + 1);

        assertThat(
                fromOutput(testHarness.getOutput()),
                contains(new Watermark(initialTime), new Watermark(initialTime + 1)));
        assertThat(
                writer.watermarks,
                contains(
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime),
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime + 1)));
    }

    @Test
    public void stateIsRestored() throws Exception {
        final long initialTime = 0;

        SnapshottingBufferingSinkWriter snapshottingWriter = new SnapshottingBufferingSinkWriter();
        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(snapshottingWriter);

        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

        // we only see the watermark, so the committables must be stored in state
        assertThat(testHarness.getOutput(), contains(new Watermark(initialTime)));
        assertThat(
                snapshottingWriter.lastCheckpointId,
                equalTo(stateful ? 1L : SnapshottingBufferingSinkWriter.NOT_SNAPSHOTTED));

        testHarness.close();

        final OneInputStreamOperatorTestHarness<Integer, byte[]> restoredTestHarness =
                createTestHarness(new SnapshottingBufferingSinkWriter());

        restoredTestHarness.initializeState(snapshot);
        restoredTestHarness.open();

        // this will flush out the committables that were restored
        restoredTestHarness.endInput();

        List<StreamRecord<String>> expectedOutput = new ArrayList<>();
        if (stateful) {
            // only expect state to be recovered on stateful sink
            expectedOutput.add(
                    new StreamRecord<>(Tuple3.of(1, initialTime + 1, initialTime).toString()));
            expectedOutput.add(
                    new StreamRecord<>(Tuple3.of(2, initialTime + 2, initialTime).toString()));
        }
        assertThat(fromOutput(restoredTestHarness.getOutput()), equalTo(expectedOutput));
    }

    @Test
    public void loadPreviousSinkState() throws Exception {
        // 1. Build previous sink state
        final List<String> previousSinkInputs =
                Arrays.asList(
                        "bit", "mention", "thick", "stick", "stir", "easy", "sleep", "forth",
                        "cost", "prompt");

        final OneInputStreamOperatorTestHarness<String, String> previousSink =
                new OneInputStreamOperatorTestHarness<>(
                        new DummySinkOperator(), StringSerializer.INSTANCE);

        OperatorSubtaskState previousSinkState =
                TestHarnessUtil.buildSubtaskState(previousSink, previousSinkInputs);

        // 2. Load previous sink state and verify the output
        final OneInputStreamOperatorTestHarness<Integer, byte[]> compatibleWriterOperator =
                createCompatibleSinkOperator();

        final List<StreamRecord<String>> expectedOutput1 =
                stateful
                        ? previousSinkInputs.stream()
                                .map(StreamRecord::new)
                                .collect(Collectors.toList())
                        : new ArrayList<>();
        expectedOutput1.add(new StreamRecord<>(Tuple3.of(1, 1, Long.MIN_VALUE).toString()));

        // load the state from previous sink
        compatibleWriterOperator.initializeState(previousSinkState);

        compatibleWriterOperator.open();

        compatibleWriterOperator.processElement(1, 1);

        // this will flush out the committables that were restored from previous sink
        compatibleWriterOperator.endInput();

        OperatorSubtaskState operatorStateWithoutPreviousState =
                compatibleWriterOperator.snapshot(1L, 1L);

        compatibleWriterOperator.close();

        assertThat(
                fromOutput(compatibleWriterOperator.getOutput()),
                containsInAnyOrder(expectedOutput1.toArray()));

        // 3. Restore the sink without previous sink's state
        final OneInputStreamOperatorTestHarness<Integer, byte[]> restoredSinkOperator =
                createCompatibleSinkOperator();
        final List<StreamRecord<String>> expectedOutput2 =
                Arrays.asList(
                        new StreamRecord<>(Tuple3.of(2, 2, Long.MIN_VALUE).toString()),
                        new StreamRecord<>(Tuple3.of(3, 3, Long.MIN_VALUE).toString()));

        restoredSinkOperator.initializeState(operatorStateWithoutPreviousState);

        restoredSinkOperator.open();

        restoredSinkOperator.processElement(2, 2);
        restoredSinkOperator.processElement(3, 3);

        // this will flush out the committables that were restored
        restoredSinkOperator.endInput();

        assertThat(
                fromOutput(restoredSinkOperator.getOutput()),
                containsInAnyOrder(expectedOutput2.toArray()));
    }

    @Test
    public void receivePreCommitWithoutCommitter() throws Exception {
        final long initialTime = 0;

        PreBarrierSinkWriter writer = new PreBarrierSinkWriter();
        final OneInputStreamOperatorTestHarness<Integer, byte[]> testHarness =
                createTestHarness(writer, false);
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        // Expect that preCommit was called
        assertTrue(writer.hasReceivedPreCommit());
        testHarness.snapshot(1L, 1L);

        assertThat(
                writer.getElements(),
                contains(
                        Tuple3.of(1, initialTime + 1, initialTime).toString(),
                        Tuple3.of(2, initialTime + 2, initialTime).toString()));

        assertThat(
                writer.getWatermarks(),
                contains(new org.apache.flink.api.common.eventtime.Watermark(initialTime)));
    }

    private static class PreBarrierSinkWriter extends TestSink.DefaultSinkWriter<Integer> {

        private boolean receivedPreCommit = false;

        @Override
        public List<String> prepareCommit(boolean flush) {
            receivedPreCommit = true;
            return Collections.emptyList();
        }

        public boolean hasReceivedPreCommit() {
            return receivedPreCommit;
        }

        public List<org.apache.flink.api.common.eventtime.Watermark> getWatermarks() {
            return watermarks;
        }

        public List<String> getElements() {
            return elements;
        }
    }

    /** A {@link SinkWriter} buffers elements and snapshots them when asked. */
    private static class SnapshottingBufferingSinkWriter extends BufferingSinkWriter {
        public static final int NOT_SNAPSHOTTED = -1;
        long lastCheckpointId = NOT_SNAPSHOTTED;

        @Override
        public List<String> snapshotState(long checkpointId) throws IOException {
            lastCheckpointId = checkpointId;
            return elements;
        }

        @Override
        void restoredFrom(List<String> states) {
            this.elements = new ArrayList<>(states);
        }
    }

    private static class DummySinkOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        static final String DUMMY_SINK_STATE_NAME = "dummy_sink_state";

        static final ListStateDescriptor<byte[]> SINK_STATE_DESC =
                new ListStateDescriptor<>(
                        DUMMY_SINK_STATE_NAME, BytePrimitiveArraySerializer.INSTANCE);
        ListState<String> sinkState;

        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            sinkState =
                    new SimpleVersionedListState<>(
                            context.getOperatorStateStore().getListState(SINK_STATE_DESC),
                            TestSink.StringCommittableSerializer.INSTANCE);
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            sinkState.add(element.getValue());
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, byte[]> createCompatibleSinkOperator()
            throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new SinkOperatorFactory<>(
                        getBuilder(new SnapshottingBufferingSinkWriter())
                                .setCompatibleStateNames(DummySinkOperator.DUMMY_SINK_STATE_NAME)
                                .build(),
                        false,
                        true),
                IntSerializer.INSTANCE);
    }

    /**
     * A {@link SinkWriter} that only returns committables from {@link #prepareCommit(boolean)} when
     * {@code flush} is {@code true}.
     */
    private static class BufferingSinkWriter extends TestSink.DefaultSinkWriter<Integer> {
        @Override
        public List<String> prepareCommit(boolean flush) {
            if (!flush) {
                return Collections.emptyList();
            }
            List<String> result = elements;
            elements = new ArrayList<>();
            return result;
        }
    }

    /**
     * A {@link SinkWriter} that buffers the committables and send the cached committables per
     * second.
     */
    private static class TimeBasedBufferingSinkWriter extends TestSink.DefaultSinkWriter<Integer>
            implements Sink.ProcessingTimeService.ProcessingTimeCallback {

        private final List<String> cachedCommittables = new ArrayList<>();

        @Override
        public void write(Integer element, Context context) {
            cachedCommittables.add(
                    Tuple3.of(element, context.timestamp(), context.currentWatermark()).toString());
        }

        void setProcessingTimerService(Sink.ProcessingTimeService processingTimerService) {
            super.setProcessingTimerService(processingTimerService);
            this.processingTimerService.registerProcessingTimer(1000, this);
        }

        @Override
        public void onProcessingTime(long time) throws IOException {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimerService.registerProcessingTimer(time + 1000, this);
        }
    }

    private OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(
            TestSink.DefaultSinkWriter<Integer> writer) throws Exception {
        return createTestHarness(writer, true);
    }

    private OneInputStreamOperatorTestHarness<Integer, byte[]> createTestHarness(
            TestSink.DefaultSinkWriter<Integer> writer, boolean withCommitter) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new SinkOperatorFactory<>(getBuilder(writer).build(), false, withCommitter),
                IntSerializer.INSTANCE);
    }

    private TestSink.Builder<Integer> getBuilder(TestSink.DefaultSinkWriter<Integer> writer) {
        TestSink.Builder<Integer> builder =
                TestSink.newBuilder()
                        .setWriter(writer)
                        .setCommittableSerializer(TestSink.StringCommittableSerializer.INSTANCE);
        if (stateful) {
            builder.withWriterState();
        }
        return builder;
    }
}

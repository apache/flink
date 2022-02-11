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
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.fromOutput;
import static org.assertj.core.api.Assertions.assertThat;

class SinkWriterOperatorTest {

    @Test
    void testNotEmitCommittablesWithoutCommitter() throws Exception {
        final TestSink.DefaultSinkWriter<Integer> sinkWriter = new TestSink.DefaultSinkWriter<>();
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new SinkWriterOperatorFactory<>(
                                TestSink.newBuilder().setWriter(sinkWriter).build().asV2()));
        testHarness.open();
        testHarness.processElement(1, 1);

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(sinkWriter.elements).containsOnly("(1,1," + Long.MIN_VALUE + ")");

        testHarness.prepareSnapshotPreBarrier(1);
        assertThat(testHarness.getOutput()).isEmpty();
        // Elements are flushed
        assertThat(sinkWriter.elements).isEmpty();
        testHarness.close();
    }

    @Test
    void testWatermarkPropagatedToSinkWriter() throws Exception {
        final long initialTime = 0;

        final TestSink.DefaultSinkWriter<Integer> writer = new TestSink.DefaultSinkWriter<>();
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new SinkWriterOperatorFactory<>(
                                TestSink.newBuilder().setWriter(writer).build().asV2()));
        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processWatermark(initialTime + 1);

        assertThat(testHarness.getOutput())
                .containsExactly(new Watermark(initialTime), new Watermark(initialTime + 1));
        assertThat(writer.watermarks)
                .containsExactly(
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime),
                        new org.apache.flink.api.common.eventtime.Watermark(initialTime + 1));
        testHarness.close();
    }

    @Test
    public void testTimeBasedBufferingSinkWriter() throws Exception {
        final long initialTime = 0;

        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new SinkWriterOperatorFactory<>(
                                TestSink.newBuilder()
                                        .setDefaultCommitter()
                                        .setWriter(new TimeBasedBufferingSinkWriter())
                                        .build()
                                        .asV2()));

        testHarness.open();

        testHarness.setProcessingTime(0L);

        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);

        // Expect empty committableSummary
        assertBasicOutput(testHarness.getOutput(), 0, 1L);
        testHarness.getOutput().poll();

        testHarness.getProcessingTimeService().setCurrentTime(2001);

        testHarness.prepareSnapshotPreBarrier(2L);

        assertBasicOutput(testHarness.getOutput(), 2, 2L);
        testHarness.close();
    }

    @Test
    void testEmitOnFlushWithCommitter() throws Exception {
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(
                        new SinkWriterOperatorFactory<>(
                                TestSink.newBuilder().setDefaultCommitter().build().asV2()));

        testHarness.open();
        assertThat(testHarness.getOutput()).isEmpty();

        testHarness.processElement(1, 1);
        testHarness.processElement(2, 2);

        // flush
        testHarness.prepareSnapshotPreBarrier(1);

        assertBasicOutput(testHarness.getOutput(), 2, 1L);
        testHarness.close();
    }

    @Test
    void testEmitOnEndOfInputInBatchMode() throws Exception {
        final SinkWriterOperatorFactory<Integer, Integer> writerOperatorFactory =
                new SinkWriterOperatorFactory<>(
                        TestSink.newBuilder().setDefaultCommitter().build().asV2());
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(writerOperatorFactory);

        testHarness.open();
        assertThat(testHarness.getOutput()).isEmpty();

        testHarness.processElement(1, 1);
        testHarness.endInput();
        assertBasicOutput(testHarness.getOutput(), 1, Long.MAX_VALUE);

        // Not flush new records during snapshot barrier
        testHarness.processElement(2, 2);
        testHarness.prepareSnapshotPreBarrier(1);
        assertThat(testHarness.getOutput()).hasSize(2);
        testHarness.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStateRestore(boolean stateful) throws Exception {

        final long initialTime = 0;

        final SnapshottingBufferingSinkWriter snapshottingWriter =
                new SnapshottingBufferingSinkWriter();
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                createTestHarnessWithBufferingSinkWriter(snapshottingWriter, stateful);

        testHarness.open();

        testHarness.processWatermark(initialTime);
        testHarness.processElement(1, initialTime + 1);
        testHarness.processElement(2, initialTime + 2);

        testHarness.prepareSnapshotPreBarrier(1L);
        OperatorSubtaskState snapshot = testHarness.snapshot(1L, 1L);

        // we see the watermark and the committable summary, so the committables must be stored in
        // state
        assertThat(testHarness.getOutput()).hasSize(2).contains(new Watermark(initialTime));
        assertThat(snapshottingWriter.lastCheckpointId)
                .isEqualTo(stateful ? 1L : SnapshottingBufferingSinkWriter.NOT_SNAPSHOTTED);

        testHarness.close();

        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                restoredTestHarness =
                        createTestHarnessWithBufferingSinkWriter(
                                new SnapshottingBufferingSinkWriter(), stateful);

        restoredTestHarness.initializeState(snapshot);
        restoredTestHarness.open();

        // this will flush out the committables that were restored
        restoredTestHarness.endInput();

        if (stateful) {
            assertBasicOutput(restoredTestHarness.getOutput(), 2, Long.MAX_VALUE);
        } else {
            assertThat(fromOutput(restoredTestHarness.getOutput()).get(0).asRecord().getValue())
                    .isInstanceOf(CommittableSummary.class)
                    .satisfies(
                            cs ->
                                    SinkV2Assertions.assertThat((CommittableSummary<?>) cs)
                                            .hasOverallCommittables(0)
                                            .hasPendingCommittables(0)
                                            .hasFailedCommittables(0));
        }
        restoredTestHarness.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLoadPreviousSinkState(boolean stateful) throws Exception {
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
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                compatibleWriterOperator = createCompatibleStateTestHarness(stateful);

        final List<String> expectedOutput1 =
                stateful ? new ArrayList<>(previousSinkInputs) : new ArrayList<>();
        expectedOutput1.add(Tuple3.of(1, 1, Long.MIN_VALUE).toString());

        // load the state from previous sink
        compatibleWriterOperator.initializeState(previousSinkState);

        compatibleWriterOperator.open();

        compatibleWriterOperator.processElement(1, 1);

        // this will flush out the committables that were restored from previous sink
        compatibleWriterOperator.endInput();

        OperatorSubtaskState operatorStateWithoutPreviousState =
                compatibleWriterOperator.snapshot(1L, 1L);

        compatibleWriterOperator.close();

        assertEmitted(expectedOutput1, compatibleWriterOperator.getOutput());

        // 3. Restore the sink without previous sink's state
        final OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                restoredSinkOperator = createCompatibleStateTestHarness(stateful);
        final List<String> expectedOutput2 =
                Arrays.asList(
                        Tuple3.of(2, 2, Long.MIN_VALUE).toString(),
                        Tuple3.of(3, 3, Long.MIN_VALUE).toString());

        restoredSinkOperator.initializeState(operatorStateWithoutPreviousState);

        restoredSinkOperator.open();

        restoredSinkOperator.processElement(2, 2);
        restoredSinkOperator.processElement(3, 3);

        // this will flush out the committables that were restored
        restoredSinkOperator.endInput();

        assertEmitted(expectedOutput2, restoredSinkOperator.getOutput());
        restoredSinkOperator.close();
    }

    @SuppressWarnings("unchecked")
    private static void assertEmitted(List<String> records, Queue<Object> output) {

        final List<StreamElement> collected = fromOutput(output);
        assertThat(collected).hasSize(records.size() + 1);
        assertThat(collected.get(0).asRecord().getValue())
                .isInstanceOf(CommittableSummary.class)
                .satisfies(
                        cs ->
                                SinkV2Assertions.assertThat(((CommittableSummary<?>) cs))
                                        .hasPendingCommittables(records.size())
                                        .hasOverallCommittables(records.size())
                                        .hasFailedCommittables(0));

        final List<String> committables = new ArrayList<>();

        for (int i = 1; i <= records.size(); i++) {
            Object value = collected.get(i).asRecord().getValue();
            assertThat(value).isInstanceOf(CommittableWithLineage.class);
            committables.add(((CommittableWithLineage<String>) value).getCommittable());
        }
        assertThat(committables).containsExactlyInAnyOrderElementsOf(records);
    }

    private static OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
            createTestHarnessWithBufferingSinkWriter(
                    SnapshottingBufferingSinkWriter snapshottingWriter, boolean stateful)
                    throws Exception {
        final TestSink.Builder<Integer> builder =
                TestSink.newBuilder().setDefaultCommitter().setWriter(snapshottingWriter);
        if (stateful) {
            builder.withWriterState();
        }
        final SinkWriterOperatorFactory<Integer, Integer> writerOperatorFactory =
                new SinkWriterOperatorFactory<>(builder.build().asV2());
        return new OneInputStreamOperatorTestHarness<>(writerOperatorFactory);
    }

    private static OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
            createCompatibleStateTestHarness(boolean stateful) throws Exception {
        final SnapshottingBufferingSinkWriter snapshottingWriter =
                new SnapshottingBufferingSinkWriter();
        final TestSink.Builder<Integer> builder =
                TestSink.newBuilder()
                        .setDefaultCommitter()
                        .setCompatibleStateNames(DummySinkOperator.DUMMY_SINK_STATE_NAME)
                        .setWriter(snapshottingWriter);
        if (stateful) {
            builder.withWriterState();
        }
        final SinkWriterOperatorFactory<Integer, Integer> writerOperatorFactory =
                new SinkWriterOperatorFactory<>(builder.build().asV2());
        return new OneInputStreamOperatorTestHarness<>(writerOperatorFactory);
    }

    private static void assertBasicOutput(
            Collection<Object> queuedOutput,
            int numberOfCommittables,
            @Nullable Long checkpointId) {
        List<StreamElement> output = fromOutput(queuedOutput);
        assertThat(output).hasSize(numberOfCommittables + 1);
        assertThat(output.get(0).asRecord().getValue())
                .isInstanceOf(CommittableSummary.class)
                .satisfies(
                        cs ->
                                SinkV2Assertions.assertThat((CommittableSummary<?>) cs)
                                        .hasOverallCommittables(numberOfCommittables)
                                        .hasPendingCommittables(numberOfCommittables)
                                        .hasFailedCommittables(0));
        for (int i = 1; i <= numberOfCommittables; i++) {
            assertThat(output.get(i).asRecord().getValue())
                    .isInstanceOf(CommittableWithLineage.class)
                    .satisfies(
                            cl ->
                                    SinkV2Assertions.assertThat((CommittableWithLineage<?>) cl)
                                            .hasCheckpointId(checkpointId)
                                            .hasSubtaskId(0));
        }
    }

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
        public void onProcessingTime(long time) {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimerService.registerProcessingTimer(time + 1000, this);
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

    private static class SnapshottingBufferingSinkWriter
            extends TestSink.DefaultSinkWriter<Integer> {
        public static final int NOT_SNAPSHOTTED = -1;
        long lastCheckpointId = NOT_SNAPSHOTTED;

        @Override
        public List<String> snapshotState(long checkpointId) {
            lastCheckpointId = checkpointId;
            return elements;
        }

        @Override
        void restoredFrom(List<String> states) {
            this.elements = new ArrayList<>(states);
        }

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
}

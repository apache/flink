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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineageAssert;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.DefaultCommitter;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.DefaultCommittingSinkWriter;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.DefaultSinkWriter;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.DefaultStatefulSinkWriter;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.Record;
import org.apache.flink.streaming.runtime.operators.sink.TestSinkV2.RecordSerializer;
import org.apache.flink.streaming.runtime.operators.sink.committables.SinkV1CommittableDeserializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.flink.api.connector.sink2.InitContext.INITIAL_CHECKPOINT_ID;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableSummary;
import static org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions.committableWithLineage;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;

class SinkV2SinkWriterOperatorTest {
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testLoadPreviousSinkState(boolean stateful) throws Exception {
        // 1. Build previous sink state
        final List<String> previousSinkInputs =
                Arrays.asList(
                        "bit", "mention", "thick", "stick", "stir", "easy", "sleep", "forth",
                        "cost", "prompt");

        DefaultStatefulSinkWriter<Integer> writer = new DefaultStatefulSinkWriter<>();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWithPostCommitTopology(true)
                        .setWriter(writer)
                        .setWriterState(stateful)
                        .setCompatibleStateNames(CompatibleStateSinkOperator.SINK_STATE_NAME)
                        .build();
        int expectedState = 5;
        OperatorSubtaskState previousSinkState;
        try (OneInputStreamOperatorTestHarness<String, String> previousSink =
                new OneInputStreamOperatorTestHarness<>(
                        new CompatibleStateSinkOperator<>(
                                TestSinkV2.WRITER_SERIALIZER, expectedState),
                        StringSerializer.INSTANCE)) {

            previousSinkState = TestHarnessUtil.buildSubtaskState(previousSink, previousSinkInputs);
        }

        // 2. Load previous sink state and verify state
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                compatibleWriterOperator =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(sink))) {

            // load the state from previous sink
            compatibleWriterOperator.initializeState(previousSinkState);
            assertThat(writer.getRecordCount()).isEqualTo(stateful ? expectedState : 0);

            // 3. do another snapshot and check if this also can be restored without compabitible
            // state
            // name
            compatibleWriterOperator.prepareSnapshotPreBarrier(1L);
            snapshot = compatibleWriterOperator.snapshot(1L, 1L);
        }

        // 4. Restore the sink without previous sink's state
        DefaultStatefulSinkWriter<Integer> restoredWriter = new DefaultStatefulSinkWriter<>();
        TestSinkV2<Integer> restoredSink =
                TestSinkV2.newBuilder()
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWithPostCommitTopology(true)
                        .setWriter(restoredWriter)
                        .setWriterState(stateful)
                        .setCompatibleStateNames(CompatibleStateSinkOperator.SINK_STATE_NAME)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                restoredSinkOperator =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(restoredSink))) {

            restoredSinkOperator.initializeState(snapshot);
            assertThat(restoredWriter.getRecordCount()).isEqualTo(stateful ? expectedState : 0);
        }
    }

    @Test
    void testNotEmitCommittablesWithoutCommitter() throws Exception {
        DefaultSinkWriter<Integer> writer = new DefaultSinkWriter<>();
        TestSinkV2<Integer> sink = TestSinkV2.newBuilder().setWriter(writer).build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {
            testHarness.open();
            testHarness.processElement(1, 1);

            assertThat(testHarness.extractOutputValues()).isEmpty();
            assertThat(writer.getRecordsOfCurrentCheckpoint())
                    .containsOnly(new Record<>(1, 1L, Long.MIN_VALUE));

            testHarness.prepareSnapshotPreBarrier(1);
            assertThat(testHarness.extractOutputValues()).isEmpty();
            // Elements are flushed
            assertThat(writer.getRecordsOfCurrentCheckpoint()).isEmpty();
        }
    }

    @Test
    void testWatermarkPropagatedToSinkWriter() throws Exception {
        final long initialTime = 0;

        DefaultSinkWriter<Integer> writer = new DefaultSinkWriter<>();
        TestSinkV2<Integer> sink = TestSinkV2.newBuilder().setWriter(writer).build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {
            testHarness.open();

            testHarness.processWatermark(initialTime);
            testHarness.processWatermark(initialTime + 1);

            assertThat(testHarness.getOutput())
                    .containsExactly(
                            new org.apache.flink.streaming.api.watermark.Watermark(initialTime),
                            new org.apache.flink.streaming.api.watermark.Watermark(
                                    initialTime + 1));
            assertThat(writer.getWatermarks())
                    .containsExactly(new Watermark(initialTime), new Watermark(initialTime + 1));
        }
    }

    @Test
    void testTimeBasedBufferingSinkWriter() throws Exception {
        final long initialTime = 0;

        DefaultSinkWriter<Integer> writer = new TimeBasedBufferingSinkWriter();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(writer)
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {

            testHarness.open();

            testHarness.setProcessingTime(0L);

            testHarness.processElement(1, initialTime + 1);
            testHarness.processElement(2, initialTime + 2);

            testHarness.prepareSnapshotPreBarrier(1L);

            // Expect empty committableSummary
            assertBasicOutput(testHarness.extractOutputValues(), 0, 1L);

            testHarness.getProcessingTimeService().setCurrentTime(2001);

            testHarness.prepareSnapshotPreBarrier(2L);

            assertBasicOutput(
                    testHarness.extractOutputValues().stream().skip(1).collect(Collectors.toList()),
                    2,
                    2L);
        }
    }

    @Test
    void testEmitOnFlushWithCommitter() throws Exception {
        DefaultSinkWriter<Integer> writer = new DefaultCommittingSinkWriter<>();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(writer)
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {

            testHarness.open();
            assertThat(testHarness.extractOutputValues()).isEmpty();

            testHarness.processElement(1, 1);
            testHarness.processElement(2, 2);

            // flush
            testHarness.prepareSnapshotPreBarrier(1);

            assertBasicOutput(testHarness.extractOutputValues(), 2, 1L);
        }
    }

    @Test
    void testEmitOnEndOfInputInBatchMode() throws Exception {
        DefaultSinkWriter<Integer> writer = new DefaultCommittingSinkWriter<>();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(writer)
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .build();
        final SinkWriterOperatorFactory<Integer, Integer> writerOperatorFactory =
                new SinkWriterOperatorFactory<>(sink);
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(writerOperatorFactory)) {

            testHarness.open();
            assertThat(testHarness.extractOutputValues()).isEmpty();

            testHarness.processElement(1, 1);
            testHarness.endInput();
            assertBasicOutput(testHarness.extractOutputValues(), 1, 1L);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testStateRestore(boolean stateful) throws Exception {

        final long initialTime = 0;

        DefaultStatefulSinkWriter<Integer> writer = new DefaultStatefulSinkWriter<>();
        final TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWithPostCommitTopology(true)
                        .setWriter(writer)
                        .setWriterState(stateful)
                        .build();
        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {

            testHarness.open();

            testHarness.processWatermark(initialTime);
            testHarness.processElement(1, initialTime + 1);
            testHarness.processElement(2, initialTime + 2);

            testHarness.prepareSnapshotPreBarrier(1L);
            snapshot = testHarness.snapshot(1L, 1L);

            assertThat(writer.getRecordCount()).isEqualTo(2);
            assertThat(writer.getLastCheckpointId()).isEqualTo(stateful ? 1L : -1L);
        }

        DefaultStatefulSinkWriter<Integer> restoredWriter = new DefaultStatefulSinkWriter<>();
        final TestSinkV2<Integer> restoredSink =
                TestSinkV2.newBuilder()
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWithPostCommitTopology(true)
                        .setWriter(restoredWriter)
                        .setWriterState(stateful)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                restoredTestHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(restoredSink))) {

            restoredTestHarness.initializeState(snapshot);
            restoredTestHarness.open();

            // check that the previous state is correctly restored
            assertThat(restoredWriter.getRecordCount()).isEqualTo(stateful ? 2 : 0);
        }
    }

    @Test
    void testRestoreCommitterState() throws Exception {
        final List<Record<Integer>> committables =
                Arrays.asList(new Record<>(1, 1L, 1), new Record<>(2, 2L, 2));

        DefaultSinkWriter<Integer> writer = new DefaultCommittingSinkWriter<>();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(writer)
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .build();
        final OperatorSubtaskState committerState;
        try (OneInputStreamOperatorTestHarness<Record<Integer>, Record<Integer>> committer =
                new OneInputStreamOperatorTestHarness<>(
                        new TestCommitterOperator(new RecordSerializer<>()))) {

            committerState = TestHarnessUtil.buildSubtaskState(committer, committables);
        }

        final ListAssert<CommittableMessage<Integer>> records;
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>> testHarness =
                new OneInputStreamOperatorTestHarness<>(new SinkWriterOperatorFactory<>(sink))) {

            testHarness.initializeState(committerState);

            testHarness.open();

            testHarness.prepareSnapshotPreBarrier(2);

            records = assertThat(testHarness.extractOutputValues()).hasSize(4);
        }

        records.element(0, as(committableSummary()))
                .hasCheckpointId(INITIAL_CHECKPOINT_ID)
                .hasOverallCommittables(committables.size());
        records.<CommittableWithLineageAssert<Record<Integer>>>element(
                        1, as(committableWithLineage()))
                .hasCommittable(committables.get(0))
                .hasCheckpointId(INITIAL_CHECKPOINT_ID)
                .hasSubtaskId(0);
        records.<CommittableWithLineageAssert<Record<Integer>>>element(
                        2, as(committableWithLineage()))
                .hasCommittable(committables.get(1))
                .hasCheckpointId(INITIAL_CHECKPOINT_ID)
                .hasSubtaskId(0);
        records.element(3, as(committableSummary())).hasCheckpointId(2L).hasOverallCommittables(0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHandleEndInputInStreamingMode(boolean isCheckpointingEnabled) throws Exception {
        DefaultSinkWriter<Integer> writer = new DefaultCommittingSinkWriter<>();
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(writer)
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Record<Integer>>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(sink))) {
            testHarness.open();
            testHarness.processElement(1, 1);

            assertThat(testHarness.extractOutputValues()).isEmpty();
            Record<Integer> record = new Record<>(1, 1L, Long.MIN_VALUE);
            assertThat(writer.getRecordsOfCurrentCheckpoint()).containsOnly(record);

            testHarness.endInput();

            if (isCheckpointingEnabled) {
                testHarness.prepareSnapshotPreBarrier(1);
            }

            List<Record<Integer>> committables = Collections.singletonList(record);

            ListAssert<CommittableMessage<Record<Integer>>> records =
                    assertThat(testHarness.extractOutputValues()).hasSize(committables.size() + 1);
            records.element(0, as(committableSummary()))
                    .hasOverallCommittables(committables.size());

            records.filteredOn(message -> message instanceof CommittableWithLineage)
                    .map(
                            message ->
                                    ((CommittableWithLineage<Record<Integer>>) message)
                                            .getCommittable())
                    .containsExactlyInAnyOrderElementsOf(committables);
            assertThat(writer.getRecordsOfCurrentCheckpoint()).isEmpty();
        }
    }

    @Test
    void testDoubleEndOfInput() throws Exception {
        TestSinkV2<Integer> sink =
                TestSinkV2.newBuilder()
                        .setWriter(new DefaultCommittingSinkWriter<Integer>())
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWriterState(true)
                        .build();

        OperatorSubtaskState snapshot;
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Record<Integer>>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(sink))) {
            testHarness.open();
            testHarness.processElement(1, 1);

            testHarness.endInput();
            testHarness.prepareSnapshotPreBarrier(1);
            snapshot = testHarness.snapshot(1, 1);

            assertBasicOutput(testHarness.extractOutputValues(), 1, 1L);
        }

        final TestSinkV2<Integer> restoredSink =
                TestSinkV2.newBuilder()
                        .setCommitter(new DefaultCommitter<>(), RecordSerializer::new)
                        .setWriter(new DefaultStatefulSinkWriter<Integer>())
                        .setWriterState(true)
                        .build();
        try (OneInputStreamOperatorTestHarness<Integer, CommittableMessage<Integer>>
                restoredTestHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(restoredSink))) {
            restoredTestHarness.setRestoredCheckpointId(1L);
            restoredTestHarness.initializeState(snapshot);
            restoredTestHarness.open();
            restoredTestHarness.processElement(2, 2);

            restoredTestHarness.endInput();
            restoredTestHarness.prepareSnapshotPreBarrier(3);
            restoredTestHarness.snapshot(3, 1);

            // asserts the guessed checkpoint id which needs
            assertBasicOutput(restoredTestHarness.extractOutputValues(), 1, 2L);
        }
    }

    @Test
    void testInitContext() throws Exception {
        final AtomicReference<WriterInitContext> initContext = new AtomicReference<>();
        final Sink<String> sink =
                context -> {
                    initContext.set(context);
                    return null;
                };

        final int subtaskId = 1;
        final int parallelism = 10;
        final TypeSerializer<String> typeSerializer = StringSerializer.INSTANCE;
        final JobID jobID = new JobID();

        final MockEnvironment environment =
                MockEnvironment.builder()
                        .setSubtaskIndex(subtaskId)
                        .setParallelism(parallelism)
                        .setMaxParallelism(parallelism)
                        .setJobID(jobID)
                        .setExecutionConfig(new ExecutionConfig().enableObjectReuse())
                        .build();

        try (OneInputStreamOperatorTestHarness<String, CommittableMessage<Record<Integer>>>
                testHarness =
                        new OneInputStreamOperatorTestHarness<>(
                                new SinkWriterOperatorFactory<>(sink),
                                typeSerializer,
                                environment)) {
            testHarness.open();

            assertThat(initContext.get().getUserCodeClassLoader()).isNotNull();
            assertThat(initContext.get().getMailboxExecutor()).isNotNull();
            assertThat(initContext.get().getProcessingTimeService()).isNotNull();
            assertThat(initContext.get().getTaskInfo().getIndexOfThisSubtask())
                    .isEqualTo(subtaskId);
            assertThat(initContext.get().getTaskInfo().getNumberOfParallelSubtasks())
                    .isEqualTo(parallelism);
            assertThat(initContext.get().getTaskInfo().getAttemptNumber()).isZero();
            assertThat(initContext.get().metricGroup()).isNotNull();
            assertThat(initContext.get().getRestoredCheckpointId()).isNotPresent();
            assertThat(initContext.get().isObjectReuseEnabled()).isTrue();
            assertThat(initContext.get().createInputSerializer()).isEqualTo(typeSerializer);
            assertThat(initContext.get().getJobInfo().getJobId()).isEqualTo(jobID);
        }
    }

    private static void assertBasicOutput(
            List<? extends CommittableMessage<?>> output,
            int numberOfCommittables,
            long checkpointId) {
        ListAssert<? extends CommittableMessage<?>> records =
                assertThat(output).hasSize(numberOfCommittables + 1);
        records.element(0, as(committableSummary())).hasOverallCommittables(numberOfCommittables);
        records.filteredOn(r -> r instanceof CommittableWithLineage)
                .allSatisfy(
                        cl ->
                                SinkV2Assertions.assertThat((CommittableWithLineage<?>) cl)
                                        .hasCheckpointId(checkpointId)
                                        .hasSubtaskId(0));
    }

    private static class TimeBasedBufferingSinkWriter extends DefaultCommittingSinkWriter<Integer>
            implements ProcessingTimeService.ProcessingTimeCallback {

        private final List<Record<Integer>> cachedCommittables = new ArrayList<>();
        private ProcessingTimeService processingTimeService;

        @Override
        public void write(Integer element, Context context) {
            cachedCommittables.add(
                    new Record<>(element, context.timestamp(), context.currentWatermark()));
        }

        @Override
        public void onProcessingTime(long time) {
            elements.addAll(cachedCommittables);
            cachedCommittables.clear();
            this.processingTimeService.registerTimer(time + 1000, this);
        }

        @Override
        public void init(WriterInitContext context) {
            this.processingTimeService = context.getProcessingTimeService();
            this.processingTimeService.registerTimer(1000, this);
        }
    }

    private static class TestCommitterOperator extends AbstractStreamOperator<Record<Integer>>
            implements OneInputStreamOperator<Record<Integer>, Record<Integer>> {

        private static final ListStateDescriptor<byte[]> STREAMING_COMMITTER_RAW_STATES_DESC =
                new ListStateDescriptor<>(
                        "streaming_committer_raw_states", BytePrimitiveArraySerializer.INSTANCE);
        private ListState<List<Record<Integer>>> committerState;
        private final List<Record<Integer>> buffer = new ArrayList<>();
        private final SimpleVersionedSerializer<Record<Integer>> serializer;

        public TestCommitterOperator(SimpleVersionedSerializer<Record<Integer>> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            committerState =
                    new SimpleVersionedListState<>(
                            context.getOperatorStateStore()
                                    .getListState(STREAMING_COMMITTER_RAW_STATES_DESC),
                            new TestingCommittableSerializer(serializer));
        }

        @Override
        public void processElement(StreamRecord<Record<Integer>> element) {
            buffer.add(element.getValue());
        }

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            committerState.add(buffer);
        }
    }

    /** Writes state to test whether the sink can read from alternative state names. */
    private static class CompatibleStateSinkOperator<T> extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {

        static final String SINK_STATE_NAME = "compatible_sink_state";

        static final ListStateDescriptor<byte[]> SINK_STATE_DESC =
                new ListStateDescriptor<>(SINK_STATE_NAME, BytePrimitiveArraySerializer.INSTANCE);
        ListState<T> sinkState;
        private final SimpleVersionedSerializer<T> serializer;
        private final T initialState;

        public CompatibleStateSinkOperator(
                SimpleVersionedSerializer<T> serializer, T initialState) {
            this.serializer = serializer;
            this.initialState = initialState;
        }

        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            sinkState =
                    new SimpleVersionedListState<>(
                            context.getOperatorStateStore().getListState(SINK_STATE_DESC),
                            serializer);
            if (!context.isRestored()) {
                sinkState.add(initialState);
            }
        }

        @Override
        public void processElement(StreamRecord<String> element) {
            // do nothing
        }
    }

    private static class TestingCommittableSerializer
            extends SinkV1WriterCommittableSerializer<Record<Integer>> {

        private final SimpleVersionedSerializer<Record<Integer>> committableSerializer;

        public TestingCommittableSerializer(
                SimpleVersionedSerializer<Record<Integer>> committableSerializer) {
            super(committableSerializer);
            this.committableSerializer = committableSerializer;
        }

        @Override
        public byte[] serialize(List<Record<Integer>> obj) throws IOException {
            final DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeInt(SinkV1CommittableDeserializer.MAGIC_NUMBER);
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    committableSerializer, obj, out);
            return out.getCopyOfBuffer();
        }
    }
}

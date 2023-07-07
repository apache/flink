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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierTracker;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.checkpointing.SingleCheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.checkpointing.UpstreamRecoveryTracker;
import org.apache.flink.streaming.runtime.io.flushing.FlushEventHandler;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.clock.SystemClock;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamTaskNetworkInput}. */
public class StreamTaskNetworkInputTest {

    private static final int PAGE_SIZE = 1000;

    private final IOManager ioManager = new IOManagerAsync();

    @After
    public void tearDown() throws Exception {
        ioManager.close();
    }

    @Test
    public void testIsAvailableWithBufferedDataInDeserializer() throws Exception {
        List<BufferOrEvent> buffers = Collections.singletonList(createDataBuffer());

        VerifyRecordsDataOutput<Long> output = new VerifyRecordsDataOutput<>();
        StreamTaskNetworkInput<Long> input = createStreamTaskNetworkInput(buffers);

        assertHasNextElement(input, output);
        assertHasNextElement(input, output);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(2);
    }

    /**
     * InputGate on CheckpointBarrier can enqueue a mailbox action to execute and
     * StreamTaskNetworkInput must allow this action to execute before processing a following
     * record.
     */
    @Test
    public void testNoDataProcessedAfterCheckpointBarrier() throws Exception {
        CheckpointBarrier barrier =
                new CheckpointBarrier(0, 0, CheckpointOptions.forCheckpointWithDefaultLocation());

        List<BufferOrEvent> buffers = new ArrayList<>(2);
        buffers.add(new BufferOrEvent(barrier, new InputChannelInfo(0, 0)));
        buffers.add(createDataBuffer());

        VerifyRecordsDataOutput<Long> output = new VerifyRecordsDataOutput<>();
        StreamTaskNetworkInput<Long> input = createStreamTaskNetworkInput(buffers);

        assertHasNextElement(input, output);
        assertThat(output.getNumberOfEmittedRecords()).isZero();
    }

    private Map<InputChannelInfo, TestRecordDeserializer> createDeserializers(
            CheckpointableInput inputGate) {
        return inputGate.getChannelInfos().stream()
                .collect(
                        Collectors.toMap(
                                Function.identity(),
                                unused ->
                                        new TestRecordDeserializer(
                                                ioManager.getSpillingDirectoriesPaths())));
    }

    @Test
    public void testSnapshotAfterEndOfPartition() throws Exception {
        int numInputChannels = 1;
        int channelId = 0;
        int checkpointId = 0;

        VerifyRecordsDataOutput<Long> output = new VerifyRecordsDataOutput<>();
        LongSerializer inSerializer = LongSerializer.INSTANCE;
        StreamTestSingleInputGate<Long> inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);
        StreamTaskInput<Long> input =
                new StreamTaskNetworkInput<>(
                        new CheckpointedInputGate(
                                inputGate.getInputGate(),
                                SingleCheckpointBarrierHandler
                                        .createUnalignedCheckpointBarrierHandler(
                                                TestSubtaskCheckpointCoordinator.INSTANCE,
                                                "test",
                                                new DummyCheckpointInvokable(),
                                                SystemClock.getInstance(),
                                                false,
                                                inputGate.getInputGate()),
                                new FlushEventHandler(new DummyCheckpointInvokable(),"test"),
                                new SyncMailboxExecutor()),
                        inSerializer,
                        ioManager,
                        new StatusWatermarkValve(numInputChannels),
                        0,
                        () -> false);

        inputGate.sendEvent(
                new CheckpointBarrier(
                        checkpointId,
                        0L,
                        CheckpointOptions.forCheckpointWithDefaultLocation().toUnaligned()),
                channelId);
        inputGate.sendElement(new StreamRecord<>(42L), channelId);

        assertHasNextElement(input, output);
        assertHasNextElement(input, output);
        assertThat(output.getNumberOfEmittedRecords()).isOne();

        // send EndOfPartitionEvent and ensure that deserializer has been released
        inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, channelId);
        input.emitNext(output);

        // now snapshot all inflight buffers
        CompletableFuture<Void> completableFuture =
                input.prepareSnapshot(ChannelStateWriter.NO_OP, checkpointId);
        completableFuture.join();
    }

    @Test
    public void testReleasingDeserializerTimely() throws Exception {

        int numInputChannels = 2;
        LongSerializer inSerializer = LongSerializer.INSTANCE;
        StreamTestSingleInputGate<Long> inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);

        DataOutput<Long> output = new NoOpDataOutput<>();
        Map<InputChannelInfo, TestRecordDeserializer> deserializers =
                createDeserializers(inputGate.getInputGate());
        Map<InputChannelInfo, TestRecordDeserializer> copiedDeserializers =
                new HashMap<>(deserializers);
        StreamTaskInput<Long> input =
                new TestStreamTaskNetworkInput(
                        inputGate, inSerializer, numInputChannels, deserializers);

        for (InputChannelInfo channelInfo : inputGate.getInputGate().getChannelInfos()) {
            assertThat(deserializers.get(channelInfo)).isNotNull();
            inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, channelInfo.getInputChannelIdx());
            input.emitNext(output);
            assertThat(copiedDeserializers.get(channelInfo).isCleared()).isTrue();
            assertThat(deserializers.get(channelInfo)).isNull();
        }
    }

    @Test
    public void testInputStatusAfterEndOfRecovery() throws Exception {
        int numInputChannels = 2;
        LongSerializer inSerializer = LongSerializer.INSTANCE;
        StreamTestSingleInputGate<Long> inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);

        DataOutput<Long> output = new NoOpDataOutput<>();
        Map<InputChannelInfo, TestRecordDeserializer> deserializers =
                createDeserializers(inputGate.getInputGate());

        StreamTaskInput<Long> input =
                new TestStreamTaskNetworkInput(
                        inputGate, inSerializer, numInputChannels, deserializers);

        inputGate.sendElement(new StreamRecord<>(42L), 0);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        inputGate.sendEvent(EndOfChannelStateEvent.INSTANCE, 0);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        inputGate.sendEvent(EndOfChannelStateEvent.INSTANCE, 1);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.END_OF_RECOVERY);
    }

    @Test
    public void testRecordsAreProcessedInBatches() throws Exception {
        int numInputChannels = 2;
        Random random = new Random();
        LongSerializer inSerializer = LongSerializer.INSTANCE;
        StreamTestSingleInputGate<Long> inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);
        StreamTaskNetworkInput<Long> input =
                new StreamTaskNetworkInput<>(
                        createCheckpointedInputGate(inputGate.getInputGate()),
                        inSerializer,
                        ioManager,
                        new StatusWatermarkValve(numInputChannels),
                        0,
                        () -> true);
        VerifyRecordsDataOutput<Long> output = new VerifyRecordsDataOutput<>();

        // Test for records are processed in batches
        int expectedElementCount = 3;
        for (int i = 0; i < expectedElementCount; i++) {
            inputGate.sendElement(new StreamRecord<>((long) i), random.nextInt(numInputChannels));
        }

        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(expectedElementCount);

        // Test for records are processed in batches during processing event
        inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, 0);
        for (int i = 0; i < expectedElementCount; i++) {
            inputGate.sendElement(new StreamRecord<>((long) i), 1);
        }
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(expectedElementCount * 2);
    }

    @Test
    public void testBatchProcessingRecordsCanBeInterrupted() throws Exception {
        int numInputChannels = 2;
        Random random = new Random();
        LongSerializer inSerializer = LongSerializer.INSTANCE;
        StreamTestSingleInputGate<Long> inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);

        AtomicBoolean canEmitBatchOfRecords = new AtomicBoolean();
        StreamTaskNetworkInput<Long> input =
                new StreamTaskNetworkInput<>(
                        createCheckpointedInputGate(inputGate.getInputGate()),
                        inSerializer,
                        ioManager,
                        new StatusWatermarkValve(numInputChannels),
                        0,
                        canEmitBatchOfRecords::get);

        VerifyRecordsDataOutput<Long> output = new VerifyRecordsDataOutput<>();

        // Test for batch processing records can be interrupted and can be recovered
        int expectedElementCount = 5;
        for (int i = 0; i < expectedElementCount; i++) {
            inputGate.sendElement(new StreamRecord<>((long) i), random.nextInt(numInputChannels));
        }

        canEmitBatchOfRecords.set(false);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isOne();

        canEmitBatchOfRecords.set(true);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(expectedElementCount);

        // Test for batch processing records can be interrupted and can be recovered during
        // processing event
        inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, 0);
        for (int i = 0; i < expectedElementCount; i++) {
            inputGate.sendElement(new StreamRecord<>((long) i), 1);
        }

        // Just process the event
        canEmitBatchOfRecords.set(false);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.MORE_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(expectedElementCount);

        canEmitBatchOfRecords.set(true);
        assertThat(input.emitNext(output)).isEqualTo(DataInputStatus.NOTHING_AVAILABLE);
        assertThat(output.getNumberOfEmittedRecords()).isEqualTo(expectedElementCount * 2);
    }

    private BufferOrEvent createDataBuffer() throws IOException {
        try (BufferBuilder bufferBuilder =
                BufferBuilderTestUtils.createEmptyBufferBuilder(PAGE_SIZE)) {
            BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
            serializeRecord(42L, bufferBuilder);
            serializeRecord(44L, bufferBuilder);

            return new BufferOrEvent(bufferConsumer.build(), new InputChannelInfo(0, 0));
        }
    }

    private StreamTaskNetworkInput<Long> createStreamTaskNetworkInput(List<BufferOrEvent> buffers) {
        return new StreamTaskNetworkInput<>(
                createCheckpointedInputGate(new MockInputGate(1, buffers, false)),
                LongSerializer.INSTANCE,
                ioManager,
                new StatusWatermarkValve(1),
                0,
                () -> false);
    }

    private static CheckpointedInputGate createCheckpointedInputGate(InputGate inputGate) {
        return new CheckpointedInputGate(
                inputGate,
                new CheckpointBarrierTracker(
                        1, new DummyCheckpointInvokable(), SystemClock.getInstance(), false),
                new FlushEventHandler(new DummyCheckpointInvokable(), "test"),
                new SyncMailboxExecutor(),
                UpstreamRecoveryTracker.forInputGate(inputGate));
    }

    private void serializeRecord(long value, BufferBuilder bufferBuilder) throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(128);
        SerializationDelegate<StreamElement> serializationDelegate =
                new SerializationDelegate<>(new StreamElementSerializer<>(LongSerializer.INSTANCE));
        serializationDelegate.setInstance(new StreamRecord<>(value));
        ByteBuffer serializedRecord =
                RecordWriter.serializeRecord(serializer, serializationDelegate);
        bufferBuilder.appendAndCommit(serializedRecord);

        assertThat(bufferBuilder.isFull()).isFalse();
    }

    private static <T> void assertHasNextElement(StreamTaskInput<T> input, DataOutput<T> output)
            throws Exception {
        assertThat(input.getAvailableFuture().isDone()).isTrue();
        DataInputStatus status = input.emitNext(output);
        assertThat(status).isEqualTo(DataInputStatus.MORE_AVAILABLE);
    }

    private static class TestRecordDeserializer
            extends SpillingAdaptiveSpanningRecordDeserializer<
                    DeserializationDelegate<StreamElement>> {

        private boolean cleared = false;

        public TestRecordDeserializer(String[] tmpDirectories) {
            super(tmpDirectories);
        }

        @Override
        public void clear() {
            cleared = true;
        }

        public boolean isCleared() {
            return cleared;
        }
    }

    private static class NoOpDataOutput<T> implements DataOutput<T> {

        @Override
        public void emitRecord(StreamRecord<T> record) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}
    }

    private static class VerifyRecordsDataOutput<T> extends NoOpDataOutput<T> {

        private int numberOfEmittedRecords;

        @Override
        public void emitRecord(StreamRecord<T> record) {
            numberOfEmittedRecords++;
        }

        int getNumberOfEmittedRecords() {
            return numberOfEmittedRecords;
        }
    }

    private static class TestStreamTaskNetworkInput
            extends AbstractStreamTaskNetworkInput<Long, TestRecordDeserializer> {
        public TestStreamTaskNetworkInput(
                StreamTestSingleInputGate<Long> inputGate,
                LongSerializer inSerializer,
                int numInputChannels,
                Map<InputChannelInfo, TestRecordDeserializer> deserializers) {
            super(
                    createCheckpointedInputGate(inputGate.getInputGate()),
                    inSerializer,
                    new StatusWatermarkValve(numInputChannels),
                    0,
                    deserializers,
                    () -> false);
        }

        @Override
        public CompletableFuture<Void> prepareSnapshot(
                ChannelStateWriter channelStateWriter, long checkpointId)
                throws CheckpointException {
            throw new UnsupportedOperationException();
        }
    }
}

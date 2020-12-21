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
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.memory.DataOutputSerializer;
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
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.operators.testutils.DummyCheckpointInvokable;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.operators.SyncMailboxExecutor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;

import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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

        VerifyRecordsDataOutput output = new VerifyRecordsDataOutput<>();
        StreamTaskNetworkInput input = createStreamTaskNetworkInput(buffers);

        assertHasNextElement(input, output);
        assertHasNextElement(input, output);
        assertEquals(2, output.getNumberOfEmittedRecords());
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

        VerifyRecordsDataOutput output = new VerifyRecordsDataOutput<>();
        StreamTaskNetworkInput input = createStreamTaskNetworkInput(buffers);

        assertHasNextElement(input, output);
        assertEquals(0, output.getNumberOfEmittedRecords());
    }

    private TestRecordDeserializer[] createDeserializers(int numberOfInputChannels) {
        return IntStream.range(0, numberOfInputChannels)
                .mapToObj(
                        index ->
                                new TestRecordDeserializer(ioManager.getSpillingDirectoriesPaths()))
                .toArray(TestRecordDeserializer[]::new);
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
        TestRecordDeserializer[] deserializers = createDeserializers(numInputChannels);
        StreamTaskNetworkInput<Long> input =
                new StreamTaskNetworkInput<>(
                        new CheckpointedInputGate(
                                inputGate.getInputGate(),
                                SingleCheckpointBarrierHandler
                                        .createUnalignedCheckpointBarrierHandler(
                                                TestSubtaskCheckpointCoordinator.INSTANCE,
                                                "test",
                                                new DummyCheckpointInvokable(),
                                                inputGate.getInputGate()),
                                new SyncMailboxExecutor()),
                        inSerializer,
                        new StatusWatermarkValve(numInputChannels),
                        0,
                        deserializers);

        inputGate.sendEvent(
                new CheckpointBarrier(
                        checkpointId,
                        0L,
                        CheckpointOptions.forCheckpointWithDefaultLocation().toUnaligned()),
                channelId);
        inputGate.sendElement(new StreamRecord<>(42L), channelId);

        assertHasNextElement(input, output);
        assertHasNextElement(input, output);
        assertEquals(1, output.getNumberOfEmittedRecords());

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
        StreamTestSingleInputGate inputGate =
                new StreamTestSingleInputGate<>(numInputChannels, 0, inSerializer, 1024);

        TestRecordDeserializer[] deserializers = new TestRecordDeserializer[numInputChannels];
        for (int i = 0; i < deserializers.length; i++) {
            deserializers[i] = new TestRecordDeserializer(ioManager.getSpillingDirectoriesPaths());
        }

        TestRecordDeserializer[] copiedDeserializers =
                Arrays.copyOf(deserializers, deserializers.length);
        DataOutput output = new NoOpDataOutput<>();
        StreamTaskNetworkInput input =
                new StreamTaskNetworkInput<>(
                        new CheckpointedInputGate(
                                inputGate.getInputGate(),
                                new CheckpointBarrierTracker(1, new DummyCheckpointInvokable()),
                                new SyncMailboxExecutor()),
                        inSerializer,
                        new StatusWatermarkValve(1),
                        0,
                        deserializers);

        for (int i = 0; i < numInputChannels; i++) {
            assertNotNull(deserializers[i]);
            inputGate.sendEvent(EndOfPartitionEvent.INSTANCE, i);
            input.emitNext(output);
            assertTrue(copiedDeserializers[i].isCleared());
        }
    }

    private BufferOrEvent createDataBuffer() throws IOException {
        BufferBuilder bufferBuilder = BufferBuilderTestUtils.createEmptyBufferBuilder(PAGE_SIZE);
        BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer();
        serializeRecord(42L, bufferBuilder);
        serializeRecord(44L, bufferBuilder);

        return new BufferOrEvent(bufferConsumer.build(), new InputChannelInfo(0, 0));
    }

    private StreamTaskNetworkInput createStreamTaskNetworkInput(List<BufferOrEvent> buffers) {
        return new StreamTaskNetworkInput<>(
                new CheckpointedInputGate(
                        new MockInputGate(1, buffers, false),
                        new CheckpointBarrierTracker(1, new DummyCheckpointInvokable()),
                        new SyncMailboxExecutor()),
                LongSerializer.INSTANCE,
                ioManager,
                new StatusWatermarkValve(1),
                0);
    }

    private void serializeRecord(long value, BufferBuilder bufferBuilder) throws IOException {
        DataOutputSerializer serializer = new DataOutputSerializer(128);
        SerializationDelegate<StreamElement> serializationDelegate =
                new SerializationDelegate<>(new StreamElementSerializer<>(LongSerializer.INSTANCE));
        serializationDelegate.setInstance(new StreamRecord<>(value));
        ByteBuffer serializedRecord =
                RecordWriter.serializeRecord(serializer, serializationDelegate);
        bufferBuilder.appendAndCommit(serializedRecord);

        assertFalse(bufferBuilder.isFull());
    }

    private static void assertHasNextElement(StreamTaskNetworkInput input, DataOutput output)
            throws Exception {
        assertTrue(input.getAvailableFuture().isDone());
        InputStatus status = input.emitNext(output);
        assertThat(status, is(InputStatus.MORE_AVAILABLE));
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
        public void emitStreamStatus(StreamStatus streamStatus) {}

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
}

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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.util.DeserializationUtils;
import org.apache.flink.runtime.operators.shipping.OutputEmitter;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.testutils.serialization.types.SerializationTestType;
import org.apache.flink.testutils.serialization.types.SerializationTestTypeFactory;
import org.apache.flink.testutils.serialization.types.Util;
import org.apache.flink.types.IntValue;
import org.apache.flink.util.XORShiftRandom;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link RecordWriter}. */
@ExtendWith(ParameterizedTestExtension.class)
class RecordWriterTest {

    @Parameter public boolean isBroadcastWriter;

    @Parameters(name = "isBroadcastWriter={0}")
    public static List<Boolean> parameters() throws Exception {
        return Arrays.asList(false, true);
    }

    // ---------------------------------------------------------------------------------------------
    // Resource release tests
    // ---------------------------------------------------------------------------------------------

    /** Tests broadcasting events when no records have been emitted yet. */
    @TestTemplate
    void testBroadcastEventNoRecords() throws Exception {
        int numberOfChannels = 4;
        int bufferSize = 32;

        ResultPartition partition = createResultPartition(bufferSize, numberOfChannels);
        RecordWriter<ByteArrayIO> writer = createRecordWriter(partition);
        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        Integer.MAX_VALUE + 919192L,
                        Integer.MAX_VALUE + 18828228L,
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        // No records emitted yet, broadcast should not request a buffer
        writer.broadcastEvent(barrier);

        assertThat(partition.getBufferPool().bestEffortGetNumOfUsedBuffers()).isZero();

        for (int i = 0; i < numberOfChannels; i++) {
            assertThat(partition.getNumberOfQueuedBuffers(i)).isOne();
            ResultSubpartitionView view =
                    partition.createSubpartitionView(i, new NoOpBufferAvailablityListener());
            BufferOrEvent boe = parseBuffer(view.getNextBuffer().buffer(), i);
            assertThat(boe.isEvent()).isTrue();
            assertThat(boe.getEvent()).isEqualTo(barrier);
            assertThat(view.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable()).isFalse();
        }
    }

    /** Tests broadcasting events when records have been emitted. */
    @TestTemplate
    void testBroadcastEventMixedRecords() throws Exception {
        Random rand = new XORShiftRandom();
        int numberOfChannels = 4;
        int bufferSize = 32;
        int lenBytes = 4; // serialized length

        ResultPartition partition = createResultPartition(bufferSize, numberOfChannels);
        RecordWriter<ByteArrayIO> writer = createRecordWriter(partition);
        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        Integer.MAX_VALUE + 1292L,
                        Integer.MAX_VALUE + 199L,
                        CheckpointOptions.forCheckpointWithDefaultLocation());

        // Emit records on some channels first (requesting buffers), then
        // broadcast the event. The record buffers should be emitted first, then
        // the event. After the event, no new buffer should be requested.

        // (i) Smaller than the buffer size
        byte[] bytes = new byte[bufferSize / 2];
        rand.nextBytes(bytes);

        writer.emit(new ByteArrayIO(bytes));

        // (ii) Larger than the buffer size
        bytes = new byte[bufferSize + 1];
        rand.nextBytes(bytes);

        writer.emit(new ByteArrayIO(bytes));

        // (iii) Exactly the buffer size
        bytes = new byte[bufferSize - lenBytes];
        rand.nextBytes(bytes);

        writer.emit(new ByteArrayIO(bytes));

        // (iv) Broadcast the event
        writer.broadcastEvent(barrier);

        if (isBroadcastWriter) {
            assertThat(partition.getBufferPool().bestEffortGetNumOfUsedBuffers()).isEqualTo(3);

            for (int i = 0; i < numberOfChannels; i++) {
                assertThat(partition.getNumberOfQueuedBuffers(i))
                        .isEqualTo(4); // 3 buffer + 1 event

                ResultSubpartitionView view =
                        partition.createSubpartitionView(i, new NoOpBufferAvailablityListener());
                for (int j = 0; j < 3; j++) {
                    assertThat(parseBuffer(view.getNextBuffer().buffer(), 0).isBuffer()).isTrue();
                }

                BufferOrEvent boe = parseBuffer(view.getNextBuffer().buffer(), i);
                assertThat(boe.isEvent()).isTrue();
                assertThat(boe.getEvent()).isEqualTo(barrier);
            }
        } else {
            assertThat(partition.getBufferPool().bestEffortGetNumOfUsedBuffers()).isEqualTo(4);
            ResultSubpartitionView[] views = new ResultSubpartitionView[4];

            assertThat(partition.getNumberOfQueuedBuffers(0)).isEqualTo(2); // 1 buffer + 1 event
            views[0] = partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
            assertThat(parseBuffer(views[0].getNextBuffer().buffer(), 0).isBuffer()).isTrue();

            assertThat(partition.getNumberOfQueuedBuffers(1)).isEqualTo(3); // 2 buffers + 1 event
            views[1] = partition.createSubpartitionView(1, new NoOpBufferAvailablityListener());
            assertThat(parseBuffer(views[1].getNextBuffer().buffer(), 1).isBuffer()).isTrue();
            assertThat(parseBuffer(views[1].getNextBuffer().buffer(), 1).isBuffer()).isTrue();

            assertThat(partition.getNumberOfQueuedBuffers(2)).isEqualTo(2); // 1 buffer + 1 event
            views[2] = partition.createSubpartitionView(2, new NoOpBufferAvailablityListener());
            assertThat(parseBuffer(views[2].getNextBuffer().buffer(), 2).isBuffer()).isTrue();

            views[3] = partition.createSubpartitionView(3, new NoOpBufferAvailablityListener());
            assertThat(partition.getNumberOfQueuedBuffers(3)).isOne(); // 0 buffers + 1 event

            // every queue's last element should be the event
            for (int i = 0; i < numberOfChannels; i++) {
                BufferOrEvent boe = parseBuffer(views[i].getNextBuffer().buffer(), i);
                assertThat(boe.isEvent()).isTrue();
                assertThat(boe.getEvent()).isEqualTo(barrier);
            }
        }
    }

    /**
     * Tests that event buffers are properly recycled when broadcasting events to multiple channels.
     */
    @TestTemplate
    void testBroadcastEventBufferReferenceCounting() throws Exception {
        int bufferSize = 32 * 1024;
        int numSubpartitions = 2;

        ResultPartition partition = createResultPartition(bufferSize, numSubpartitions);
        RecordWriter<?> writer = createRecordWriter(partition);

        writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);

        // get references to buffer consumers (copies from the original event buffer consumer)
        Buffer[] buffers = new Buffer[numSubpartitions];

        // process all collected events (recycles the buffer)
        for (int i = 0; i < numSubpartitions; i++) {
            assertThat(partition.getNumberOfQueuedBuffers(i)).isOne();
            ResultSubpartitionView view =
                    partition.createSubpartitionView(i, new NoOpBufferAvailablityListener());
            buffers[i] = view.getNextBuffer().buffer();
            assertThat(parseBuffer(buffers[i], i).isEvent()).isTrue();
        }

        for (int i = 0; i < numSubpartitions; ++i) {
            assertThat(buffers[i].isRecycled()).isTrue();
        }
    }

    /**
     * Tests that broadcasted events' buffers are independent (in their (reader) indices) once they
     * are put into the queue for Netty when broadcasting events to multiple channels.
     */
    @TestTemplate
    void testBroadcastEventBufferIndependence() throws Exception {
        verifyBroadcastBufferOrEventIndependence(true);
    }

    /**
     * Tests that broadcasted records' buffers are independent (in their (reader) indices) once they
     * are put into the queue for Netty when broadcasting events to multiple channels.
     */
    @TestTemplate
    void testBroadcastEmitBufferIndependence() throws Exception {
        verifyBroadcastBufferOrEventIndependence(false);
    }

    /**
     * Tests that records are broadcast via {@link RecordWriter#broadcastEmit(IOReadableWritable)}.
     */
    @TestTemplate
    void testBroadcastEmitRecord(@TempDir Path tempPath) throws Exception {
        final int numberOfChannels = 4;
        final int bufferSize = 32;
        final int numValues = 8;
        final int serializationLength = 4;

        final ResultPartition partition = createResultPartition(bufferSize, numberOfChannels);
        final RecordWriter<SerializationTestType> writer = createRecordWriter(partition);
        final RecordDeserializer<SerializationTestType> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {tempPath.toString()});

        final ArrayDeque<SerializationTestType> serializedRecords = new ArrayDeque<>();
        final Iterable<SerializationTestType> records =
                Util.randomRecords(numValues, SerializationTestTypeFactory.INT);
        for (SerializationTestType record : records) {
            serializedRecords.add(record);
            writer.broadcastEmit(record);
        }

        final int numRequiredBuffers = numValues / (bufferSize / (4 + serializationLength));
        if (isBroadcastWriter) {
            assertThat(partition.getBufferPool().bestEffortGetNumOfUsedBuffers())
                    .isEqualTo(numRequiredBuffers);
        } else {
            assertThat(partition.getBufferPool().bestEffortGetNumOfUsedBuffers())
                    .isEqualTo(numRequiredBuffers * numberOfChannels);
        }

        for (int i = 0; i < numberOfChannels; i++) {
            assertThat(partition.getNumberOfQueuedBuffers(i)).isEqualTo(numRequiredBuffers);
            ResultSubpartitionView view =
                    partition.createSubpartitionView(i, new NoOpBufferAvailablityListener());
            verifyDeserializationResults(
                    view, deserializer, serializedRecords.clone(), numRequiredBuffers, numValues);
        }
    }

    /**
     * Tests that the RecordWriter is available iif the respective LocalBufferPool has at-least one
     * available buffer.
     */
    @TestTemplate
    void testIsAvailableOrNot() throws Exception {
        // setup
        final NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        final BufferPool localPool = globalPool.createBufferPool(1, 1, 1, Integer.MAX_VALUE, 0);
        final ResultPartitionWriter resultPartition =
                new ResultPartitionBuilder().setBufferPoolFactory(() -> localPool).build();
        resultPartition.setup();

        final RecordWriter<?> recordWriter = createRecordWriter(resultPartition);

        try {
            // record writer is available because of initial available global pool
            assertThat(recordWriter.getAvailableFuture()).isDone();

            // request one buffer from the local pool to make it unavailable afterwards
            try (BufferBuilder bufferBuilder = localPool.requestBufferBuilder(0)) {
                assertThat(bufferBuilder).isNotNull();
                assertThat(recordWriter.getAvailableFuture()).isNotDone();

                // recycle the buffer to make the local pool available again
                final Buffer buffer = BufferBuilderTestUtils.buildSingleBuffer(bufferBuilder);
                buffer.recycleBuffer();
            }
            assertThat(recordWriter.getAvailableFuture()).isDone();
            assertThat(recordWriter.getAvailableFuture()).isEqualTo(recordWriter.AVAILABLE);

        } finally {
            localPool.lazyDestroy();
            globalPool.destroy();
        }
    }

    private void verifyBroadcastBufferOrEventIndependence(boolean broadcastEvent) throws Exception {
        ResultPartition partition = createResultPartition(4096, 2);
        RecordWriter<IntValue> writer = createRecordWriter(partition);

        if (broadcastEvent) {
            writer.broadcastEvent(EndOfPartitionEvent.INSTANCE);
        } else {
            writer.broadcastEmit(new IntValue(0));
        }

        // verify added to all queues
        assertThat(partition.getNumberOfQueuedBuffers(0)).isOne();
        assertThat(partition.getNumberOfQueuedBuffers(1)).isOne();

        ResultSubpartitionView view0 =
                partition.createSubpartitionView(0, new NoOpBufferAvailablityListener());
        ResultSubpartitionView view1 =
                partition.createSubpartitionView(1, new NoOpBufferAvailablityListener());

        // these two buffers may share the memory but not the indices!
        Buffer buffer1 = view0.getNextBuffer().buffer();
        Buffer buffer2 = view1.getNextBuffer().buffer();
        assertThat(buffer1.getReaderIndex()).isZero();
        assertThat(buffer2.getReaderIndex()).isZero();
        buffer1.setReaderIndex(1);
        assertThat(buffer2.getReaderIndex())
                .withFailMessage("Buffer 2 shares the same reader index as buffer 1")
                .isZero();
    }

    public static void verifyDeserializationResults(
            ResultSubpartitionView view,
            RecordDeserializer<SerializationTestType> deserializer,
            ArrayDeque<SerializationTestType> expectedRecords,
            int numRequiredBuffers,
            int numValues)
            throws Exception {
        int assertRecords = 0;
        for (int j = 0; j < numRequiredBuffers; j++) {
            Buffer buffer = view.getNextBuffer().buffer();
            deserializer.setNextBuffer(buffer);

            assertRecords += DeserializationUtils.deserializeRecords(expectedRecords, deserializer);
        }
        assertThat(view.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable()).isFalse();
        assertThat(assertRecords).isEqualTo(numValues);
    }

    /** Creates the {@link RecordWriter} instance based on whether it is a broadcast writer. */
    private RecordWriter createRecordWriter(ResultPartitionWriter writer) {
        if (isBroadcastWriter) {
            return new RecordWriterBuilder()
                    .setChannelSelector(new OutputEmitter(ShipStrategyType.BROADCAST, 0))
                    .build(writer);
        } else {
            return new RecordWriterBuilder().build(writer);
        }
    }

    public static ResultPartition createResultPartition(int bufferSize, int numSubpartitions)
            throws IOException {
        NettyShuffleEnvironment env =
                new NettyShuffleEnvironmentBuilder().setBufferSize(bufferSize).build();
        ResultPartition partition =
                createPartition(env, ResultPartitionType.PIPELINED, numSubpartitions);
        partition.setup();
        return partition;
    }

    // ---------------------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------------------

    static BufferOrEvent parseBuffer(Buffer buffer, int targetChannel) throws IOException {
        if (buffer.isBuffer()) {
            return new BufferOrEvent(buffer, new InputChannelInfo(0, targetChannel));
        } else {
            // is event:
            AbstractEvent event =
                    EventSerializer.fromBuffer(buffer, RecordWriterTest.class.getClassLoader());
            buffer.recycleBuffer(); // the buffer is not needed anymore
            return new BufferOrEvent(event, new InputChannelInfo(0, targetChannel));
        }
    }

    private static class ByteArrayIO implements IOReadableWritable {

        private final byte[] bytes;

        public ByteArrayIO(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.write(bytes);
        }

        @Override
        public void read(DataInputView in) throws IOException {
            in.readFully(bytes);
        }
    }
}

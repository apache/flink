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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredBufferStoreImpl;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests buffer ownership semantics of {@link ChannelStateFilteringHandler.GateFilterHandler}. Each
 * test verifies that buffers are properly recycled on both success and failure paths.
 */
class GateFilterHandlerBufferOwnershipTest {

    private static final int BUFFER_SIZE = 1024;
    private static final SubtaskConnectionDescriptor KEY = new SubtaskConnectionDescriptor(0, 0);
    private static final InputChannelInfo TARGET_CHANNEL = new InputChannelInfo(0, 0);

    @TempDir private Path temporaryFolder;

    @Test
    void testSourceBufferRecycledOnSuccess() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter();
        handler.filterAndRewrite(0, 0, sourceBuffer, outputWriter, TARGET_CHANNEL);

        // sourceBuffer should be recycled by the deserializer after consumption
        assertThat(sourceBuffer.isRecycled()).isTrue();

        outputWriter.flush();
        outputWriter.close();
    }

    @Test
    void testSourceBufferRecycledWhenAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter();
        handler.filterAndRewrite(0, 0, sourceBuffer, outputWriter, TARGET_CHANNEL);

        // sourceBuffer should still be recycled even though no output was produced
        assertThat(sourceBuffer.isRecycled()).isTrue();

        outputWriter.flush();
        outputWriter.close();
    }

    @Test
    void testSourceBufferRecycledOnInvalidVirtualChannel() throws Exception {
        // Create handler with KEY=(0,0) but call with (1,1) to trigger IllegalStateException
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter();

        assertThatThrownBy(
                        () ->
                                handler.filterAndRewrite(
                                        1, 1, sourceBuffer, outputWriter, TARGET_CHANNEL))
                .isInstanceOf(IllegalStateException.class);

        // sourceBuffer must be recycled even when lookup fails before setNextBuffer
        assertThat(sourceBuffer.isRecycled()).isTrue();

        outputWriter.close();
    }

    /**
     * Tests that the production cleanup path works: when ChannelStateFilteringHandler is used in a
     * try-with-resources block, its close() clears all GateFilterHandlers and their deserializers,
     * ensuring buffers held by the deserializer are recycled.
     */
    @Test
    void testCloseRecyclesDeserializerHeldBufferAfterError() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> gateHandler =
                createHandler(RecordFilter.acceptAll());
        // Wrap in ChannelStateFilteringHandler, the production-level owner
        ChannelStateFilteringHandler filteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler<?>[] {gateHandler});

        // Create a large buffer that will require multiple writes
        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L, 4L, 5L);

        // Create a dispatcher that throws on the second write call
        FilteredBufferDispatcher failingWriter = new FailingOutputWriter();

        // Simulate the production try-with-resources pattern
        assertThatThrownBy(
                        () -> {
                            try (ChannelStateFilteringHandler ignored = filteringHandler) {
                                filteringHandler.filterAndRewrite(
                                        0, 0, 0, sourceBuffer, failingWriter, TARGET_CHANNEL);
                            }
                        })
                .isInstanceOf(IOException.class)
                .hasMessage("Simulated write failure");

        // After close(), the entire cleanup chain has fired:
        // ChannelStateFilteringHandler.close() -> GateFilterHandler.clear()
        //   -> VirtualChannel.clear() -> deserializer.clear() -> sourceBuffer.recycleBuffer()
        assertThat(sourceBuffer.isRecycled()).isTrue();
    }

    // -------------------------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------------------------

    private ChannelStateFilteringHandler.GateFilterHandler<Long> createHandler(
            RecordFilter<Long> filter) {
        RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {System.getProperty("java.io.tmpdir")});
        VirtualChannel<Long> vc = new VirtualChannel<>(deserializer, filter);

        Map<SubtaskConnectionDescriptor, VirtualChannel<Long>> channels = new HashMap<>();
        channels.put(KEY, vc);

        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        return new ChannelStateFilteringHandler.GateFilterHandler<>(channels, serializer);
    }

    private FilteredBufferDispatcher createTestOutputWriter() throws IOException {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(TARGET_CHANNEL);
        Map<InputChannelInfo, RecoveredBufferStoreImpl> storesByChannel = new HashMap<>();
        storesByChannel.put(TARGET_CHANNEL, store);

        String[] spillDirs = new String[] {temporaryFolder.toString()};
        BufferRequester newBufferPerRequest =
                new BufferRequester() {
                    @Override
                    public Buffer requestBuffer(InputChannelInfo channelInfo) {
                        return createEmptyBuffer();
                    }

                    @Override
                    public Buffer requestBufferBlocking(InputChannelInfo channelInfo) {
                        return createEmptyBuffer();
                    }

                    @Override
                    public void releaseExclusiveBuffers() {}
                };
        return new FilteredBufferDispatcherImpl(
                storesByChannel,
                ChannelStateWriter.NO_OP,
                spillDirs,
                BUFFER_SIZE,
                newBufferPerRequest);
    }

    private Buffer createBufferWithRecords(Long... values) {
        try {
            StreamElementSerializer<Long> serializer =
                    new StreamElementSerializer<>(LongSerializer.INSTANCE);
            DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);

            for (Long value : values) {
                DataOutputSerializer recordOutput = new DataOutputSerializer(64);
                serializer.serialize(new StreamRecord<>(value), recordOutput);
                int recordLength = recordOutput.length();
                output.writeInt(recordLength);
                output.write(recordOutput.getSharedBuffer(), 0, recordLength);
            }

            byte[] data = output.getCopyOfBuffer();
            MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
            segment.put(0, data, 0, data.length);

            NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
            buffer.setSize(data.length);
            return buffer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Buffer createEmptyBuffer() {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
    }

    /**
     * A dispatcher that fails on the second write call. Used to test cleanup paths when
     * filterAndRewrite encounters an error during serialization output.
     */
    private static class FailingOutputWriter implements FilteredBufferDispatcher {
        private int writeCount = 0;

        @Override
        public void write(byte[] data, int length, InputChannelInfo channelInfo)
                throws IOException {
            writeCount++;
            if (writeCount > 1) {
                throw new IOException("Simulated write failure");
            }
        }

        @Override
        public void flush() {}

        @Override
        public void drainPendingSpill() {}

        @Override
        public void close() {}
    }
}

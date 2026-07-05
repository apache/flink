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
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests buffer ownership semantics of {@link ChannelStateFilteringHandler.GateFilterHandler}. Each
 * test verifies that source buffers are properly recycled on both success and failure paths.
 */
class GateFilterHandlerBufferOwnershipTest {

    private static final int BUFFER_SIZE = 1024;
    private static final SubtaskConnectionDescriptor KEY = new SubtaskConnectionDescriptor(0, 0);

    @Test
    void testSourceBufferRecycledOnSuccess() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        handler.filterAndRewrite(0, 0, sourceBuffer, new DataOutputSerializer(BUFFER_SIZE));

        assertThat(sourceBuffer.isRecycled()).isTrue();
    }

    @Test
    void testSourceBufferRecycledWhenAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        handler.filterAndRewrite(0, 0, sourceBuffer, new DataOutputSerializer(BUFFER_SIZE));

        assertThat(sourceBuffer.isRecycled()).isTrue();
    }

    @Test
    void testSourceBufferRecycledOnInvalidVirtualChannel() {
        // Create handler with KEY=(0,0) but call with (1,1) to trigger IllegalStateException.
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L);

        assertThatThrownBy(
                        () ->
                                handler.filterAndRewrite(
                                        1, 1, sourceBuffer, new DataOutputSerializer(BUFFER_SIZE)))
                .isInstanceOf(IllegalStateException.class);

        // sourceBuffer must be recycled even when lookup fails before setNextBuffer.
        assertThat(sourceBuffer.isRecycled()).isTrue();
    }

    /**
     * When filterAndRewrite throws mid-processing, the deserializer may still hold sourceBuffer. In
     * production, ChannelStateFilteringHandler is used in a try-with-resources block (see {@code
     * SequentialChannelStateReaderImpl#readInputData}), so its close() is guaranteed to be called,
     * which triggers clear() on all GateFilterHandlers and their deserializers. This test simulates
     * that exact pattern.
     */
    @Test
    void testCloseRecyclesDeserializerHeldBufferAfterError() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> gateHandler =
                createHandler(RecordFilter.acceptAll());
        ChannelStateFilteringHandler filteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler<?>[] {gateHandler});

        // A serializer that throws while writing the second record's length prefix, triggering a
        // mid-processing failure after the first record has already been emitted.
        DataOutputSerializer failingSerializer = new FailingAfterFirstRecordSerializer();
        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L, 4L, 5L);

        assertThatThrownBy(
                        () -> {
                            try (ChannelStateFilteringHandler ignored = filteringHandler) {
                                filteringHandler.filterAndRewrite(
                                        0, 0, 0, sourceBuffer, failingSerializer);
                            }
                        })
                .isInstanceOf(IOException.class)
                .hasMessage("Simulated write failure");

        // After close(), the entire cleanup chain has fired.
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

    /**
     * A {@link DataOutputSerializer} that throws an IOException while writing the second record's
     * length prefix, simulating a failure mid-stream to verify that the source buffer is still
     * recycled via the filtering handler's close() cleanup chain. Each surviving record begins with
     * a {@code writeInt} placeholder for its length, so the second {@code writeInt} marks the start
     * of the second record.
     */
    private static final class FailingAfterFirstRecordSerializer extends DataOutputSerializer {
        private int writeIntCount = 0;

        FailingAfterFirstRecordSerializer() {
            super(BUFFER_SIZE);
        }

        @Override
        public void writeInt(int v) throws IOException {
            if (++writeIntCount > 1) {
                throw new IOException("Simulated write failure");
            }
            super.writeInt(v);
        }
    }
}

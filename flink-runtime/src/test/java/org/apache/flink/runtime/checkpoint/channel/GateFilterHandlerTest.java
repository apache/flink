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
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChannelStateFilteringHandler.GateFilterHandler}. */
class GateFilterHandlerTest {

    private static final int BUFFER_SIZE = 1024;
    private static final SubtaskConnectionDescriptor KEY = new SubtaskConnectionDescriptor(0, 0);

    @Test
    void testAllRecordsPassFilter() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        List<Buffer> result = handler.filterAndRewrite(0, 0, sourceBuffer, this::createEmptyBuffer);

        // deserializeBuffers consumes (recycles) each buffer via the deserializer
        List<Long> values = deserializeBuffers(result);
        assertThat(values).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        List<Buffer> result = handler.filterAndRewrite(0, 0, sourceBuffer, this::createEmptyBuffer);

        assertThat(result).isEmpty();
    }

    @Test
    void testPartialFiltering() throws Exception {
        RecordFilter<Long> keepEven = record -> record.getValue() % 2 == 0;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(keepEven);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L, 4L, 5L);
        List<Buffer> result = handler.filterAndRewrite(0, 0, sourceBuffer, this::createEmptyBuffer);

        List<Long> values = deserializeBuffers(result);
        assertThat(values).containsExactly(2L, 4L);
    }

    @Test
    void testSmallOutputBufferProducesMultipleBuffers() throws Exception {
        // Use a very small output buffer size so records must span multiple buffers
        int smallBufferSize = 8;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        List<Buffer> result =
                handler.filterAndRewrite(
                        0, 0, sourceBuffer, () -> createEmptyBuffer(smallBufferSize));

        // Each Long record needs 4 bytes length + ~9 bytes data > 8-byte buffer
        assertThat(result.size()).isGreaterThan(1);

        List<Long> values = deserializeBuffers(result);
        assertThat(values).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testEmptyBuffer() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer emptyBuffer = createEmptyBuffer();
        emptyBuffer.setSize(0);

        List<Buffer> result = handler.filterAndRewrite(0, 0, emptyBuffer, this::createEmptyBuffer);

        assertThat(result).isEmpty();
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

    private Buffer createBufferWithRecords(Long... values) throws IOException {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        return serializeRecordsToBuffer(serializer, values);
    }

    /** Serializes records into a buffer using Flink's length-prefixed format. */
    private Buffer serializeRecordsToBuffer(
            StreamElementSerializer<Long> serializer, Long... values) throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);

        for (Long value : values) {
            // Serialize using the same length-prefixed format as Flink
            DataOutputSerializer recordOutput = new DataOutputSerializer(64);
            serializer.serialize(new StreamRecord<>(value), recordOutput);
            int recordLength = recordOutput.length();

            // Write 4-byte big-endian length prefix
            output.writeInt(recordLength);
            // Write record bytes
            output.write(recordOutput.getSharedBuffer(), 0, recordLength);
        }

        byte[] data = output.getCopyOfBuffer();
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        segment.put(0, data, 0, data.length);

        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(data.length);
        return buffer;
    }

    private Buffer createEmptyBuffer() {
        return createEmptyBuffer(BUFFER_SIZE);
    }

    private Buffer createEmptyBuffer(int size) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
    }

    private List<Long> deserializeBuffers(List<Buffer> buffers) throws IOException {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        SpillingAdaptiveSpanningRecordDeserializer<DeserializationDelegate<StreamElement>>
                deserializer =
                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                new String[] {System.getProperty("java.io.tmpdir")});
        DeserializationDelegate<StreamElement> delegate =
                new NonReusingDeserializationDelegate<>(serializer);

        List<Long> values = new ArrayList<>();
        for (Buffer buffer : buffers) {
            deserializer.setNextBuffer(buffer);
            while (true) {
                RecordDeserializer.DeserializationResult result =
                        deserializer.getNextRecord(delegate);
                if (result.isFullRecord()) {
                    StreamElement element = delegate.getInstance();
                    if (element.isRecord()) {
                        @SuppressWarnings("unchecked")
                        StreamRecord<Long> record = (StreamRecord<Long>) element;
                        values.add(record.getValue());
                    }
                }
                if (result.isBufferConsumed()) {
                    break;
                }
            }
        }
        return values;
    }
}

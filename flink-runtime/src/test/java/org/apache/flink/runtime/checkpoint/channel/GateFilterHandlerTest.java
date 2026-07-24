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
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, sourceBuffer, output);

        List<Long> values = readRecordsFromSerializer(output);
        assertThat(values).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, sourceBuffer, output);

        // No bytes should be written when all records are filtered out.
        assertThat(output.length()).isZero();
    }

    @Test
    void testPartialFiltering() throws Exception {
        RecordFilter<Long> keepEven = record -> record.getValue() % 2 == 0;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(keepEven);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L, 4L, 5L);
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, sourceBuffer, output);

        List<Long> values = readRecordsFromSerializer(output);
        assertThat(values).containsExactly(2L, 4L);
    }

    @Test
    void testEmptyBuffer() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer emptyBuffer = createEmptyBuffer();
        emptyBuffer.setSize(0);

        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, emptyBuffer, output);

        // No data written for an empty source buffer.
        assertThat(output.length()).isZero();
    }

    @Test
    void testSourceBufferRecycledOnSuccess() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, sourceBuffer, output);

        assertThat(sourceBuffer.isRecycled()).isTrue();
    }

    @Test
    void testSourceBufferRecycledWhenAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L);
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);
        handler.filterAndRewrite(0, 0, sourceBuffer, output);

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

    private Buffer createBufferWithRecords(Long... values) throws IOException {
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
    }

    private Buffer createEmptyBuffer() {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
    }

    /**
     * Deserializes the records the handler appended into {@code output}. The body format is
     * repeated (4B recordLen + N bytes of serialized StreamElement), which the deserializer reads
     * directly.
     */
    private List<Long> readRecordsFromSerializer(DataOutputSerializer output) throws Exception {
        List<Long> values = new ArrayList<>();
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        DeserializationDelegate<StreamElement> delegate =
                new NonReusingDeserializationDelegate<>(serializer);

        byte[] bodyBytes = output.getCopyOfBuffer();
        if (bodyBytes.length == 0) {
            return values;
        }
        MemorySegment memSeg = MemorySegmentFactory.allocateUnpooledSegment(bodyBytes.length);
        memSeg.put(0, bodyBytes);
        NetworkBuffer buf = new NetworkBuffer(memSeg, FreeingBufferRecycler.INSTANCE);
        buf.setSize(bodyBytes.length);

        SpillingAdaptiveSpanningRecordDeserializer<DeserializationDelegate<StreamElement>>
                deserializer =
                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                new String[] {System.getProperty("java.io.tmpdir")});
        deserializer.setNextBuffer(buf);

        RecordDeserializer.DeserializationResult result;
        do {
            result = deserializer.getNextRecord(delegate);
            if (result.isFullRecord()) {
                StreamElement element = delegate.getInstance();
                if (element.isRecord()) {
                    @SuppressWarnings("unchecked")
                    StreamRecord<Long> record = (StreamRecord<Long>) element;
                    values.add(record.getValue());
                }
            }
        } while (!result.isBufferConsumed());
        return values;
    }
}

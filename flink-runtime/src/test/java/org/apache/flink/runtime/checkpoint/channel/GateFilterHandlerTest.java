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
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    /**
     * On a fan-in rescale two old channels A and B fold into one new channel: A's high watermark is
     * held while B has none, then the merged output carries the group minimum, not A's higher one.
     */
    @Test
    void testFanInWatermarkMinMergeAndHold() throws Exception {
        SubtaskConnectionDescriptor keyA = new SubtaskConnectionDescriptor(0, 0);
        SubtaskConnectionDescriptor keyB = new SubtaskConnectionDescriptor(0, 1);
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createMergedHandler(keyA, keyB);

        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);

        // A delivers a record then a high watermark; B has not produced a watermark yet, so the
        // watermark must be suppressed at this point.
        handler.filterAndRewrite(
                0,
                0,
                createBufferWithElements(new StreamRecord<>(1L), new Watermark(100L)),
                output);
        // B delivers a record and a lower watermark; now the group can emit min(100, 30) = 30.
        handler.filterAndRewrite(
                0, 1, createBufferWithElements(new StreamRecord<>(2L), new Watermark(30L)), output);

        List<StreamElement> elements = readElementsFromSerializer(output);

        // Both records pass through the filter.
        assertThat(elements).filteredOn(StreamElement::isRecord).hasSize(2);
        // The only watermark emitted is the min (30) — the premature 100 is never written.
        assertThat(elements)
                .filteredOn(StreamElement::isWatermark)
                .extracting(e -> e.asWatermark().getTimestamp())
                .containsExactly(30L);
    }

    /**
     * An {@code IDLE} watermark status from a single merged old channel must not idle the whole new
     * channel while another merged old channel is still active: the aggregation emits {@code
     * ACTIVE}.
     */
    @Test
    void testFanInWatermarkStatusAnyActive() throws Exception {
        SubtaskConnectionDescriptor keyA = new SubtaskConnectionDescriptor(0, 0);
        SubtaskConnectionDescriptor keyB = new SubtaskConnectionDescriptor(0, 1);
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createMergedHandler(keyA, keyB);

        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);

        // A goes idle, but B is still active (default), so the merged status stays ACTIVE.
        handler.filterAndRewrite(0, 0, createBufferWithElements(WatermarkStatus.IDLE), output);

        List<StreamElement> elements = readElementsFromSerializer(output);
        assertThat(elements).hasSize(1);
        assertThat(elements.get(0).isWatermarkStatus()).isTrue();
        assertThat(elements.get(0).asWatermarkStatus().isActive()).isTrue();
    }

    // -------------------------------------------------------------------------------------------
    // Helper methods
    // -------------------------------------------------------------------------------------------

    /**
     * Builds a handler whose two old channels {@code keyA} / {@code keyB} fold into a single new
     * channel (they share one watermark merge group), mirroring a fan-in rescale.
     */
    private ChannelStateFilteringHandler.GateFilterHandler<Long> createMergedHandler(
            SubtaskConnectionDescriptor keyA, SubtaskConnectionDescriptor keyB) {
        VirtualChannel<Long> vcA = newVirtualChannel();
        VirtualChannel<Long> vcB = newVirtualChannel();

        Map<SubtaskConnectionDescriptor, VirtualChannel<Long>> channels = new HashMap<>();
        channels.put(keyA, vcA);
        channels.put(keyB, vcB);

        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        // new channel 0 folds old channels {0, 1}, so both keys share one watermark merge group.
        RescaleMappings channelMapping = RescaleMappings.of(Stream.of(new int[] {0, 1}), 2);
        return new ChannelStateFilteringHandler.GateFilterHandler<>(
                channels, serializer, channelMapping);
    }

    private VirtualChannel<Long> newVirtualChannel() {
        RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {System.getProperty("java.io.tmpdir")});
        return new VirtualChannel<>(deserializer, RecordFilter.acceptAll());
    }

    private Buffer createBufferWithElements(StreamElement... elements) throws IOException {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        DataOutputSerializer output = new DataOutputSerializer(BUFFER_SIZE);

        for (StreamElement element : elements) {
            DataOutputSerializer recordOutput = new DataOutputSerializer(64);
            serializer.serialize(element, recordOutput);
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

    private List<StreamElement> readElementsFromSerializer(DataOutputSerializer output)
            throws Exception {
        List<StreamElement> elements = new ArrayList<>();
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        DeserializationDelegate<StreamElement> delegate =
                new NonReusingDeserializationDelegate<>(serializer);

        byte[] bodyBytes = output.getCopyOfBuffer();
        if (bodyBytes.length == 0) {
            return elements;
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
                elements.add(delegate.getInstance());
            }
        } while (!result.isBufferConsumed());
        return elements;
    }

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
        return new ChannelStateFilteringHandler.GateFilterHandler<>(
                channels, serializer, RescaleMappings.SYMMETRIC_IDENTITY);
    }

    private Buffer createBufferWithRecords(Long... values) throws IOException {
        return createBufferWithElements(
                Arrays.stream(values).map(StreamRecord::new).toArray(StreamElement[]::new));
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

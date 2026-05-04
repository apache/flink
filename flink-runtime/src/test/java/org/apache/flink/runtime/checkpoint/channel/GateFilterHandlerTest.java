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
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChannelStateFilteringHandler.GateFilterHandler}. */
class GateFilterHandlerTest {

    private static final int BUFFER_SIZE = 1024;
    private static final SubtaskConnectionDescriptor KEY = new SubtaskConnectionDescriptor(0, 0);
    private static final InputChannelInfo TARGET_CHANNEL = new InputChannelInfo(0, 0);

    @TempDir private Path temporaryFolder;

    @Test
    void testAllRecordsPassFilter() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(TARGET_CHANNEL);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter(store);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        handler.filterAndRewrite(0, 0, sourceBuffer, outputWriter, TARGET_CHANNEL);
        outputWriter.flush();
        outputWriter.close();

        List<Long> values = drainAndDeserialize(store);
        assertThat(values).containsExactly(1L, 2L, 3L);
    }

    @Test
    void testAllRecordsFilteredOut() throws Exception {
        RecordFilter<Long> rejectAll = record -> false;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(rejectAll);

        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(TARGET_CHANNEL);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter(store);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L);
        handler.filterAndRewrite(0, 0, sourceBuffer, outputWriter, TARGET_CHANNEL);
        outputWriter.flush();
        outputWriter.close();

        List<Long> values = drainAndDeserialize(store);
        assertThat(values).isEmpty();
    }

    @Test
    void testPartialFiltering() throws Exception {
        RecordFilter<Long> keepEven = record -> record.getValue() % 2 == 0;
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler = createHandler(keepEven);

        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(TARGET_CHANNEL);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter(store);

        Buffer sourceBuffer = createBufferWithRecords(1L, 2L, 3L, 4L, 5L);
        handler.filterAndRewrite(0, 0, sourceBuffer, outputWriter, TARGET_CHANNEL);
        outputWriter.flush();
        outputWriter.close();

        List<Long> values = drainAndDeserialize(store);
        assertThat(values).containsExactly(2L, 4L);
    }

    @Test
    void testEmptyBuffer() throws Exception {
        ChannelStateFilteringHandler.GateFilterHandler<Long> handler =
                createHandler(RecordFilter.acceptAll());

        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(TARGET_CHANNEL);
        FilteredBufferDispatcher outputWriter = createTestOutputWriter(store);

        Buffer emptyBuffer = createEmptyBuffer();
        emptyBuffer.setSize(0);

        handler.filterAndRewrite(0, 0, emptyBuffer, outputWriter, TARGET_CHANNEL);
        outputWriter.flush();
        outputWriter.close();

        List<Long> values = drainAndDeserialize(store);
        assertThat(values).isEmpty();
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
        return new ChannelStateFilteringHandler.GateFilterHandler<>(channels, serializer);
    }

    private FilteredBufferDispatcher createTestOutputWriter(RecoveredBufferStoreImpl store)
            throws IOException {
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
        return createEmptyBuffer(BUFFER_SIZE);
    }

    private Buffer createEmptyBuffer(int size) {
        MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
        return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
    }

    /** Drains all buffers from the store and deserializes Long values. */
    private List<Long> drainAndDeserialize(RecoveredBufferStoreImpl store) throws IOException {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        SpillingAdaptiveSpanningRecordDeserializer<DeserializationDelegate<StreamElement>>
                deserializer =
                        new SpillingAdaptiveSpanningRecordDeserializer<>(
                                new String[] {System.getProperty("java.io.tmpdir")});
        DeserializationDelegate<StreamElement> delegate =
                new NonReusingDeserializationDelegate<>(serializer);

        List<Long> values = new ArrayList<>();
        Buffer buffer;
        while ((buffer = takeUnderLock(store)) != null) {
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

    private static Buffer takeUnderLock(RecoveredBufferStoreImpl store) {
        synchronized (store) {
            return store.tryTake();
        }
    }
}

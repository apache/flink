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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.collections.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.apache.flink.core.memory.MemorySegmentFactory.allocateUnpooledSegment;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.array;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.rescalingDescriptor;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.set;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.createBufferBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests {@link DemultiplexingRecordDeserializer}. */
public class DemultiplexingRecordDeserializerTest {
    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private IOManager ioManager;

    @Before
    public void setup() {
        ioManager = new IOManagerAsync();
    }

    @After
    public void cleanup() {
        ioManager = new IOManagerAsync();
    }

    /**
     * Tests {@link SubtaskConnectionDescriptor} by mixing buffers from 4 different virtual
     * channels.
     */
    @Test
    public void testUpscale() throws IOException {
        DemultiplexingRecordDeserializer<Long> deserializer =
                DemultiplexingRecordDeserializer.create(
                        new InputChannelInfo(2, 0),
                        rescalingDescriptor(
                                to(0, 1),
                                array(mappings(), mappings(), mappings(to(2, 3), to(4, 5))),
                                emptySet()),
                        unused ->
                                new SpillingAdaptiveSpanningRecordDeserializer<>(
                                        ioManager.getSpillingDirectoriesPaths()),
                        unused -> RecordFilter.all());

        assertEquals(
                Sets.newSet(
                        new SubtaskConnectionDescriptor(0, 2),
                        new SubtaskConnectionDescriptor(0, 3),
                        new SubtaskConnectionDescriptor(1, 2),
                        new SubtaskConnectionDescriptor(1, 3)),
                deserializer.getVirtualChannelSelectors());

        for (int i = 0; i < 100; i++) {
            SubtaskConnectionDescriptor selector =
                    Iterables.get(deserializer.getVirtualChannelSelectors(), random.nextInt(4));

            long start = selector.getInputSubtaskIndex() << 4 | selector.getOutputSubtaskIndex();

            MemorySegment memorySegment = allocateUnpooledSegment(128);
            try (BufferBuilder bufferBuilder = createBufferBuilder(memorySegment)) {
                Buffer buffer = writeLongs(bufferBuilder, start + 1L, start + 2L, start + 3L);

                deserializer.select(selector);
                deserializer.setNextBuffer(buffer);
            }

            assertEquals(
                    Arrays.asList(start + 1L, start + 2L, start + 3L), readLongs(deserializer));
            assertTrue(memorySegment.isFreed());
        }
    }

    /** Tests that {@link RecordFilter} are used correctly. */
    @Test
    public void testAmbiguousChannels() throws IOException {
        DemultiplexingRecordDeserializer<Long> deserializer =
                DemultiplexingRecordDeserializer.create(
                        new InputChannelInfo(1, 0),
                        rescalingDescriptor(
                                to(41, 42),
                                array(mappings(), mappings(to(2, 3), to(4, 5))),
                                set(42)),
                        unused ->
                                new SpillingAdaptiveSpanningRecordDeserializer<>(
                                        ioManager.getSpillingDirectoriesPaths()),
                        unused -> new RecordFilter(new ModSelector(2), LongSerializer.INSTANCE, 1));

        assertEquals(
                Sets.newSet(
                        new SubtaskConnectionDescriptor(41, 2),
                        new SubtaskConnectionDescriptor(41, 3),
                        new SubtaskConnectionDescriptor(42, 2),
                        new SubtaskConnectionDescriptor(42, 3)),
                deserializer.getVirtualChannelSelectors());

        for (int i = 0; i < 100; i++) {
            MemorySegment memorySegment = allocateUnpooledSegment(128);
            try (BufferBuilder bufferBuilder = createBufferBuilder(memorySegment)) {
                // add one even and one odd number
                Buffer buffer = writeLongs(bufferBuilder, i, i + 1L);

                SubtaskConnectionDescriptor selector =
                        Iterables.get(deserializer.getVirtualChannelSelectors(), i / 10 % 2);
                deserializer.select(selector);
                deserializer.setNextBuffer(buffer);

                if (selector.getInputSubtaskIndex() == 41) {
                    assertEquals(Arrays.asList((long) i, i + 1L), readLongs(deserializer));
                } else {
                    // only odd should occur in output
                    assertEquals(Arrays.asList(i / 2 * 2 + 1L), readLongs(deserializer));
                }
            }

            assertTrue(memorySegment.isFreed());
        }
    }

    /** Tests that Watermarks are only forwarded when all watermarks are received. */
    @Test
    public void testWatermarks() throws IOException {
        DemultiplexingRecordDeserializer<Long> deserializer =
                DemultiplexingRecordDeserializer.create(
                        new InputChannelInfo(0, 0),
                        rescalingDescriptor(
                                to(0, 1), array(mappings(to(0, 1), to(4, 5))), emptySet()),
                        unused ->
                                new SpillingAdaptiveSpanningRecordDeserializer<>(
                                        ioManager.getSpillingDirectoriesPaths()),
                        unused -> RecordFilter.all());

        assertEquals(4, deserializer.getVirtualChannelSelectors().size());

        for (Iterator<SubtaskConnectionDescriptor> iterator =
                        deserializer.getVirtualChannelSelectors().iterator();
                iterator.hasNext(); ) {
            SubtaskConnectionDescriptor selector = iterator.next();
            MemorySegment memorySegment = allocateUnpooledSegment(128);
            try (BufferBuilder bufferBuilder = createBufferBuilder(memorySegment)) {
                final long ts =
                        42L + selector.getInputSubtaskIndex() + selector.getOutputSubtaskIndex();
                Buffer buffer = write(bufferBuilder, new Watermark(ts));

                deserializer.select(selector);
                deserializer.setNextBuffer(buffer);
            }

            if (iterator.hasNext()) {
                assertEquals(Collections.emptyList(), read(deserializer));
            } else {
                // last channel, min should be 42 + 0 + 0
                assertEquals(Arrays.asList(new Watermark(42)), read(deserializer));
            }

            assertTrue(memorySegment.isFreed());
        }
    }

    private Buffer writeLongs(BufferBuilder bufferBuilder, long... elements) throws IOException {
        return write(
                bufferBuilder,
                Arrays.stream(elements).mapToObj(StreamRecord::new).toArray(StreamElement[]::new));
    }

    private Buffer write(BufferBuilder bufferBuilder, StreamElement... elements)
            throws IOException {
        try (BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumer()) {
            DataOutputSerializer output = new DataOutputSerializer(128);
            final SerializationDelegate<StreamElement> delegate =
                    new SerializationDelegate<>(
                            new StreamElementSerializer<>(LongSerializer.INSTANCE));
            for (StreamElement element : elements) {
                delegate.setInstance(element);
                bufferBuilder.appendAndCommit(RecordWriter.serializeRecord(output, delegate));
            }
            return bufferConsumer.build();
        }
    }

    private List<StreamElement> read(DemultiplexingRecordDeserializer<Long> deserializer)
            throws IOException {
        final NonReusingDeserializationDelegate<StreamElement> delegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(LongSerializer.INSTANCE));

        List<StreamElement> results = new ArrayList<>();
        RecordDeserializer.DeserializationResult result;
        do {
            result = deserializer.getNextRecord(delegate);
            if (result.isFullRecord()) {
                results.add(delegate.getInstance());
            }
        } while (!result.isBufferConsumed());

        return results;
    }

    private List<Long> readLongs(DemultiplexingRecordDeserializer<Long> deserializer)
            throws IOException {
        return read(deserializer).stream()
                .map(element -> element.<Long>asRecord().getValue())
                .collect(Collectors.toList());
    }

    private static class ModSelector
            implements ChannelSelector<SerializationDelegate<StreamRecord<Long>>> {
        private final int numberOfChannels;

        private ModSelector(int numberOfChannels) {
            this.numberOfChannels = numberOfChannels;
        }

        @Override
        public void setup(int numberOfChannels) {}

        @Override
        public int selectChannel(SerializationDelegate<StreamRecord<Long>> record) {
            return (int) (record.getInstance().getValue() % numberOfChannels);
        }

        @Override
        public boolean isBroadcast() {
            return false;
        }
    }
}

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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilter;
import org.apache.flink.streaming.runtime.io.recovery.VirtualChannel;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveredChannelStateHandlerFilterRoutingTest {

    @TempDir Path tempDir;

    private NetworkBufferPool networkBufferPool;
    private SingleInputGate inputGate;
    private InputChannelInfo channelInfo;

    @BeforeEach
    void setUp() {
        // Plenty of segments so the filter-on path does not deadlock on the bounded pool.
        networkBufferPool = new NetworkBufferPool(64, MemoryManager.DEFAULT_PAGE_SIZE);
        inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();
        channelInfo = new InputChannelInfo(0, 0);
    }

    @AfterEach
    void tearDown() throws Exception {
        inputGate.close();
        networkBufferPool.destroy();
    }

    @Test
    void testFilterOnRoutesOutputToChannelState() throws Exception {
        ChannelStateFilteringHandler filteringHandler = newPassThroughFilteringHandler();
        try (ChannelStateFilteringHandler ignored = filteringHandler) {
            SpillingWithFilteringHandler handler = newFilterOnHandler(filteringHandler);
            invokeRecoverWithRecords(handler, 1L, 2L, 3L);

            // Surviving records accumulate in the in-memory segment serializer; they are only
            // sealed and flushed to a spill file on channel switch or close. With a single channel
            // and no switch, close() is what seals the segment, so the assertion must follow it.
            handler.close();

            assertThat(handler.peekSpillFilesForTesting())
                    .as("filter-on path must spill the surviving records to a file")
                    .isNotEmpty();
        }
    }

    @Test
    void testFilterOnAccumulatorBuffersComeFromHeapNotPool() throws Exception {
        // The accumulator's prefilter + postfilter buffers are unpooled heap segments owned by
        // the handler — invoking filter recovery must NOT consume any network buffer pool
        // segments for the accumulator path. Channel-side exclusive buffer reservation, if any,
        // happens at handler construction and is already reflected in the pre-recover snapshot.
        ChannelStateFilteringHandler filteringHandler = newPassThroughFilteringHandler();
        SpillingWithFilteringHandler handler = newFilterOnHandler(filteringHandler);
        try (ChannelStateFilteringHandler ignored = filteringHandler) {
            int availableBeforeRecover = networkBufferPool.getNumberOfAvailableMemorySegments();

            invokeRecoverWithRecords(handler, 1L, 2L, 3L);

            int availableAfterRecover = networkBufferPool.getNumberOfAvailableMemorySegments();
            assertThat(availableAfterRecover)
                    .as("filter accumulator buffers must not be sourced from the network pool")
                    .isEqualTo(availableBeforeRecover);

            handler.close();
            inputGate.close();
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                    .as("pool count after close must match pre-recover (filter took nothing)")
                    .isEqualTo(availableBeforeRecover);
        }
    }

    @Test
    void testFilterOnDoesNotInvokeChannelOnRecoveredStateBuffer() throws Exception {
        ChannelStateFilteringHandler filteringHandler = newPassThroughFilteringHandler();
        try (ChannelStateFilteringHandler ignored = filteringHandler;
                SpillingWithFilteringHandler handler = newFilterOnHandler(filteringHandler)) {
            invokeRecoverWithRecords(handler, 1L, 2L, 3L);

            int queuedDuringRecovery = countQueuedRecoveredBuffers();
            assertThat(queuedDuringRecovery)
                    .as("filter-on must not enqueue buffers into the channel during recovery")
                    .isEqualTo(0);
        }
    }

    @Test
    void testFilterOffDoesNotCreateChannelState() throws Exception {
        try (NoSpillingHandler handler = newFilterOffHandler()) {
            invokeRecoverWithRawBytes(handler, new byte[] {1, 2, 3, 4});

            // NoSpillingHandler has no channelStateWriter, so peek always returns null.
            assertThat(handler.getProducedChannelState())
                    .as("filter-off close() must not publish a FetchedChannelState either")
                    .isNull();
        }
    }

    @Test
    void testFilterOffMaintainsMasterBehavior() throws Exception {
        try (NoSpillingHandler handler = newFilterOffHandler()) {
            invokeRecoverWithRawBytes(handler, new byte[] {1, 2, 3, 4});

            // Filter-off path enqueues the SubtaskConnectionDescriptor event plus the data
            // buffer directly into the channel's recoveredBuffers.
            int queued = countQueuedRecoveredBuffers();
            assertThat(queued)
                    .as("filter-off must enqueue the descriptor + data buffer into the channel")
                    .isGreaterThanOrEqualTo(2);
        }
    }

    private SpillingWithFilteringHandler newFilterOnHandler(
            ChannelStateFilteringHandler filteringHandler) {
        // FLINK-38544 transitional: constructed directly because the factory still routes the
        // flag-on filtering case to the in-memory FilteringHandler; goes back through
        // AbstractInputChannelRecoveredStateHandler.create(...) when the spilling backend lands.
        return new SpillingWithFilteringHandler(
                new InputGate[] {inputGate},
                identityRescalingForOneGate(),
                filteringHandler,
                MemoryManager.DEFAULT_PAGE_SIZE,
                new String[] {tempDir.toString()});
    }

    private NoSpillingHandler newFilterOffHandler() {
        return (NoSpillingHandler)
                AbstractInputChannelRecoveredStateHandler.create(
                        new InputGate[] {inputGate},
                        identityRescalingForOneGate(),
                        false,
                        null,
                        MemoryManager.DEFAULT_PAGE_SIZE,
                        null);
    }

    private static InflightDataRescalingDescriptor identityRescalingForOneGate() {
        return new InflightDataRescalingDescriptor(
                new InflightDataRescalingDescriptor.InflightDataGateOrPartitionRescalingDescriptor
                        [] {
                    new InflightDataRescalingDescriptor
                            .InflightDataGateOrPartitionRescalingDescriptor(
                            new int[] {1},
                            RescaleMappings.identity(1, 1),
                            new HashSet<>(),
                            InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor.MappingType
                                    .IDENTITY)
                });
    }

    private ChannelStateFilteringHandler newPassThroughFilteringHandler() {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                new SpillingAdaptiveSpanningRecordDeserializer<>(
                        new String[] {System.getProperty("java.io.tmpdir")});
        VirtualChannel<Long> vc = new VirtualChannel<>(deserializer, RecordFilter.acceptAll());
        Map<SubtaskConnectionDescriptor, VirtualChannel<Long>> channels = new HashMap<>();
        // The handler will invoke filterAndRewrite with oldSubtaskIndex=1 — keep the key aligned.
        channels.put(new SubtaskConnectionDescriptor(1, channelInfo.getInputChannelIdx()), vc);

        ChannelStateFilteringHandler.GateFilterHandler<Long> gateHandler =
                new ChannelStateFilteringHandler.GateFilterHandler<>(channels, serializer);
        return new ChannelStateFilteringHandler(
                new ChannelStateFilteringHandler.GateFilterHandler<?>[] {gateHandler});
    }

    private void invokeRecoverWithRecords(
            AbstractInputChannelRecoveredStateHandler handler, Long... values) throws Exception {
        Buffer source = createRecordBuffer(values);
        invokeRecoverWithBuffer(handler, source);
    }

    private void invokeRecoverWithRawBytes(
            AbstractInputChannelRecoveredStateHandler handler, byte[] data) throws Exception {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(data.length);
        seg.put(0, data);
        NetworkBuffer source = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
        source.setSize(data.length);
        invokeRecoverWithBuffer(handler, source);
    }

    /**
     * Mirrors the chunkReader's getBuffer + recover sequence: handler-issued buffer is filled with
     * the source data, then handed back to recover.
     */
    private void invokeRecoverWithBuffer(
            AbstractInputChannelRecoveredStateHandler handler, Buffer source) throws Exception {
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bwc = handler.getBuffer(channelInfo);
        try {
            Buffer dest = bwc.context;
            int len = source.readableBytes();
            source.getMemorySegment()
                    .copyTo(
                            source.getMemorySegmentOffset(),
                            dest.getMemorySegment(),
                            dest.getMemorySegmentOffset(),
                            len);
            dest.setSize(len);
        } finally {
            source.recycleBuffer();
        }
        // oldSubtaskIndex=1 matches the pass-through filter's virtual channel key. The filter-off
        // path ignores this argument's mapping (it only flows into a SubtaskConnectionDescriptor).
        handler.recover(channelInfo, 1, bwc);
    }

    private static Buffer createRecordBuffer(Long... values) throws IOException {
        StreamElementSerializer<Long> serializer =
                new StreamElementSerializer<>(LongSerializer.INSTANCE);
        DataOutputSerializer output = new DataOutputSerializer(256);
        for (Long v : values) {
            DataOutputSerializer rec = new DataOutputSerializer(64);
            serializer.serialize(new StreamRecord<>(v), rec);
            int recordLength = rec.length();
            output.writeInt(recordLength);
            output.write(rec.getSharedBuffer(), 0, recordLength);
        }
        byte[] data = output.getCopyOfBuffer();
        MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
        segment.put(0, data, 0, data.length);
        NetworkBuffer buf = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buf.setSize(data.length);
        return buf;
    }

    /**
     * Counts the number of buffers currently queued in the only recovered input channel by draining
     * via the public {@code getNextBuffer()} entry point. After this helper returns the channel
     * queue is empty by definition; tests should not call it twice expecting the same answer.
     */
    private int countQueuedRecoveredBuffers() throws IOException {
        RecoveredInputChannel ch = (RecoveredInputChannel) inputGate.getChannel(0);
        int count = 0;
        while (true) {
            Optional<InputChannel.BufferAndAvailability> next = ch.getNextBuffer();
            if (!next.isPresent()) {
                break;
            }
            count++;
            next.get().buffer().recycleBuffer();
            if (count > 1000) {
                throw new IllegalStateException("Unexpected unbounded queue contents");
            }
        }
        return count;
    }
}

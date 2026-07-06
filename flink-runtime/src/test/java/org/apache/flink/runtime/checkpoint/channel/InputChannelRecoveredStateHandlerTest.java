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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of different implementation of {@link AbstractInputChannelRecoveredStateHandler}. */
class InputChannelRecoveredStateHandlerTest extends RecoveredChannelStateHandlerTest {
    @TempDir private Path tmpDir;

    private static final int preAllocatedSegments = 3;
    private NetworkBufferPool networkBufferPool;
    private SingleInputGate inputGate;
    // NoSpillingHandler: checkpointingDuringRecoveryEnabled=false, filteringHandler=null
    private AbstractInputChannelRecoveredStateHandler icsHandler;
    private InputChannelInfo channelInfo;

    @BeforeEach
    void setUp() {
        // given: Segment provider with defined number of allocated segments.
        networkBufferPool =
                new NetworkBufferPool(preAllocatedSegments, MemoryManager.DEFAULT_PAGE_SIZE);

        // and: Configured input gate with recovered channel.
        inputGate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        icsHandler = buildNoSpillingHandler(inputGate);

        channelInfo = new InputChannelInfo(0, 0);
    }

    private AbstractInputChannelRecoveredStateHandler buildNoSpillingHandler(
            SingleInputGate inputGate) {
        return AbstractInputChannelRecoveredStateHandler.create(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }),
                false,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                null);
    }

    private AbstractInputChannelRecoveredStateHandler buildMultiChannelHandler() {
        // Setup multi-channel scenario to trigger distribution constraint validation
        SingleInputGate multiChannelGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(2)
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        return AbstractInputChannelRecoveredStateHandler.create(
                new InputGate[] {multiChannelGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {2},
                                    // Force 1:many mapping after inversion
                                    mappings(to(0), to(0)),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.RESCALING)
                        }),
                false,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                null);
    }

    /** Builds a handler in filtering mode (non-null filtering handler, no-op stub). */
    private SpillingWithFilteringHandler buildFilteringInputChannelStateHandler() {
        // Empty GateFilterHandler array: filtering is "enabled" structurally, but no gate-level
        // filter logic runs. Suitable for exercising getBuffer() routing only.
        ChannelStateFilteringHandler stubFilteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler[0]);
        return (SpillingWithFilteringHandler)
                AbstractInputChannelRecoveredStateHandler.create(
                        new InputGate[] {inputGate},
                        new InflightDataRescalingDescriptor(
                                new InflightDataRescalingDescriptor
                                                .InflightDataGateOrPartitionRescalingDescriptor[] {
                                    new InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor(
                                            new int[] {1},
                                            RescaleMappings.identity(1, 1),
                                            new HashSet<>(),
                                            InflightDataRescalingDescriptor
                                                    .InflightDataGateOrPartitionRescalingDescriptor
                                                    .MappingType.IDENTITY)
                                }),
                        true,
                        stubFilteringHandler,
                        MemoryManager.DEFAULT_PAGE_SIZE,
                        new String[] {tmpDir.toAbsolutePath().toString()});
    }

    private AbstractInputChannelRecoveredStateHandler buildSpillingNoFilteringHandler(
            String[] spillTmpDirectories) {
        return AbstractInputChannelRecoveredStateHandler.create(
                new InputGate[] {inputGate},
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {1},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }),
                true,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                spillTmpDirectories);
    }

    @Test
    void testBufferDistributedToMultipleInputChannelsThrowsException() throws Exception {
        // Test constraint that prevents buffer distribution to multiple channels
        try (AbstractInputChannelRecoveredStateHandler handler = buildMultiChannelHandler()) {
            assertThatThrownBy(() -> handler.getBuffer(channelInfo))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(
                            "One buffer is only distributed to one target InputChannel since "
                                    + "one buffer is expected to be processed once by the same task.");
        }
    }

    @Test
    void testRecycleBufferBeforeRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        bufferWithContext.buffer.close();

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    @Test
    void testRecycleBufferAfterRecoverWasCalled() throws Exception {
        // when: Request the buffer.
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);

        // and: Recycle buffer outside.
        icsHandler.recover(channelInfo, 0, bufferWithContext);

        // Close the gate for flushing the cached recycled buffers to the segment provider.
        inputGate.close();

        // then: All pre-allocated segments should be successfully recycled.
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(preAllocatedSegments);
    }

    @Test
    void testPreFilterBufferIsolationFromNetworkBufferPool() throws Exception {
        try (SpillingWithFilteringHandler filteringHandler =
                buildFilteringInputChannelStateHandler()) {
            int availableBefore = networkBufferPool.getNumberOfAvailableMemorySegments();

            RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                    filteringHandler.getBuffer(channelInfo);
            try {
                Buffer buffer = bufferWithContext.context;
                // Heap-backed: NetworkBuffer wrapping a heap (non off-heap) segment.
                assertThat(buffer).isInstanceOf(NetworkBuffer.class);
                assertThat(buffer.getMemorySegment().isOffHeap()).isFalse();
                assertThat(buffer.getMemorySegment().size())
                        .isEqualTo(MemoryManager.DEFAULT_PAGE_SIZE);
                // Pool is untouched.
                assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                        .isEqualTo(availableBefore);
            } finally {
                bufferWithContext.context.recycleBuffer();
            }
        }
    }

    @Test
    void testNonFilteringModeUsesNetworkBufferPool() throws Exception {
        int availableBefore = networkBufferPool.getNumberOfAvailableMemorySegments();

        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                icsHandler.getBuffer(channelInfo);
        try {
            Buffer buffer = bufferWithContext.context;
            // Pool allocation reduces available count.
            assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                    .isLessThan(availableBefore);
            // Memory segment comes from pre-allocated pool (off-heap).
            assertThat(buffer.getMemorySegment().isOffHeap()).isTrue();
        } finally {
            bufferWithContext.context.recycleBuffer();
        }
    }

    @Test
    void testPreFilterSegmentReusedAcrossCalls() throws Exception {
        try (SpillingWithFilteringHandler filteringHandler =
                buildFilteringInputChannelStateHandler()) {
            // First getBuffer() lazily allocates the segment.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> first =
                    filteringHandler.getBuffer(channelInfo);
            MemorySegment segment1 = first.context.getMemorySegment();
            first.context.recycleBuffer();

            // Second getBuffer() must reuse the same segment instance.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> second =
                    filteringHandler.getBuffer(channelInfo);
            MemorySegment segment2 = second.context.getMemorySegment();
            second.context.recycleBuffer();

            assertThat(segment2).isSameAs(segment1);
            // Third call, same check.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> third =
                    filteringHandler.getBuffer(channelInfo);
            MemorySegment segment3 = third.context.getMemorySegment();
            third.context.recycleBuffer();

            assertThat(segment3).isSameAs(segment1);

            // Internal assertion: inUse flipped back to false after each recycle.
            assertThat(filteringHandler.isPreFilterBufferInUse()).isFalse();
        }
    }

    @Test
    void testGetBufferThrowsWhenPriorBufferNotRecycled() throws Exception {
        try (SpillingWithFilteringHandler filteringHandler =
                buildFilteringInputChannelStateHandler()) {
            RecoveredChannelStateHandler.BufferWithContext<Buffer> first =
                    filteringHandler.getBuffer(channelInfo);
            try {
                assertThat(filteringHandler.isPreFilterBufferInUse()).isTrue();

                // Without recycling, requesting another buffer must fail.
                assertThatThrownBy(() -> filteringHandler.getBuffer(channelInfo))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Previous pre-filter buffer has not been recycled");
            } finally {
                first.context.recycleBuffer();
            }

            // After recycling, a new getBuffer() succeeds.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> second =
                    filteringHandler.getBuffer(channelInfo);
            second.context.recycleBuffer();
        }
    }

    @Test
    void testPreFilterSegmentFreedOnClose() throws Exception {
        SpillingWithFilteringHandler filteringHandler = buildFilteringInputChannelStateHandler();
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                filteringHandler.getBuffer(channelInfo);
        bufferWithContext.context.recycleBuffer();

        MemorySegment segment = filteringHandler.getPreFilterSegmentForTesting();
        assertThat(segment).isNotNull();
        assertThat(segment.isFreed()).isFalse();

        filteringHandler.close();

        assertThat(segment.isFreed()).isTrue();
        assertThat(filteringHandler.getPreFilterSegmentForTesting()).isNull();
    }

    @Test
    void testSpillingHandlerRequiresSpillDirectories() {
        assertThatThrownBy(() -> buildSpillingNoFilteringHandler(null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> buildSpillingNoFilteringHandler(new String[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spillTmpDirectories must not be empty");
    }

    // -------------------------------------------------------------------------------------------
    // Filter-on / filter-off routing.
    //
    // These use a dedicated large-pool fixture (a pass-through filter on filter-on must not
    // deadlock on a bounded pool), independent of the small-pool fixture in setUp().
    // -------------------------------------------------------------------------------------------

    @Test
    void testFilterOnRoutesOutputToChannelState() throws Exception {
        try (RoutingFixture fx = newRoutingFixture()) {
            ChannelStateFilteringHandler filteringHandler = fx.newPassThroughFilteringHandler();
            try (ChannelStateFilteringHandler ignored = filteringHandler) {
                SpillingWithFilteringHandler handler = fx.newFilterOnHandler(filteringHandler);
                fx.invokeRecoverWithRecords(handler, 1L, 2L, 3L);

                // Surviving records accumulate in the in-memory segment serializer; they are only
                // sealed and flushed to a spill file on channel switch or close. With a single
                // channel and no switch, close() is what seals the segment, so the assertion must
                // follow it.
                handler.close();

                assertThat(handler.peekSpillFilesForTesting())
                        .as("filter-on path must spill the surviving records to a file")
                        .isNotEmpty();
            }
        }
    }

    @Test
    void testFilterOnAccumulatorBuffersComeFromHeapNotPool() throws Exception {
        try (RoutingFixture fx = newRoutingFixture()) {
            // The accumulator's prefilter + postfilter buffers are unpooled heap segments owned by
            // the handler — invoking filter recovery must NOT consume any network buffer pool
            // segments for the accumulator path.
            ChannelStateFilteringHandler filteringHandler = fx.newPassThroughFilteringHandler();
            SpillingWithFilteringHandler handler = fx.newFilterOnHandler(filteringHandler);
            try (ChannelStateFilteringHandler ignored = filteringHandler) {
                int availableBeforeRecover = fx.pool.getNumberOfAvailableMemorySegments();

                fx.invokeRecoverWithRecords(handler, 1L, 2L, 3L);

                assertThat(fx.pool.getNumberOfAvailableMemorySegments())
                        .as("filter accumulator buffers must not be sourced from the network pool")
                        .isEqualTo(availableBeforeRecover);

                handler.close();
                fx.gate.close();
                assertThat(fx.pool.getNumberOfAvailableMemorySegments())
                        .as("pool count after close must match pre-recover (filter took nothing)")
                        .isEqualTo(availableBeforeRecover);
            }
        }
    }

    @Test
    void testFilterOnDoesNotInvokeChannelOnRecoveredStateBuffer() throws Exception {
        try (RoutingFixture fx = newRoutingFixture()) {
            ChannelStateFilteringHandler filteringHandler = fx.newPassThroughFilteringHandler();
            try (ChannelStateFilteringHandler ignored = filteringHandler;
                    SpillingWithFilteringHandler handler =
                            fx.newFilterOnHandler(filteringHandler)) {
                fx.invokeRecoverWithRecords(handler, 1L, 2L, 3L);

                assertThat(fx.countQueuedRecoveredBuffers())
                        .as("filter-on must not enqueue buffers into the channel during recovery")
                        .isEqualTo(0);
            }
        }
    }

    @Test
    void testFilterOffMaintainsMasterBehavior() throws Exception {
        try (RoutingFixture fx = newRoutingFixture();
                NoSpillingHandler handler = fx.newFilterOffHandler()) {
            fx.invokeRecoverWithRawBytes(handler, new byte[] {1, 2, 3, 4});

            // Filter-off path enqueues the SubtaskConnectionDescriptor event plus the data buffer
            // directly into the channel's recoveredBuffers.
            assertThat(fx.countQueuedRecoveredBuffers())
                    .as("filter-off must enqueue the descriptor + data buffer into the channel")
                    .isGreaterThanOrEqualTo(2);
        }
    }

    private RoutingFixture newRoutingFixture() {
        return new RoutingFixture();
    }

    /**
     * Self-contained fixture for the filter routing tests: a large network buffer pool (so a
     * pass-through filter-on path does not deadlock on a bounded pool) with its own recovered input
     * gate and channel.
     */
    private static final class RoutingFixture implements AutoCloseable {
        private final NetworkBufferPool pool =
                new NetworkBufferPool(64, MemoryManager.DEFAULT_PAGE_SIZE);
        private final SingleInputGate gate =
                new SingleInputGateBuilder()
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(pool)
                        .build();
        private final InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        private final Path spillDir;

        RoutingFixture() {
            try {
                spillDir = java.nio.file.Files.createTempDirectory("filter-routing-");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        SpillingWithFilteringHandler newFilterOnHandler(
                ChannelStateFilteringHandler filteringHandler) {
            return (SpillingWithFilteringHandler)
                    AbstractInputChannelRecoveredStateHandler.create(
                            new InputGate[] {gate},
                            identityRescalingForOneGate(),
                            true,
                            filteringHandler,
                            MemoryManager.DEFAULT_PAGE_SIZE,
                            new String[] {spillDir.toString()});
        }

        NoSpillingHandler newFilterOffHandler() {
            return (NoSpillingHandler)
                    AbstractInputChannelRecoveredStateHandler.create(
                            new InputGate[] {gate},
                            identityRescalingForOneGate(),
                            false,
                            null,
                            MemoryManager.DEFAULT_PAGE_SIZE,
                            null);
        }

        ChannelStateFilteringHandler newPassThroughFilteringHandler() {
            StreamElementSerializer<Long> serializer =
                    new StreamElementSerializer<>(LongSerializer.INSTANCE);
            RecordDeserializer<DeserializationDelegate<StreamElement>> deserializer =
                    new SpillingAdaptiveSpanningRecordDeserializer<>(
                            new String[] {System.getProperty("java.io.tmpdir")});
            VirtualChannel<Long> vc = new VirtualChannel<>(deserializer, RecordFilter.acceptAll());
            Map<SubtaskConnectionDescriptor, VirtualChannel<Long>> channels = new HashMap<>();
            // The handler invokes filterAndRewrite with oldSubtaskIndex=1 — keep the key aligned.
            channels.put(new SubtaskConnectionDescriptor(1, channelInfo.getInputChannelIdx()), vc);

            ChannelStateFilteringHandler.GateFilterHandler<Long> gateHandler =
                    new ChannelStateFilteringHandler.GateFilterHandler<>(channels, serializer);
            return new ChannelStateFilteringHandler(
                    new ChannelStateFilteringHandler.GateFilterHandler<?>[] {gateHandler});
        }

        void invokeRecoverWithRecords(
                AbstractInputChannelRecoveredStateHandler handler, Long... values)
                throws Exception {
            invokeRecoverWithBuffer(handler, createRecordBuffer(values));
        }

        void invokeRecoverWithRawBytes(
                AbstractInputChannelRecoveredStateHandler handler, byte[] data) throws Exception {
            MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(data.length);
            seg.put(0, data);
            NetworkBuffer source = new NetworkBuffer(seg, FreeingBufferRecycler.INSTANCE);
            source.setSize(data.length);
            invokeRecoverWithBuffer(handler, source);
        }

        /**
         * Mirrors the chunkReader's getBuffer + recover sequence: the handler-issued buffer is
         * filled with the source data, then handed back to recover.
         */
        private void invokeRecoverWithBuffer(
                AbstractInputChannelRecoveredStateHandler handler, Buffer source) throws Exception {
            RecoveredChannelStateHandler.BufferWithContext<Buffer> bwc =
                    handler.getBuffer(channelInfo);
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
            // oldSubtaskIndex=1 matches the pass-through filter's virtual channel key. The
            // filter-off path ignores this argument's mapping (it only flows into a
            // SubtaskConnectionDescriptor).
            handler.recover(channelInfo, 1, bwc);
        }

        private Buffer createRecordBuffer(Long... values) throws IOException {
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
         * Counts the buffers currently queued in the only recovered input channel by draining via
         * the public {@code getNextBuffer()} entry point. After this returns the channel queue is
         * empty by definition.
         */
        int countQueuedRecoveredBuffers() throws IOException {
            RecoveredInputChannel ch = (RecoveredInputChannel) gate.getChannel(0);
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

        private static InflightDataRescalingDescriptor identityRescalingForOneGate() {
            return new InflightDataRescalingDescriptor(
                    new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor[] {
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

        @Override
        public void close() throws Exception {
            gate.close();
            pool.destroy();
        }
    }
}

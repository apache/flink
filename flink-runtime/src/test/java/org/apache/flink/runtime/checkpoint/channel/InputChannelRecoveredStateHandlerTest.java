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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.mappings;
import static org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptorUtil.to;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of different implementation of {@link InputChannelRecoveredStateHandler}. */
class InputChannelRecoveredStateHandlerTest extends RecoveredChannelStateHandlerTest {
    private static final int preAllocatedSegments = 3;
    private NetworkBufferPool networkBufferPool;
    private SingleInputGate inputGate;
    private InputChannelRecoveredStateHandler icsHandler;
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

        icsHandler = buildInputChannelStateHandler(inputGate);

        channelInfo = new InputChannelInfo(0, 0);
    }

    private InputChannelRecoveredStateHandler buildInputChannelStateHandler(
            SingleInputGate inputGate) {
        return new InputChannelRecoveredStateHandler(
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
                null,
                MemoryManager.DEFAULT_PAGE_SIZE);
    }

    private InputChannelRecoveredStateHandler buildMultiChannelHandler() {
        // Setup multi-channel scenario to trigger distribution constraint validation
        SingleInputGate multiChannelGate =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(2)
                        .setChannelFactory(InputChannelBuilder::buildLocalRecoveredChannel)
                        .setSegmentProvider(networkBufferPool)
                        .build();

        return new InputChannelRecoveredStateHandler(
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
                null,
                MemoryManager.DEFAULT_PAGE_SIZE);
    }

    /** Builds a handler in filtering mode (non-null filtering handler, no-op stub). */
    private InputChannelRecoveredStateHandler buildFilteringInputChannelStateHandler() {
        // Empty GateFilterHandler array: filtering is "enabled" structurally, but no gate-level
        // filter logic runs. Suitable for exercising getBuffer() routing only.
        ChannelStateFilteringHandler stubFilteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler[0]);
        return new InputChannelRecoveredStateHandler(
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
                stubFilteringHandler,
                MemoryManager.DEFAULT_PAGE_SIZE);
    }

    @Test
    void testBufferDistributedToMultipleInputChannelsThrowsException() throws Exception {
        // Test constraint that prevents buffer distribution to multiple channels
        try (InputChannelRecoveredStateHandler handler = buildMultiChannelHandler()) {
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
        try (InputChannelRecoveredStateHandler filteringHandler =
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
        try (InputChannelRecoveredStateHandler filteringHandler =
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
        try (InputChannelRecoveredStateHandler filteringHandler =
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
        InputChannelRecoveredStateHandler filteringHandler =
                buildFilteringInputChannelStateHandler();
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
}

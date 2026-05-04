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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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
                false,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                null);
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
                false,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                null);
    }

    /** Feature on, no filter — raw passthrough mode (heap-segment buffers). */
    private InputChannelRecoveredStateHandler buildFeatureEnabledStateHandler() {
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
                true,
                null,
                MemoryManager.DEFAULT_PAGE_SIZE,
                null);
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
        try (InputChannelRecoveredStateHandler recoveredStateHandler =
                buildFeatureEnabledStateHandler()) {
            int availableBefore = networkBufferPool.getNumberOfAvailableMemorySegments();

            RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                    recoveredStateHandler.getBuffer(channelInfo);
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
        try (InputChannelRecoveredStateHandler recoveredStateHandler =
                buildFeatureEnabledStateHandler()) {
            // First getBuffer() lazily allocates the segment.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> first =
                    recoveredStateHandler.getBuffer(channelInfo);
            MemorySegment segment1 = first.context.getMemorySegment();
            first.context.recycleBuffer();

            // Second getBuffer() must reuse the same segment instance.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> second =
                    recoveredStateHandler.getBuffer(channelInfo);
            MemorySegment segment2 = second.context.getMemorySegment();
            second.context.recycleBuffer();

            assertThat(segment2).isSameAs(segment1);
            // Third call, same check.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> third =
                    recoveredStateHandler.getBuffer(channelInfo);
            MemorySegment segment3 = third.context.getMemorySegment();
            third.context.recycleBuffer();

            assertThat(segment3).isSameAs(segment1);

            // Internal assertion: inUse flipped back to false after each recycle.
            assertThat(recoveredStateHandler.isPreFilterBufferInUse()).isFalse();
        }
    }

    @Test
    void testGetBufferThrowsWhenPriorBufferNotRecycled() throws Exception {
        try (InputChannelRecoveredStateHandler recoveredStateHandler =
                buildFeatureEnabledStateHandler()) {
            RecoveredChannelStateHandler.BufferWithContext<Buffer> first =
                    recoveredStateHandler.getBuffer(channelInfo);
            try {
                assertThat(recoveredStateHandler.isPreFilterBufferInUse()).isTrue();

                // Without recycling, requesting another buffer must fail.
                assertThatThrownBy(() -> recoveredStateHandler.getBuffer(channelInfo))
                        .isInstanceOf(IllegalStateException.class)
                        .hasMessageContaining("Previous pre-filter buffer has not been recycled");
            } finally {
                first.context.recycleBuffer();
            }

            // After recycling, a new getBuffer() succeeds.
            RecoveredChannelStateHandler.BufferWithContext<Buffer> second =
                    recoveredStateHandler.getBuffer(channelInfo);
            second.context.recycleBuffer();
        }
    }

    @Test
    void testPreFilterSegmentFreedOnClose() throws Exception {
        InputChannelRecoveredStateHandler recoveredStateHandler = buildFeatureEnabledStateHandler();
        RecoveredChannelStateHandler.BufferWithContext<Buffer> bufferWithContext =
                recoveredStateHandler.getBuffer(channelInfo);
        bufferWithContext.context.recycleBuffer();

        MemorySegment segment = recoveredStateHandler.getPreFilterSegmentForTesting();
        assertThat(segment).isNotNull();
        assertThat(segment.isFreed()).isFalse();

        recoveredStateHandler.close();

        assertThat(segment.isFreed()).isTrue();
        assertThat(recoveredStateHandler.getPreFilterSegmentForTesting()).isNull();
    }

    /**
     * AT-FRCV (input half): finishRecovery() triggers finishReadRecoveredState() on every gate
     * exactly once; close() must NOT invoke it again (close is pure resource release).
     *
     * <p>The internal {@code recoveryFinished} idempotency guard is verified via reflection. We
     * also verify that the pre-filter segment — allocated during recovery — is freed by close() and
     * not by finishRecovery(), confirming the clean separation of lifecycle concerns.
     */
    @Test
    void testFinishRecoveryTriggersConversion() throws Exception {
        InputChannelRecoveredStateHandler recoveredStateHandler = buildFeatureEnabledStateHandler();

        // Allocate and recycle a pre-filter buffer so preFilterSegment is live before close().
        RecoveredChannelStateHandler.BufferWithContext<Buffer> buf =
                recoveredStateHandler.getBuffer(channelInfo);
        buf.context.recycleBuffer();
        MemorySegment segmentBeforeFinish = recoveredStateHandler.getPreFilterSegmentForTesting();
        assertThat(segmentBeforeFinish).isNotNull();

        // Before finishRecovery(): recoveryFinished == false.
        assertThat(getRecoveryFinishedFlag(recoveredStateHandler)).isFalse();

        // finishRecovery() must complete without error and flip the guard.
        recoveredStateHandler.finishRecovery();
        assertThat(getRecoveryFinishedFlag(recoveredStateHandler)).isTrue();

        // Idempotency: second call keeps the guard at true and does not re-invoke the gate loop.
        recoveredStateHandler.finishRecovery();
        assertThat(getRecoveryFinishedFlag(recoveredStateHandler)).isTrue();

        // close() is pure resource release: segment freed, guard unchanged (still true).
        recoveredStateHandler.close();
        assertThat(getRecoveryFinishedFlag(recoveredStateHandler))
                .as("close() must not alter the recoveryFinished flag")
                .isTrue();
        assertThat(recoveredStateHandler.getPreFilterSegmentForTesting())
                .as("close() must null-out the preFilterSegment reference")
                .isNull();
        assertThat(segmentBeforeFinish.isFreed())
                .as("close() must free the preFilterSegment")
                .isTrue();
    }

    /** Reads the private {@code recoveryFinished} field via reflection. */
    private static boolean getRecoveryFinishedFlag(InputChannelRecoveredStateHandler handler)
            throws Exception {
        java.lang.reflect.Field field =
                InputChannelRecoveredStateHandler.class.getDeclaredField("recoveryFinished");
        field.setAccessible(true);
        return (boolean) field.get(handler);
    }

    /** Raw passthrough forwards source bytes verbatim to the mapped target channel. */
    @Test
    void testRecoverRawPassthroughForwardsBufferBytesToDispatcher() throws Exception {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        try (InputChannelRecoveredStateHandler handler =
                buildHandler(true, null, dispatcher, inputGate)) {
            byte[] payload = new byte[] {7, 11, 13, 17, 19};
            RecoveredChannelStateHandler.BufferWithContext<Buffer> ctx =
                    handler.getBuffer(channelInfo);
            ctx.context.asByteBuf().writeBytes(payload);

            handler.recover(channelInfo, 0, ctx);

            assertThat(dispatcher.writes).hasSize(1);
            RecordingDispatcher.Write w = dispatcher.writes.get(0);
            assertThat(w.length).isEqualTo(payload.length);
            assertThat(w.channelInfo)
                    .as("dispatcher receives the mapped target channel info")
                    .isEqualTo(new InputChannelInfo(0, 0));
            assertThat(Arrays.copyOf(w.data, w.length)).containsExactly(payload);
        }
    }

    /** Filtering needs a sink — fail at construction rather than NPE deep in recovery. */
    @Test
    void testConstructorRejectsFilteringHandlerWithoutDispatcher() {
        ChannelStateFilteringHandler filteringHandler =
                new ChannelStateFilteringHandler(
                        new ChannelStateFilteringHandler.GateFilterHandler[0]);
        assertThatThrownBy(() -> buildHandler(true, filteringHandler, null, inputGate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("filteringHandler requires a dispatcher");
    }

    /** Feature off: extra handlers would be silently ignored — reject at construction. */
    @Test
    void testConstructorRejectsHandlersWhenFeatureDisabled() {
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        assertThatThrownBy(() -> buildHandler(false, null, dispatcher, inputGate))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("checkpointingDuringRecoveryEnabled=true");
    }

    private static InputChannelRecoveredStateHandler buildHandler(
            boolean checkpointingDuringRecoveryEnabled,
            ChannelStateFilteringHandler filteringHandler,
            FilteredBufferDispatcher dispatcher,
            SingleInputGate gate) {
        return new InputChannelRecoveredStateHandler(
                new InputGate[] {gate},
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
                checkpointingDuringRecoveryEnabled,
                filteringHandler,
                MemoryManager.DEFAULT_PAGE_SIZE,
                dispatcher);
    }

    /** Records each {@code write} for assertions; copies bytes so the source may be recycled. */
    private static final class RecordingDispatcher implements FilteredBufferDispatcher {
        final List<Write> writes = new ArrayList<>();

        @Override
        public void write(byte[] data, int length, InputChannelInfo channelInfo) {
            writes.add(new Write(Arrays.copyOf(data, length), length, channelInfo));
        }

        @Override
        public void flush() {}

        @Override
        public void drainPendingSpill() {}

        @Override
        public void close() {}

        static final class Write {
            final byte[] data;
            final int length;
            final InputChannelInfo channelInfo;

            Write(byte[] data, int length, InputChannelInfo channelInfo) {
                this.data = data;
                this.length = length;
                this.channelInfo = channelInfo;
            }
        }
    }
}

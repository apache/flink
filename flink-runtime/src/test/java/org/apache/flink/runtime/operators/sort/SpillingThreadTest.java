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

package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntComparator;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.util.MutableObjectIterator;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpillingThreadTest {

    private static final int PAGE_SIZE = 4 * 1024;
    private static final int NUM_PAGES = 100;

    private MemoryManager memoryManager;
    private IOManager ioManager;
    private final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;
    private final TypeComparator<Integer> comparator = new IntComparator(true);

    @BeforeEach
    void beforeEach() {
        this.memoryManager =
                MemoryManagerBuilder.newBuilder()
                        .setMemorySize(NUM_PAGES * PAGE_SIZE)
                        .setPageSize(PAGE_SIZE)
                        .build();
        this.ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        ioManager.close();
        if (memoryManager != null) {
            assertThat(memoryManager.verifyEmpty())
                    .withFailMessage(
                            "Memory leak: not all segments have been returned to the memory manager.")
                    .isTrue();
            memoryManager.shutdown();
            memoryManager = null;
        }
    }

    @ParameterizedTest(name = "Trigger zero-based division with maxFanIn: {0}, channelCount: {1}")
    @MethodSource("maxFanInChannelCountConfigurations")
    void testMergeChannelListDivideByZero(int maxFanIn, int channelCount) throws Exception {
        // Allocate minimal memory for SpillingThread
        DummyInvokable owner = new DummyInvokable();
        List<MemorySegment> sortReadMemory = memoryManager.allocatePages(owner, 5);
        List<MemorySegment> writeMemory = memoryManager.allocatePages(owner, 5);
        // Allocate at least maxFanIn read buffers to ensure each channel gets at least 1 segment
        // channelsToMergePerStep can be up to maxFanIn, so we need at least that many buffers
        List<MemorySegment> readBuffers = memoryManager.allocatePages(owner, maxFanIn);
        List<MemorySegment> writeBuffers = memoryManager.allocatePages(owner, 10);

        SpillChannelManager spillChannelManager = null;
        try {
            SpillingThread<Integer> spillingThread =
                    createSpillingThread(maxFanIn, sortReadMemory, writeMemory);

            // Get the SpillChannelManager to clean it up later
            spillChannelManager = getSpillChannelManager(spillingThread);

            // Create channelIDs list with the specified size
            // Note: These channels are registered with SpillChannelManager for cleanup
            // Files are created so the code can proceed to the divide-by-zero point
            List<ChannelWithBlockCount> channelIDs =
                    createChannelList(channelCount, spillChannelManager);

            // Use reflection to call the private mergeChannelList method
            Method mergeChannelListMethod =
                    SpillingThread.class.getDeclaredMethod(
                            "mergeChannelList", List.class, List.class, List.class);
            mergeChannelListMethod.setAccessible(true);

            // This should throw ArithmeticException: / by zero
            // The original issue specifically mentions maxFanIn=18, channelCount=7055 as
            // problematic.
            // Even if the initial calculation doesn't show channelsToMergePerStep=0, floating-point
            // precision issues or edge cases in the calculation may cause it to become 0, or the
            // divide-by-zero may occur in a different code path (e.g., when numMerges=0).
            assertThatThrownBy(
                            () ->
                                    mergeChannelListMethod.invoke(
                                            spillingThread, channelIDs, readBuffers, writeBuffers))
                    .hasRootCauseInstanceOf(ArithmeticException.class)
                    .hasRootCauseMessage("/ by zero");
        } finally {
            // Clean up SpillChannelManager to remove any temporary files
            if (spillChannelManager != null) {
                try {
                    spillChannelManager.close();
                } catch (Exception ignored) {
                    // Ignore cleanup errors
                }
            }
            // Clean up all allocated memory
            memoryManager.release(sortReadMemory);
            memoryManager.release(writeMemory);
            memoryManager.release(readBuffers);
            memoryManager.release(writeBuffers);
        }
    }

    /**
     * Provides test cases for problematic maxFanIn and channelIDs.size() combinations that align
     * with configuration values where the channelIDs.size() greater than or equal to some power of
     * the maxFanIn value:
     *
     * <p>(channelSize) >= (maxFanIn^3)
     *
     * <p>(channelSize) >= (maxFanIn^5)
     *
     * <p>...
     */
    private static Stream<Arguments> maxFanInChannelCountConfigurations() {
        return Stream.of(
                // maxFanIn = 5, channelIDs.size() = 125 (5^3)
                Arguments.of(5, 125),
                // maxFanIn = 6, channelIDs.size() = 216 (6^3)
                Arguments.of(6, 216),
                // maxFanIn = 18, channelIDs.size() = 5832 (18^3)
                Arguments.of(18, 5832),
                // maxFanIn = 25, channelIDs.size() = 15625 (25^3)
                Arguments.of(25, 15625),
                // maxFanIn = 36, channelIDs.size() = 46656 (36^3)
                Arguments.of(36, 46656),
                // maxFanIn = 7, channelIDs.size() = 16807 (7^5)
                Arguments.of(7, 16807));
    }

    /**
     * Uses reflection to get the SpillChannelManager from a SpillingThread instance for cleanup.
     */
    private SpillChannelManager getSpillChannelManager(SpillingThread<Integer> spillingThread)
            throws Exception {
        java.lang.reflect.Field field =
                SpillingThread.class.getDeclaredField("spillChannelManager");
        field.setAccessible(true);
        return (SpillChannelManager) field.get(spillingThread);
    }

    /**
     * Creates a simple SpillingThread instance for testing.
     *
     * @param maxFanIn the maximum fan-in value to use
     * @param sortReadMemory memory segments for sort reading (will be stored in SpillingThread)
     * @param writeMemory memory segments for writing (will be stored in SpillingThread)
     * @return a SpillingThread instance
     */
    private SpillingThread<Integer> createSpillingThread(
            int maxFanIn, List<MemorySegment> sortReadMemory, List<MemorySegment> writeMemory) {

        // Create simple mock implementations
        StageRunner.StageMessageDispatcher<Integer> dispatcher = createMockDispatcher();
        SpillChannelManager spillChannelManager = new SpillChannelManager();
        SpillingThread.SpillingBehaviour<Integer> spillingBehaviour =
                new SpillingThread.SpillingBehaviour<>() {
                    @Override
                    public void spillBuffer(
                            CircularElement<Integer> element,
                            org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView
                                    output,
                            LargeRecordHandler<Integer> largeRecordHandler) {}

                    @Override
                    public void mergeRecords(
                            MergeIterator<Integer> mergeIterator,
                            org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView
                                    output) {}
                };

        return new SpillingThread<>(
                null,
                dispatcher,
                memoryManager,
                ioManager,
                serializer,
                comparator,
                sortReadMemory,
                writeMemory,
                maxFanIn,
                spillChannelManager,
                null,
                spillingBehaviour,
                1,
                10);
    }

    /**
     * Creates a mock StageMessageDispatcher that does nothing.
     *
     * @return a mock dispatcher
     */
    private StageRunner.StageMessageDispatcher<Integer> createMockDispatcher() {
        return new StageRunner.StageMessageDispatcher<>() {
            @Override
            public void send(StageRunner.SortStage stage, CircularElement<Integer> element) {
                // No-op for testing
            }

            @Override
            public CircularElement<Integer> take(StageRunner.SortStage stage)
                    throws InterruptedException {
                // Return EOF marker to signal end
                return CircularElement.endMarker();
            }

            @Override
            public CircularElement<Integer> poll(StageRunner.SortStage stage) {
                return null;
            }

            @Override
            public void sendResult(MutableObjectIterator<Integer> result) {
                // No-op for testing
            }

            @Override
            public void close() {
                // No-op for testing
            }
        };
    }

    /**
     * Creates a list of ChannelWithBlockCount instances for testing.
     *
     * @param count the number of channels to create
     * @param spillChannelManager the channel manager to register channels with for cleanup
     * @return a list of ChannelWithBlockCount instances
     */
    private List<ChannelWithBlockCount> createChannelList(
            int count, SpillChannelManager spillChannelManager)
            throws IOException, MemoryAllocationException {
        List<ChannelWithBlockCount> channels = new ArrayList<>(count);
        FileIOChannel.Enumerator enumerator = ioManager.createChannelEnumerator();

        // Allocate minimal memory for writing channel files
        DummyInvokable owner = new DummyInvokable();
        List<MemorySegment> writeMemory = memoryManager.allocatePages(owner, 1);

        try {
            for (int i = 0; i < count; i++) {
                FileIOChannel.ID channel = enumerator.next();
                // Register channel for cleanup
                spillChannelManager.registerChannelToBeRemovedAtShutdown(channel);

                // Create a writer and write minimal valid data to the channel
                // This ensures the file has the proper header format expected by
                // ChannelReaderInputView
                BlockChannelWriter<MemorySegment> writer =
                        ioManager.createBlockChannelWriter(channel);
                spillChannelManager.registerOpenChannelToBeRemovedAtShutdown(writer);

                ChannelWriterOutputView output =
                        new ChannelWriterOutputView(
                                writer, writeMemory, memoryManager.getPageSize());

                final int blockCount = output.getBlockCount();

                spillChannelManager.unregisterOpenChannelToBeRemovedAtShutdown(writer);

                // Create a channel with the actual block count
                channels.add(new ChannelWithBlockCount(channel, blockCount));
            }
        } finally {
            memoryManager.release(writeMemory);
        }

        return channels;
    }
}

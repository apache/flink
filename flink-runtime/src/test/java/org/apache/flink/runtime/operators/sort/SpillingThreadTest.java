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
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
        // Set up supporting memory allocations, etc.
        DummyInvokable owner = new DummyInvokable();
        List<MemorySegment> sortReadMemory = memoryManager.allocatePages(owner, maxFanIn);
        List<MemorySegment> writeMemory = memoryManager.allocatePages(owner, 5);

        SpillChannelManager channelManager = new SpillChannelManager();

        try (channelManager) {
            // Spin up a thread with the expected maxFanIn and channelCount
            SpillingThread<Integer> spillingThread =
                    createSpillingThread(
                            maxFanIn, channelCount, sortReadMemory, writeMemory, channelManager);
            // Launch the thread which will trigger the underlying merging operations
            // to continually merge channels (resulting in a division-by-zero exception)
            assertThatThrownBy(spillingThread::go)
                    .isInstanceOf(ArithmeticException.class)
                    .hasMessageContaining("/ by zero");
        } catch (Exception ignored) {
            // Ignore other exceptions
        } finally {
            memoryManager.release(sortReadMemory);
            memoryManager.release(writeMemory);
        }
    }

    /**
     * Provides test cases for problematic maxFanIn and channelIDs.size() combinations.
     *
     * <p>The divide-by-zero occurs when channelIDs.size() equals a power of maxFanIn. This can
     * happen in two scenarios:
     *
     * <ul>
     *   <li>Exact powers: channelIDs.size() == maxFanIn^n - triggers divide-by-zero in first
     *       iteration
     *   <li>Greater than powers: channelIDs.size() > maxFanIn^n - may reduce to an exact power in a
     *       later iteration, triggering divide-by-zero
     * </ul>
     *
     * <p>For example, with maxFanIn=18:
     *
     * <ul>
     *   <li>5832 (18^3) triggers divide-by-zero immediately
     *   <li>7055 reduces to 5832 in first iteration, then triggers divide-by-zero in second
     *       iteration
     * </ul>
     */
    private static Stream<Arguments> maxFanInChannelCountConfigurations() {
        return Stream.of(
                // Original repro (7055 > 18^3)
                Arguments.of(18, 7055),
                // Exact powers (trigger divide-by-zero in first iteration)
                Arguments.of(5, (int) Math.pow(5, 3)),
                Arguments.of(6, (int) Math.pow(6, 3)),
                Arguments.of(18, (int) Math.pow(18, 3)),
                Arguments.of(25, (int) Math.pow(25, 3)),
                Arguments.of(36, (int) Math.pow(36, 3)),
                Arguments.of(7, (int) Math.pow(7, 5)),
                // Greater than powers (later iterates reduce to exact power)
                Arguments.of(18, (int) Math.pow(18, 3) + 1),
                Arguments.of(5, (int) Math.pow(5, 3) + 1),
                Arguments.of(6, (int) Math.pow(6, 3) + 1));
    }

    /**
     * Creates a SpillingThread configured to trigger divide-by-zero when channelCount equals a
     * power of maxFanIn.
     */
    private SpillingThread<Integer> createSpillingThread(
            int maxFanIn,
            int channelCount,
            List<MemorySegment> sortReadMemory,
            List<MemorySegment> writeMemory,
            SpillChannelManager channelManager) {
        StageRunner.StageMessageDispatcher<Integer> dispatcher =
                createDispatcherForSpilling(channelCount);
        SpillingThread.SpillingBehaviour<Integer> spillingBehaviour = createSpillingBehaviour();

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
                channelManager,
                null,
                spillingBehaviour,
                1,
                10);
    }

    /**
     * Creates a dispatcher that sends the sequence needed to trigger the divide-by-zero:
     * SPILLING_MARKER, then channelCount elements, then EOF_MARKER.
     */
    private StageRunner.StageMessageDispatcher<Integer> createDispatcherForSpilling(
            int channelCount) {
        return new StageRunner.StageMessageDispatcher<>() {
            private final AtomicInteger elementsSent = new AtomicInteger(0);

            @Override
            public void send(StageRunner.SortStage stage, CircularElement<Integer> element) {}

            @Override
            public CircularElement<Integer> take(StageRunner.SortStage stage) {
                if (stage == StageRunner.SortStage.SPILL) {
                    int current = elementsSent.getAndIncrement();
                    if (current == 0) {
                        return CircularElement.spillingMarker();
                    }
                    if (current <= channelCount) {
                        return new CircularElement<>(current - 1, createMockInMemorySorter(), null);
                    }
                    return CircularElement.endMarker();
                }
                return null;
            }

            @Override
            public CircularElement<Integer> poll(StageRunner.SortStage stage) {
                return null;
            }

            @Override
            public void sendResult(MutableObjectIterator<Integer> result) {}

            @Override
            public void close() {}
        };
    }

    /** Creates a SpillingBehaviour that writes data to ensure channels are created. */
    private SpillingThread.SpillingBehaviour<Integer> createSpillingBehaviour() {
        return new SpillingThread.SpillingBehaviour<>() {
            @Override
            public void spillBuffer(
                    CircularElement<Integer> element,
                    ChannelWriterOutputView output,
                    LargeRecordHandler<Integer> largeRecordHandler)
                    throws IOException {
                // Ensure we at least write something
                output.writeInt(0);
            }

            @Override
            public void mergeRecords(
                    MergeIterator<Integer> mergeIterator, ChannelWriterOutputView output) {}
        };
    }

    /** Creates a minimal mock InMemorySorter that implements all required methods. */
    private InMemorySorter<Integer> createMockInMemorySorter() {
        return new InMemorySorter<>() {
            @Override
            public void reset() {
                // No-op for testing
            }

            @Override
            public boolean isEmpty() {
                return true;
            }

            @Override
            public void dispose() {
                // No-op for testing
            }

            @Override
            public long getCapacity() {
                return 0;
            }

            @Override
            public long getOccupancy() {
                return 0;
            }

            @Override
            public Integer getRecord(int logicalPosition) {
                return null;
            }

            @Override
            public Integer getRecord(Integer reuse, int logicalPosition) {
                return null;
            }

            @Override
            public boolean write(Integer record) {
                return false;
            }

            @Override
            public MutableObjectIterator<Integer> getIterator() {
                return null;
            }

            @Override
            public void writeToOutput(ChannelWriterOutputView output) {
                // No-op for testing
            }

            @Override
            public void writeToOutput(
                    ChannelWriterOutputView output, LargeRecordHandler<Integer> largeRecordsOutput)
                    throws IOException {
                // No-op for testing
            }

            @Override
            public void writeToOutput(ChannelWriterOutputView output, int start, int num) {
                // No-op for testing
            }

            @Override
            public int compare(int i, int j) {
                return 0;
            }

            @Override
            public int compare(
                    int segmentNumberI,
                    int segmentOffsetI,
                    int segmentNumberJ,
                    int segmentOffsetJ) {
                return 0;
            }

            @Override
            public void swap(int i, int j) {
                // No-op for testing
            }

            @Override
            public void swap(
                    int segmentNumberI,
                    int segmentOffsetI,
                    int segmentNumberJ,
                    int segmentOffsetJ) {
                // No-op for testing
            }

            @Override
            public int size() {
                return 0;
            }

            @Override
            public int recordSize() {
                return 0;
            }

            @Override
            public int recordsPerSegment() {
                return 0;
            }
        };
    }
}

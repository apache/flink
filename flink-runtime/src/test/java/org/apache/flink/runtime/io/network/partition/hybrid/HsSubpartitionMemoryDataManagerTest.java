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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.ConsumeStatus;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingInfoProvider.SpillStatus;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createBufferBuilder;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createTestingOutputMetrics;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsSubpartitionMemoryDataManager}. */
class HsSubpartitionMemoryDataManagerTest {
    private static final int SUBPARTITION_ID = 0;

    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private static final int RECORD_SIZE = Integer.BYTES;

    private int bufferSize = RECORD_SIZE;

    @Test
    void testAppendDataRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        HsMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return createBufferBuilder(bufferSize);
                                })
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);
        assertThat(requestBufferFuture).isCompleted();
    }

    @Test
    void testAppendEventNotRequestBuffer() throws Exception {
        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        HsMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () -> {
                                    requestBufferFuture.complete(null);
                                    return null;
                                })
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionMemoryDataManager.append(createRecord(0), DataType.EVENT_BUFFER);
        assertThat(requestBufferFuture).isNotDone();
    }

    @Test
    void testAppendEventFinishCurrentBuffer() throws Exception {
        bufferSize = RECORD_SIZE * 3;
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        HsMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .setOnBufferFinishedRunnable(finishedBuffers::incrementAndGet)
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(1), DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(0);
        subpartitionMemoryDataManager.append(createRecord(2), DataType.EVENT_BUFFER);
        assertThat(finishedBuffers).hasValue(2);
    }

    @Test
    void testPeekNextToConsumeDataTypeNotMeetBufferIndexToConsume() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);

        assertThat(subpartitionMemoryDataManager.peekNextToConsumeDataType(1))
                .isEqualTo(DataType.NONE);
    }

    @Test
    void testPeekNextToConsumeDataTypeTrimHeadingReleasedBuffers() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(1), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(2), DataType.EVENT_BUFFER);

        List<BufferIndexAndChannel> toRelease =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(0, 0, 1);
        subpartitionMemoryDataManager.releaseSubpartitionBuffers(toRelease);

        assertThat(subpartitionMemoryDataManager.peekNextToConsumeDataType(2))
                .isEqualTo(DataType.EVENT_BUFFER);
    }

    @Test
    void testConsumeBufferFirstUnConsumedBufferIndexNotMeetNextToConsume() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);

        assertThat(subpartitionMemoryDataManager.consumeBuffer(1)).isNotPresent();
    }

    @Test
    void testConsumeBufferTrimHeadingReleasedBuffers() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(1), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(2), DataType.EVENT_BUFFER);

        List<BufferIndexAndChannel> toRelease =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(0, 0, 1);
        subpartitionMemoryDataManager.releaseSubpartitionBuffers(toRelease);

        assertThat(subpartitionMemoryDataManager.consumeBuffer(2)).isPresent();
    }

    @Test
    void testConsumeBufferReturnSlice() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);

        Optional<BufferAndBacklog> bufferOpt = subpartitionMemoryDataManager.consumeBuffer(0);
        assertThat(bufferOpt)
                .hasValueSatisfying(
                        (bufferAndBacklog ->
                                assertThat(bufferAndBacklog.buffer())
                                        .isInstanceOf(ReadOnlySlicedNetworkBuffer.class)));
    }

    @Test
    void testConsumeBuffer() throws Exception {
        List<BufferIndexAndChannel> consumedBufferIndexAndChannel = new ArrayList<>();
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .setOnBufferConsumedConsumer(consumedBufferIndexAndChannel::add)
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);

        subpartitionMemoryDataManager.append(createRecord(0), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(1), DataType.DATA_BUFFER);
        subpartitionMemoryDataManager.append(createRecord(2), DataType.EVENT_BUFFER);

        List<Tuple2<Integer, Buffer.DataType>> expectedRecords = new ArrayList<>();
        expectedRecords.add(Tuple2.of(0, Buffer.DataType.DATA_BUFFER));
        expectedRecords.add(Tuple2.of(1, Buffer.DataType.DATA_BUFFER));
        expectedRecords.add(Tuple2.of(2, DataType.EVENT_BUFFER));
        checkConsumedBufferAndNextDataType(
                expectedRecords,
                Arrays.asList(
                        subpartitionMemoryDataManager.consumeBuffer(0),
                        subpartitionMemoryDataManager.consumeBuffer(1),
                        subpartitionMemoryDataManager.consumeBuffer(2)));

        List<BufferIndexAndChannel> expectedBufferIndexAndChannel =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(0, 0, 1, 2);
        assertThat(consumedBufferIndexAndChannel)
                .zipSatisfy(
                        expectedBufferIndexAndChannel,
                        (consumed, expected) -> {
                            assertThat(consumed.getChannel()).isEqualTo(expected.getChannel());
                            assertThat(consumed.getBufferIndex())
                                    .isEqualTo(expected.getBufferIndex());
                        });
    }

    @Test
    void testGetBuffersSatisfyStatus() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        final int numBuffers = 4;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionMemoryDataManager.append(createRecord(i), DataType.DATA_BUFFER);
        }

        // spill buffer 1 and 2
        List<BufferIndexAndChannel> toStartSpilling =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(0, 1, 2);
        CompletableFuture<Void> spilledDoneFuture = new CompletableFuture<>();
        subpartitionMemoryDataManager.spillSubpartitionBuffers(toStartSpilling, spilledDoneFuture);

        // consume buffer 0, 1
        subpartitionMemoryDataManager.consumeBuffer(0);
        subpartitionMemoryDataManager.consumeBuffer(1);

        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.ALL, ConsumeStatus.ALL),
                Arrays.asList(0, 1, 2, 3));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.ALL, ConsumeStatus.CONSUMED),
                Arrays.asList(0, 1));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.ALL, ConsumeStatus.NOT_CONSUMED),
                Arrays.asList(2, 3));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.SPILL, ConsumeStatus.ALL),
                Arrays.asList(1, 2));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.NOT_SPILL, ConsumeStatus.ALL),
                Arrays.asList(0, 3));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.SPILL, ConsumeStatus.NOT_CONSUMED),
                Collections.singletonList(2));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.SPILL, ConsumeStatus.CONSUMED),
                Collections.singletonList(1));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.NOT_SPILL, ConsumeStatus.CONSUMED),
                Collections.singletonList(0));
        checkBufferIndex(
                subpartitionMemoryDataManager.getBuffersSatisfyStatus(
                        SpillStatus.NOT_SPILL, ConsumeStatus.NOT_CONSUMED),
                Collections.singletonList(3));
    }

    @Test
    void testSpillSubpartitionBuffers() throws Exception {
        CompletableFuture<Void> spilledDoneFuture = new CompletableFuture<>();
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(RECORD_SIZE))
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        final int numBuffers = 3;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionMemoryDataManager.append(createRecord(i), DataType.DATA_BUFFER);
        }

        List<BufferIndexAndChannel> toStartSpilling =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(0, 0, 1, 2);
        List<BufferWithIdentity> buffers =
                subpartitionMemoryDataManager.spillSubpartitionBuffers(
                        toStartSpilling, spilledDoneFuture);
        assertThat(toStartSpilling)
                .zipSatisfy(
                        buffers,
                        (expected, spilled) -> {
                            assertThat(expected.getBufferIndex())
                                    .isEqualTo(spilled.getBufferIndex());
                            assertThat(expected.getChannel()).isEqualTo(spilled.getChannelIndex());
                        });
        List<Integer> expectedValues = Arrays.asList(0, 1, 2);
        checkBuffersRefCountAndValue(buffers, Arrays.asList(2, 2, 2), expectedValues);
        spilledDoneFuture.complete(null);
        checkBuffersRefCountAndValue(buffers, Arrays.asList(1, 1, 1), expectedValues);
    }

    @Test
    void testReleaseAndMarkReadableSubpartitionBuffers() throws Exception {
        int targetChannel = 0;
        List<Integer> readableBufferIndex = new ArrayList<>();
        List<MemorySegment> recycledBuffers = new ArrayList<>();
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(
                                () ->
                                        new BufferBuilder(
                                                MemorySegmentFactory.allocateUnpooledSegment(
                                                        bufferSize),
                                                recycledBuffers::add))
                        .setMarkBufferReadableConsumer(
                                (channel, bufferIndex) -> {
                                    assertThat(channel).isEqualTo(targetChannel);
                                    readableBufferIndex.add(bufferIndex);
                                })
                        .build();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        // append data
        final int numBuffers = 3;
        for (int i = 0; i < numBuffers; i++) {
            subpartitionMemoryDataManager.append(createRecord(i), DataType.DATA_BUFFER);
        }
        // spill the last buffer and release all buffers.
        List<BufferIndexAndChannel> toRelease =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(targetChannel, 0, 1, 2);
        CompletableFuture<Void> spilledFuture = new CompletableFuture<>();
        subpartitionMemoryDataManager.spillSubpartitionBuffers(
                toRelease.subList(numBuffers - 1, numBuffers), spilledFuture);
        subpartitionMemoryDataManager.releaseSubpartitionBuffers(toRelease);
        assertThat(readableBufferIndex).isEmpty();
        // not start spilling buffers should be recycled after release.
        checkMemorySegmentValue(recycledBuffers, Arrays.asList(0, 1));

        // after spill finished, need mark readable buffers should trigger notify.
        spilledFuture.complete(null);
        assertThat(readableBufferIndex).containsExactly(2);
        checkMemorySegmentValue(recycledBuffers, Arrays.asList(0, 1, 2));
    }

    @Test
    void testMetricsUpdate() throws Exception {
        final int recordSize = bufferSize / 2;
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setRequestBufferFromPoolSupplier(() -> createBufferBuilder(bufferSize))
                        .build();

        HsOutputMetrics metrics = createTestingOutputMetrics();
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                createSubpartitionMemoryDataManager(memoryDataManagerOperation);
        subpartitionMemoryDataManager.setOutputMetrics(metrics);

        subpartitionMemoryDataManager.append(ByteBuffer.allocate(recordSize), DataType.DATA_BUFFER);
        ByteBuffer eventBuffer = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
        final int eventSize = eventBuffer.remaining();
        subpartitionMemoryDataManager.append(
                EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE),
                DataType.EVENT_BUFFER);
        assertThat(metrics.getNumBuffersOut().getCount()).isEqualTo(2);
        assertThat(metrics.getNumBytesOut().getCount()).isEqualTo(recordSize + eventSize);
    }

    private static void checkBufferIndex(
            Deque<BufferIndexAndChannel> bufferWithIdentities, List<Integer> expectedIndexes) {
        List<Integer> bufferIndexes =
                bufferWithIdentities.stream()
                        .map(BufferIndexAndChannel::getBufferIndex)
                        .collect(Collectors.toList());
        assertThat(bufferIndexes).isEqualTo(expectedIndexes);
    }

    private static void checkMemorySegmentValue(
            List<MemorySegment> memorySegments, List<Integer> expectedValues) {
        for (int i = 0; i < memorySegments.size(); i++) {
            assertThat(memorySegments.get(i).getInt(0)).isEqualTo(expectedValues.get(i));
        }
    }

    private static void checkConsumedBufferAndNextDataType(
            List<Tuple2<Integer, Buffer.DataType>> expectedRecords,
            List<Optional<BufferAndBacklog>> bufferAndBacklogOpt) {
        checkArgument(expectedRecords.size() == bufferAndBacklogOpt.size());
        for (int i = 0; i < bufferAndBacklogOpt.size(); i++) {
            final int index = i;
            assertThat(bufferAndBacklogOpt.get(index))
                    .hasValueSatisfying(
                            (bufferAndBacklog -> {
                                Buffer buffer = bufferAndBacklog.buffer();
                                int value =
                                        buffer.getNioBufferReadable()
                                                .order(ByteOrder.LITTLE_ENDIAN)
                                                .getInt();
                                Buffer.DataType dataType = buffer.getDataType();
                                assertThat(value).isEqualTo(expectedRecords.get(index).f0);
                                assertThat(dataType).isEqualTo(expectedRecords.get(index).f1);
                                if (index != bufferAndBacklogOpt.size() - 1) {
                                    assertThat(bufferAndBacklog.getNextDataType())
                                            .isEqualTo(expectedRecords.get(index + 1).f1);
                                } else {
                                    assertThat(bufferAndBacklog.getNextDataType())
                                            .isEqualTo(Buffer.DataType.NONE);
                                }
                            }));
        }
    }

    private static void checkBuffersRefCountAndValue(
            List<BufferWithIdentity> bufferWithIdentities,
            List<Integer> expectedRefCounts,
            List<Integer> expectedValues) {
        for (int i = 0; i < bufferWithIdentities.size(); i++) {
            BufferWithIdentity bufferWithIdentity = bufferWithIdentities.get(i);
            Buffer buffer = bufferWithIdentity.getBuffer();
            assertThat(buffer.getNioBufferReadable().order(ByteOrder.LITTLE_ENDIAN).getInt())
                    .isEqualTo(expectedValues.get(i));
            assertThat(buffer.refCnt()).isEqualTo(expectedRefCounts.get(i));
        }
    }

    private HsSubpartitionMemoryDataManager createSubpartitionMemoryDataManager(
            HsMemoryDataManagerOperation memoryDataManagerOperation) {
        HsSubpartitionMemoryDataManager subpartitionMemoryDataManager =
                new HsSubpartitionMemoryDataManager(
                        SUBPARTITION_ID, bufferSize, lock.readLock(), memoryDataManagerOperation);
        subpartitionMemoryDataManager.setOutputMetrics(createTestingOutputMetrics());
        return subpartitionMemoryDataManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(RECORD_SIZE);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}

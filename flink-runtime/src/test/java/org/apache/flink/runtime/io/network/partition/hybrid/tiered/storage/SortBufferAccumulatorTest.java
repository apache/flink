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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.TieredStorageTestUtils.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SortBufferAccumulator}. */
class SortBufferAccumulatorTest {

    private static final int NUM_TOTAL_BUFFERS = 1000;

    private static final int BUFFER_SIZE_BYTES = 1024;

    private static final float NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private NetworkBufferPool globalPool;

    @BeforeEach
    void before() {
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, BUFFER_SIZE_BYTES);
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testAccumulateRecordsAndGenerateBuffers() throws IOException {
        testAccumulateRecordsAndGenerateBuffers(
                true,
                Arrays.asList(
                        Buffer.DataType.DATA_BUFFER, Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END));
    }

    @Test
    void testAccumulateRecordsAndGenerateBuffersWithPartialRecordUnallowed() throws IOException {
        testAccumulateRecordsAndGenerateBuffers(
                false, Collections.singletonList(Buffer.DataType.DATA_BUFFER_WITH_CLEAR_END));
    }

    private void testAccumulateRecordsAndGenerateBuffers(
            boolean isPartialRecordAllowed, Collection<Buffer.DataType> expectedDataTypes)
            throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        int indexEntrySize = 16;
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        Random random = new Random(1234);
        TieredStorageMemoryManager memoryManager = createStorageMemoryManager(numBuffers);

        int numExpectBuffers = 0;
        int currentBufferWrittenBytes = 0;
        AtomicInteger numReceivedFinishedBuffer = new AtomicInteger(0);

        // The test use only one buffer for sort, and when it is full, the sort buffer will be
        // flushed.
        try (SortBufferAccumulator bufferAccumulator =
                new SortBufferAccumulator(
                        1, 2, BUFFER_SIZE_BYTES, memoryManager, isPartialRecordAllowed)) {
            bufferAccumulator.setup(
                    ((subpartition, buffer, numRemainingBuffers) -> {
                        assertThat(buffer.getDataType()).isIn(expectedDataTypes);
                        numReceivedFinishedBuffer.incrementAndGet();
                        buffer.recycleBuffer();
                    }));
            boolean isBroadcastForPreviousRecord = false;
            for (int i = 0; i < numRecords; i++) {
                int numBytes = random.nextInt(BUFFER_SIZE_BYTES) + 1;
                ByteBuffer record = generateRandomData(numBytes, random);
                boolean isBroadcast = random.nextBoolean();
                bufferAccumulator.receive(
                        record, subpartitionId, Buffer.DataType.DATA_BUFFER, isBroadcast);
                if (currentBufferWrittenBytes + numBytes + indexEntrySize > BUFFER_SIZE_BYTES
                        || i > 0 && isBroadcastForPreviousRecord != isBroadcast) {
                    numExpectBuffers++;
                    currentBufferWrittenBytes = 0;
                }

                isBroadcastForPreviousRecord = isBroadcast;
                currentBufferWrittenBytes += numBytes + indexEntrySize;
            }
        }

        assertThat(currentBufferWrittenBytes).isLessThan(BUFFER_SIZE_BYTES);
        numExpectBuffers += currentBufferWrittenBytes == 0 ? 0 : 1;
        assertThat(numReceivedFinishedBuffer).hasValue(numExpectBuffers);
    }

    @Test
    void testWriteLargeRecord() throws IOException {
        testWriteLargeRecord(true);
    }

    @Test
    void testWriteLargeRecordWithPartialRecordUnallowed() throws IOException {
        testWriteLargeRecord(false);
    }

    private void testWriteLargeRecord(boolean isPartialRecordAllowed) throws IOException {
        int numBuffers = 15;
        Random random = new Random();
        TieredStorageMemoryManager memoryManager = createStorageMemoryManager(numBuffers);

        // The test use only one buffer for sort, and when it is full, the sort buffer will be
        // flushed.
        try (SortBufferAccumulator bufferAccumulator =
                new SortBufferAccumulator(
                        1, 2, BUFFER_SIZE_BYTES, memoryManager, isPartialRecordAllowed)) {
            AtomicInteger numReceivedBuffers = new AtomicInteger(0);
            bufferAccumulator.setup(
                    (subpartitionIndex, buffer, numRemainingBuffers) -> {
                        numReceivedBuffers.getAndAdd(1);
                        buffer.recycleBuffer();
                    });
            ByteBuffer largeRecord = generateRandomData(BUFFER_SIZE_BYTES * numBuffers, random);
            bufferAccumulator.receive(
                    largeRecord,
                    new TieredStorageSubpartitionId(0),
                    Buffer.DataType.DATA_BUFFER,
                    false);
            assertThat(numReceivedBuffers).hasValue(numBuffers);
        }
    }

    @Test
    void testNoBuffersForSort() throws IOException {
        int numBuffers = 10;
        int bufferSize = 1024;
        Random random = new Random(1111);
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        TieredStorageMemoryManager memoryManager = createStorageMemoryManager(numBuffers);

        try (SortBufferAccumulator bufferAccumulator =
                new SortBufferAccumulator(1, 1, bufferSize, memoryManager, true)) {
            bufferAccumulator.setup((subpartitionIndex, buffer, numRemainingBuffers) -> {});
            assertThatThrownBy(
                            () ->
                                    bufferAccumulator.receive(
                                            generateRandomData(1, random),
                                            subpartitionId,
                                            Buffer.DataType.DATA_BUFFER,
                                            false))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void testCloseWithUnFinishedBuffers() throws IOException {
        int numBuffers = 10;

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        SortBufferAccumulator bufferAccumulator =
                new SortBufferAccumulator(
                        1, 2, BUFFER_SIZE_BYTES, tieredStorageMemoryManager, true);
        bufferAccumulator.setup(
                ((subpartition, buffer, numRemainingBuffers) -> buffer.recycleBuffer()));
        bufferAccumulator.receive(
                generateRandomData(1, new Random()),
                new TieredStorageSubpartitionId(0),
                Buffer.DataType.DATA_BUFFER,
                false);
        assertThat(tieredStorageMemoryManager.numOwnerRequestedBuffer(bufferAccumulator))
                .isEqualTo(2);
        bufferAccumulator.close();
        assertThat(tieredStorageMemoryManager.numOwnerRequestedBuffer(bufferAccumulator)).isZero();
    }

    @Test
    void testReuseRecycledBuffersWhenFlushDataBuffer() throws IOException {
        int numBuffers = 10;
        int numSubpartitions = 10;

        AtomicInteger numRequestedBuffers = new AtomicInteger(0);
        TestingTieredStorageMemoryManager memoryManager =
                new TestingTieredStorageMemoryManager.Builder()
                        .setRequestBufferBlockingFunction(
                                owner -> {
                                    numRequestedBuffers.incrementAndGet();
                                    MemorySegment memorySegment =
                                            globalPool.requestPooledMemorySegment();
                                    // When flushing data from the data buffer, the buffers should
                                    // not be requested from here anymore because the freeSegments
                                    // can be reused before released.
                                    assertThat(numRequestedBuffers.get() <= numBuffers).isTrue();
                                    return new BufferBuilder(
                                            memorySegment,
                                            segment -> globalPool.requestPooledMemorySegment());
                                })
                        .build();

        SortBufferAccumulator bufferAccumulator =
                new SortBufferAccumulator(
                        numSubpartitions, numBuffers, BUFFER_SIZE_BYTES, memoryManager, true);
        bufferAccumulator.setup(
                ((subpartition, buffer, numRemainingBuffers) -> buffer.recycleBuffer()));

        for (int i = 0; i < numSubpartitions; i++) {
            bufferAccumulator.receive(
                    generateRandomData(10, new Random()),
                    new TieredStorageSubpartitionId(i % numSubpartitions),
                    Buffer.DataType.DATA_BUFFER,
                    false);
        }

        // Trigger flushing buffers from data buffer.
        bufferAccumulator.close();
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(int numBuffersInBufferPool)
            throws IOException {
        BufferPool bufferPool =
                globalPool.createBufferPool(numBuffersInBufferPool, numBuffersInBufferPool);
        TieredStorageMemoryManagerImpl storageMemoryManager =
                new TieredStorageMemoryManagerImpl(NUM_BUFFERS_TRIGGER_FLUSH_RATIO, true);
        storageMemoryManager.setup(
                bufferPool, Collections.singletonList(new TieredStorageMemorySpec(this, 1)));
        return storageMemoryManager;
    }
}

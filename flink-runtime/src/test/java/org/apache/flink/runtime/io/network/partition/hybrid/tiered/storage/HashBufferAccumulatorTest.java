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

import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.tiered.TieredStorageTestUtils.generateRandomData;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HashBufferAccumulator}. */
class HashBufferAccumulatorTest {

    public static final int NUM_TOTAL_BUFFERS = 1000;

    public static final int NETWORK_BUFFER_SIZE = 1024;

    private static final float NUM_BUFFERS_TRIGGER_FLUSH_RATIO = 0.6f;

    private NetworkBufferPool globalPool;

    @BeforeEach
    void before() {
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, NETWORK_BUFFER_SIZE);
    }

    @AfterEach
    void after() {
        globalPool.destroy();
    }

    @Test
    void testAccumulateRecordsAndGenerateFinishedBuffers() throws IOException {
        testAccumulateRecordsAndGenerateFinishedBuffers(true);
    }

    @Test
    void testAccumulateRecordsAndGenerateFinishedBuffersWithPartialRecordUnallowed()
            throws IOException {
        testAccumulateRecordsAndGenerateFinishedBuffers(false);
    }

    private void testAccumulateRecordsAndGenerateFinishedBuffers(boolean isPartialRecordAllowed)
            throws IOException {
        int numBuffers = 10;
        int numRecords = 1000;
        TieredStorageSubpartitionId subpartitionId = new TieredStorageSubpartitionId(0);
        Random random = new Random();

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        try (HashBufferAccumulator bufferAccumulator =
                new HashBufferAccumulator(
                        1,
                        NETWORK_BUFFER_SIZE,
                        tieredStorageMemoryManager,
                        isPartialRecordAllowed)) {
            AtomicInteger numReceivedFinishedBuffer = new AtomicInteger(0);
            bufferAccumulator.setup(
                    ((subpartition, buffer, numRemainingBuffers) -> {
                        numReceivedFinishedBuffer.incrementAndGet();
                        buffer.recycleBuffer();
                    }));

            int numRecordBytesSinceLastEvent = 0;
            int numExpectBuffers = 0;
            for (int i = 0; i < numRecords; i++) {
                boolean isBuffer = random.nextBoolean() && i != numRecords - 1;
                ByteBuffer record;
                Buffer.DataType dataType =
                        isBuffer ? Buffer.DataType.DATA_BUFFER : Buffer.DataType.EVENT_BUFFER;
                if (isBuffer) {
                    int numBytes = random.nextInt(2 * NETWORK_BUFFER_SIZE) + 1;

                    if (!isPartialRecordAllowed
                            && numRecordBytesSinceLastEvent + numBytes > NETWORK_BUFFER_SIZE) {
                        if (numRecordBytesSinceLastEvent > 0) {
                            numExpectBuffers++;
                            numRecordBytesSinceLastEvent = 0;
                        }
                    }

                    if (!isPartialRecordAllowed && numBytes > NETWORK_BUFFER_SIZE) {
                        numExpectBuffers +=
                                numBytes / NETWORK_BUFFER_SIZE
                                        + (numBytes % NETWORK_BUFFER_SIZE == 0 ? 0 : 1);
                    } else {
                        numRecordBytesSinceLastEvent += numBytes;
                    }

                    record = generateRandomData(numBytes, random);
                } else {
                    numExpectBuffers +=
                            numRecordBytesSinceLastEvent / NETWORK_BUFFER_SIZE
                                    + (numRecordBytesSinceLastEvent % NETWORK_BUFFER_SIZE == 0
                                            ? 0
                                            : 1);
                    record = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
                    numExpectBuffers++;
                    numRecordBytesSinceLastEvent = 0;
                }
                bufferAccumulator.receive(record, subpartitionId, dataType, false);
            }

            assertThat(numReceivedFinishedBuffer.get()).isEqualTo(numExpectBuffers);
        }
    }

    @Test
    void testEventShouldNotRequestBufferFromMemoryManager() throws IOException {
        int numBuffers = 10;

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        try (HashBufferAccumulator bufferAccumulator =
                new HashBufferAccumulator(
                        1, NETWORK_BUFFER_SIZE, tieredStorageMemoryManager, true)) {
            bufferAccumulator.setup(
                    ((subpartition, buffer, numRemainingBuffers) -> buffer.recycleBuffer()));

            ByteBuffer endEvent = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            bufferAccumulator.receive(
                    endEvent,
                    new TieredStorageSubpartitionId(0),
                    Buffer.DataType.EVENT_BUFFER,
                    false);

            assertThat(tieredStorageMemoryManager.numOwnerRequestedBuffer(bufferAccumulator))
                    .isZero();
        }
    }

    @Test
    void testCloseWithUnFinishedBuffers() throws IOException {
        int numBuffers = 10;

        TieredStorageMemoryManager tieredStorageMemoryManager =
                createStorageMemoryManager(numBuffers);
        HashBufferAccumulator bufferAccumulator =
                new HashBufferAccumulator(1, NETWORK_BUFFER_SIZE, tieredStorageMemoryManager, true);
        bufferAccumulator.setup(
                ((subpartition, buffer, numRemainingBuffers) -> buffer.recycleBuffer()));
        bufferAccumulator.receive(
                generateRandomData(1, new Random()),
                new TieredStorageSubpartitionId(0),
                Buffer.DataType.DATA_BUFFER,
                false);
        assertThat(tieredStorageMemoryManager.numOwnerRequestedBuffer(bufferAccumulator))
                .isEqualTo(1);
        bufferAccumulator.close();
        assertThat(tieredStorageMemoryManager.numOwnerRequestedBuffer(this)).isZero();
    }

    private TieredStorageMemoryManagerImpl createStorageMemoryManager(int numBuffersInBufferPool)
            throws IOException {
        BufferPool bufferPool =
                globalPool.createBufferPool(
                        numBuffersInBufferPool, numBuffersInBufferPool, numBuffersInBufferPool);
        TieredStorageMemoryManagerImpl storageMemoryManager =
                new TieredStorageMemoryManagerImpl(NUM_BUFFERS_TRIGGER_FLUSH_RATIO, true);
        storageMemoryManager.setup(
                bufferPool, Collections.singletonList(new TieredStorageMemorySpec(this, 1)));
        return storageMemoryManager;
    }
}

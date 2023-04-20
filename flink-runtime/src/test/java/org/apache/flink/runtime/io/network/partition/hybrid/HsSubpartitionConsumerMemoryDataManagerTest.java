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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.ReadOnlySlicedNetworkBuffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createBuffer;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link HsSubpartitionConsumerMemoryDataManager}. */
@SuppressWarnings("FieldAccessNotGuarded")
class HsSubpartitionConsumerMemoryDataManagerTest {

    private static final int BUFFER_SIZE = Long.BYTES;

    private static final int SUBPARTITION_ID = 0;

    @Test
    void testPeekNextToConsumeDataTypeNotMeetBufferIndexToConsume() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(0, false));
        assertThat(
                        subpartitionConsumerMemoryDataManager.peekNextToConsumeDataType(
                                1, new ArrayDeque<>()))
                .isEqualTo(Buffer.DataType.NONE);
    }

    @Test
    void testPeekNextToConsumeDataTypeTrimHeadingReleasedBuffers() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        HsBufferContext buffer1 = createBufferContext(0, false);
        HsBufferContext buffer2 = createBufferContext(1, false);
        subpartitionConsumerMemoryDataManager.addBuffer(buffer1);
        subpartitionConsumerMemoryDataManager.addBuffer(buffer2);
        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(2, true));

        buffer1.release();
        buffer2.release();

        assertThat(
                        subpartitionConsumerMemoryDataManager.peekNextToConsumeDataType(
                                2, Collections.emptyList()))
                .isEqualTo(Buffer.DataType.EVENT_BUFFER);
    }

    @Test
    void testConsumeBufferFirstUnConsumedBufferIndexNotMeetNextToConsume() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(0, false));
        assertThat(subpartitionConsumerMemoryDataManager.consumeBuffer(1, Collections.emptyList()))
                .isNotPresent();
    }

    @Test
    void testConsumeBufferTrimHeadingReleasedBuffers() throws Exception {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        HsBufferContext buffer1 = createBufferContext(0, false);
        HsBufferContext buffer2 = createBufferContext(1, false);
        subpartitionConsumerMemoryDataManager.addBuffer(buffer1);
        subpartitionConsumerMemoryDataManager.addBuffer(buffer2);
        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(2, true));

        buffer1.release();
        buffer2.release();

        assertThat(subpartitionConsumerMemoryDataManager.consumeBuffer(2, Collections.emptyList()))
                .isPresent();
    }

    @Test
    void testConsumeBufferReturnSlice() {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);

        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(0, false));

        Optional<ResultSubpartition.BufferAndBacklog> bufferOpt =
                subpartitionConsumerMemoryDataManager.consumeBuffer(0, Collections.emptyList());
        assertThat(bufferOpt)
                .hasValueSatisfying(
                        (bufferAndBacklog ->
                                assertThat(bufferAndBacklog.buffer())
                                        .isInstanceOf(ReadOnlySlicedNetworkBuffer.class)));
    }

    @Test
    void testAddBuffer() {
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder().build();
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(memoryDataManagerOperation);
        ArrayDeque<HsBufferContext> initialBuffers = new ArrayDeque<>();
        initialBuffers.add(createBufferContext(0, false));
        initialBuffers.add(createBufferContext(1, false));
        subpartitionConsumerMemoryDataManager.addInitialBuffers(initialBuffers);
        subpartitionConsumerMemoryDataManager.addBuffer(createBufferContext(2, true));

        assertThat(subpartitionConsumerMemoryDataManager.consumeBuffer(0, Collections.emptyList()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(0);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.DATA_BUFFER);
                        });
        assertThat(subpartitionConsumerMemoryDataManager.consumeBuffer(1, Collections.emptyList()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(1);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.DATA_BUFFER);
                        });
        assertThat(subpartitionConsumerMemoryDataManager.consumeBuffer(2, Collections.emptyList()))
                .hasValueSatisfying(
                        bufferAndBacklog -> {
                            assertThat(bufferAndBacklog.getSequenceNumber()).isEqualTo(2);
                            assertThat(bufferAndBacklog.buffer().getDataType())
                                    .isEqualTo(Buffer.DataType.EVENT_BUFFER);
                        });
    }

    @Test
    void testRelease() {
        CompletableFuture<HsConsumerId> consumerReleasedFuture = new CompletableFuture<>();
        TestingMemoryDataManagerOperation memoryDataManagerOperation =
                TestingMemoryDataManagerOperation.builder()
                        .setOnConsumerReleasedBiConsumer(
                                (subpartitionId, consumerId) -> {
                                    consumerReleasedFuture.complete(consumerId);
                                })
                        .build();
        HsConsumerId consumerId = HsConsumerId.newId(null);
        HsSubpartitionConsumerMemoryDataManager subpartitionConsumerMemoryDataManager =
                createSubpartitionConsumerMemoryDataManager(consumerId, memoryDataManagerOperation);
        subpartitionConsumerMemoryDataManager.releaseDataView();
        assertThat(consumerReleasedFuture).isCompletedWithValue(consumerId);
    }

    private static HsBufferContext createBufferContext(int bufferIndex, boolean isEvent) {
        return new HsBufferContext(
                createBuffer(BUFFER_SIZE, isEvent), bufferIndex, SUBPARTITION_ID);
    }

    private HsSubpartitionConsumerMemoryDataManager createSubpartitionConsumerMemoryDataManager(
            HsMemoryDataManagerOperation memoryDataManagerOperation) {
        return createSubpartitionConsumerMemoryDataManager(
                HsConsumerId.DEFAULT, memoryDataManagerOperation);
    }

    private HsSubpartitionConsumerMemoryDataManager createSubpartitionConsumerMemoryDataManager(
            HsConsumerId consumerId, HsMemoryDataManagerOperation memoryDataManagerOperation) {
        return new HsSubpartitionConsumerMemoryDataManager(
                new ReentrantLock(),
                new ReentrantLock(),
                // consumerMemoryDataManager is a member of subpartitionMemoryDataManager, using a
                // fixed subpartition id is enough.
                SUBPARTITION_ID,
                consumerId,
                memoryDataManagerOperation);
    }
}

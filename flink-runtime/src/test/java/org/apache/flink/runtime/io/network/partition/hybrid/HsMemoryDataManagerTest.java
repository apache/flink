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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsFileDataIndex.SpilledBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createTestingOutputMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HsMemoryDataManager}. */
class HsMemoryDataManagerTest {
    private static final int NUM_BUFFERS = 10;

    private static final int NUM_SUBPARTITIONS = 3;

    private int poolSize = 10;

    private int bufferSize = Integer.BYTES;

    private Path dataFilePath;

    private Path indexFilePath;

    @BeforeEach
    void before(@TempDir Path tempDir) {
        this.dataFilePath = tempDir.resolve(".data");
        this.indexFilePath = tempDir.resolve(".index");
    }

    @Test
    void testAppendMarkBufferFinished() throws Exception {
        AtomicInteger finishedBuffers = new AtomicInteger(0);
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numTotalUnSpillBuffers, currentPoolSize) -> {
                                    finishedBuffers.incrementAndGet();
                                    return Optional.of(Decision.NO_ACTION);
                                })
                        .build();
        bufferSize = Integer.BYTES * 3;
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);

        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(0);

        memoryDataManager.append(createRecord(2), 0, Buffer.DataType.DATA_BUFFER);
        assertThat(finishedBuffers).hasValue(1);

        memoryDataManager.append(createRecord(3), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(4), 0, Buffer.DataType.EVENT_BUFFER);
        assertThat(finishedBuffers).hasValue(3);
    }

    @Test
    void testAppendRequestBuffer() throws Exception {
        poolSize = 3;
        List<Tuple2<Integer, Integer>> numFinishedBufferAndPoolSize = new ArrayList<>();
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnMemoryUsageChangedFunction(
                                (finishedBuffer, poolSize) -> {
                                    numFinishedBufferAndPoolSize.add(
                                            Tuple2.of(finishedBuffer, poolSize));
                                    return Optional.of(Decision.NO_ACTION);
                                })
                        .build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.append(createRecord(0), 0, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(1), 1, Buffer.DataType.DATA_BUFFER);
        memoryDataManager.append(createRecord(2), 2, Buffer.DataType.DATA_BUFFER);
        assertThat(memoryDataManager.getNumTotalRequestedBuffers()).isEqualTo(3);
        List<Tuple2<Integer, Integer>> expectedFinishedBufferAndPoolSize =
                Arrays.asList(Tuple2.of(1, 3), Tuple2.of(2, 3), Tuple2.of(3, 3));
        assertThat(numFinishedBufferAndPoolSize).isEqualTo(expectedFinishedBufferAndPoolSize);
    }

    @Test
    void testHandleDecision() throws Exception {
        final int targetSubpartition = 0;
        final int numFinishedBufferToTriggerDecision = 4;
        List<BufferIndexAndChannel> toSpill =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(
                        targetSubpartition, 0, 1, 2);
        List<BufferIndexAndChannel> toRelease =
                HybridShuffleTestUtils.createBufferIndexAndChannelsList(targetSubpartition, 2, 3);
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (numFinishedBuffers, poolSize) -> {
                                    if (numFinishedBuffers < numFinishedBufferToTriggerDecision) {
                                        return Optional.of(Decision.NO_ACTION);
                                    }
                                    return Optional.of(
                                            Decision.builder()
                                                    .addBufferToSpill(targetSubpartition, toSpill)
                                                    .addBufferToRelease(
                                                            targetSubpartition, toRelease)
                                                    .build());
                                })
                        .build();
        CompletableFuture<List<SpilledBuffer>> spilledFuture = new CompletableFuture<>();
        CompletableFuture<Integer> readableFuture = new CompletableFuture<>();
        TestingFileDataIndex dataIndex =
                TestingFileDataIndex.builder()
                        .setAddBuffersConsumer(spilledFuture::complete)
                        .setMarkBufferReadableConsumer(
                                (subpartitionId, bufferIndex) ->
                                        readableFuture.complete(bufferIndex))
                        .build();
        HsMemoryDataManager memoryDataManager =
                createMemoryDataManager(spillingStrategy, dataIndex);
        for (int i = 0; i < 4; i++) {
            memoryDataManager.append(
                    createRecord(i), targetSubpartition, Buffer.DataType.DATA_BUFFER);
        }

        assertThatFuture(spilledFuture).eventuallySucceeds();
        assertThatFuture(readableFuture).eventuallySucceeds();
        assertThat(readableFuture).isCompletedWithValue(2);
        assertThat(memoryDataManager.getNumTotalUnSpillBuffers()).isEqualTo(1);
    }

    @Test
    void testHandleEmptyDecision() throws Exception {
        CompletableFuture<Void> globalDecisionFuture = new CompletableFuture<>();
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnBufferFinishedFunction(
                                (finishedBuffer, poolSize) -> {
                                    // return empty optional to trigger global decision.
                                    return Optional.empty();
                                })
                        .setDecideActionWithGlobalInfoFunction(
                                (provider) -> {
                                    globalDecisionFuture.complete(null);
                                    return Decision.NO_ACTION;
                                })
                        .build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        // trigger an empty decision.
        memoryDataManager.onBufferFinished();
        assertThat(globalDecisionFuture).isCompleted();
    }

    @Test
    void testResultPartitionClosed() throws Exception {
        CompletableFuture<Void> resultPartitionReleaseFuture = new CompletableFuture<>();
        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnResultPartitionClosedFunction(
                                (ignore) -> {
                                    resultPartitionReleaseFuture.complete(null);
                                    return Decision.NO_ACTION;
                                })
                        .build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.close();
        assertThat(resultPartitionReleaseFuture).isCompleted();
    }

    @Test
    void testSubpartitionConsumerRelease() throws Exception {
        HsSpillingStrategy spillingStrategy = TestingSpillingStrategy.builder().build();
        HsMemoryDataManager memoryDataManager = createMemoryDataManager(spillingStrategy);
        memoryDataManager.registerNewConsumer(
                0, HsConsumerId.DEFAULT, new TestingSubpartitionConsumerInternalOperation());
        assertThatThrownBy(
                        () ->
                                memoryDataManager.registerNewConsumer(
                                        0,
                                        HsConsumerId.DEFAULT,
                                        new TestingSubpartitionConsumerInternalOperation()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Each subpartition view should have unique consumerId.");
        memoryDataManager.onConsumerReleased(0, HsConsumerId.DEFAULT);
        memoryDataManager.registerNewConsumer(
                0, HsConsumerId.DEFAULT, new TestingSubpartitionConsumerInternalOperation());
    }

    @Test
    void testPoolSizeCheck() throws Exception {
        final int requiredBuffers = 10;
        final int maxBuffers = 100;
        CompletableFuture<Void> triggerGlobalDecision = new CompletableFuture<>();

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(maxBuffers, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(requiredBuffers, maxBuffers);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(maxBuffers);

        HsSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setDecideActionWithGlobalInfoFunction(
                                (spillingInfoProvider) -> {
                                    assertThat(spillingInfoProvider.getPoolSize())
                                            .isEqualTo(requiredBuffers);
                                    triggerGlobalDecision.complete(null);
                                    return Decision.NO_ACTION;
                                })
                        .build();

        createMemoryDataManager(spillingStrategy, bufferPool);
        networkBufferPool.createBufferPool(maxBuffers - requiredBuffers, maxBuffers);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(requiredBuffers);

        assertThatFuture(triggerGlobalDecision).eventuallySucceeds();
    }

    private HsMemoryDataManager createMemoryDataManager(HsSpillingStrategy spillStrategy)
            throws Exception {
        return createMemoryDataManager(
                spillStrategy,
                new HsFileDataIndexImpl(NUM_SUBPARTITIONS, indexFilePath, 256, Long.MAX_VALUE));
    }

    private HsMemoryDataManager createMemoryDataManager(
            HsSpillingStrategy spillStrategy, HsFileDataIndex fileDataIndex) throws Exception {
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(NUM_BUFFERS, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(poolSize, poolSize);
        return createMemoryDataManager(bufferPool, spillStrategy, fileDataIndex);
    }

    private HsMemoryDataManager createMemoryDataManager(
            HsSpillingStrategy spillingStrategy, BufferPool bufferPool) throws Exception {
        return createMemoryDataManager(
                bufferPool,
                spillingStrategy,
                new HsFileDataIndexImpl(NUM_SUBPARTITIONS, indexFilePath, 256, Long.MAX_VALUE));
    }

    private HsMemoryDataManager createMemoryDataManager(
            BufferPool bufferPool, HsSpillingStrategy spillStrategy, HsFileDataIndex fileDataIndex)
            throws Exception {
        HsMemoryDataManager memoryDataManager =
                new HsMemoryDataManager(
                        NUM_SUBPARTITIONS,
                        bufferSize,
                        bufferPool,
                        spillStrategy,
                        fileDataIndex,
                        dataFilePath,
                        null,
                        1000);
        memoryDataManager.setOutputMetrics(createTestingOutputMetrics());
        return memoryDataManager;
    }

    private static ByteBuffer createRecord(int value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        byteBuffer.putInt(value);
        byteBuffer.flip();
        return byteBuffer;
    }
}

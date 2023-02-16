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

import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView.AvailabilityWithBacklog;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.partition.hybrid.HybridShuffleTestUtils.createTestingOutputMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link HsSubpartitionConsumer}. */
class HsSubpartitionViewTest {
    @Test
    void testGetNextBufferFromDisk() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        CompletableFuture<Void> consumeBufferFromMemoryFuture = new CompletableFuture<>();
        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (ignore) -> {
                                    consumeBufferFromMemoryFuture.complete(null);
                                    return Optional.empty();
                                })
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);

        BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
        assertThat(consumeBufferFromMemoryFuture).isNotCompleted();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    @Timeout(60)
    void testDeadLock(@TempDir Path dataFilePath) throws Exception {
        final int bufferSize = 16;
        NetworkBufferPool networkBufferPool = new NetworkBufferPool(10, bufferSize);
        BufferPool bufferPool = networkBufferPool.createBufferPool(10, 10);
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        CompletableFuture<Void> acquireWriteLock = new CompletableFuture<>();

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        // blocking until other thread acquire write lock.
                        acquireWriteLock.get();
                        subpartitionView.getNextBuffer();
                    }
                };

        TestingSpillingStrategy spillingStrategy =
                TestingSpillingStrategy.builder()
                        .setOnMemoryUsageChangedFunction((ignore1, ignore2) -> Optional.empty())
                        .setDecideActionWithGlobalInfoFunction(
                                (spillingInfoProvider) -> {
                                    acquireWriteLock.complete(null);
                                    try {
                                        consumerThread.trySync(10);
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                    spillingInfoProvider.getNextBufferIndexToConsume(
                                            HsConsumerId.DEFAULT);
                                    return HsSpillingStrategy.Decision.NO_ACTION;
                                })
                        .build();
        HsMemoryDataManager memoryDataManager =
                new HsMemoryDataManager(
                        1,
                        bufferSize,
                        bufferPool,
                        spillingStrategy,
                        new HsFileDataIndexImpl(
                                1, dataFilePath.resolve(".index"), 256, Long.MAX_VALUE),
                        dataFilePath.resolve(".data"),
                        null,
                        0);
        memoryDataManager.setOutputMetrics(createTestingOutputMetrics());
        HsDataView hsDataView =
                memoryDataManager.registerNewConsumer(0, HsConsumerId.DEFAULT, subpartitionView);
        subpartitionView.setMemoryDataView(hsDataView);
        subpartitionView.setDiskDataView(TestingHsDataView.NO_OP);

        consumerThread.start();
        // trigger request buffer.
        memoryDataManager.append(ByteBuffer.allocate(bufferSize), 0, DataType.DATA_BUFFER);
    }

    @Test
    void testGetNextBufferFromDiskNextDataTypeIsNone() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(0, DataType.NONE, 0);

        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();

        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setPeekNextToConsumeDataTypeFunction(
                                (bufferToConsume) -> {
                                    assertThat(bufferToConsume).isEqualTo(1);
                                    return DataType.EVENT_BUFFER;
                                })
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);

        BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
        assertThat(nextBuffer).isNotNull();
        assertThat(nextBuffer.buffer()).isSameAs(bufferAndBacklog.buffer());
        assertThat(nextBuffer.buffersInBacklog()).isEqualTo(bufferAndBacklog.buffersInBacklog());
        assertThat(nextBuffer.getSequenceNumber()).isEqualTo(bufferAndBacklog.getSequenceNumber());
        assertThat(nextBuffer.getNextDataType()).isEqualTo(DataType.EVENT_BUFFER);
    }

    @Test
    void testGetNextBufferFromMemory() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        BufferAndBacklog bufferAndBacklog = createBufferAndBacklog(1, DataType.DATA_BUFFER, 0);
        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(bufferAndBacklog))
                        .build();
        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction((bufferToConsume) -> Optional.empty())
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);

        BufferAndBacklog nextBuffer = subpartitionView.getNextBuffer();
        assertThat(nextBuffer).isSameAs(bufferAndBacklog);
    }

    @Test
    void testGetNextBufferThrowException() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) -> {
                                    throw new RuntimeException("expected exception.");
                                })
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP);

        subpartitionView.getNextBuffer();
        assertThat(subpartitionView.getFailureCause())
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("expected exception.");
        assertThat(subpartitionView.isReleased()).isTrue();
    }

    @Test
    void testGetNextBufferZeroBacklog() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        final int diskBacklog = 0;
        final int memoryBacklog = 10;
        BufferAndBacklog targetBufferAndBacklog =
                createBufferAndBacklog(diskBacklog, DataType.DATA_BUFFER, 0);

        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) -> Optional.of(targetBufferAndBacklog))
                        .build();
        TestingHsDataView memoryDataView =
                TestingHsDataView.builder().setGetBacklogSupplier(() -> memoryBacklog).build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);

        assertThat(subpartitionView.getNextBuffer())
                .satisfies(
                        (bufferAndBacklog -> {
                            // backlog is reset to maximum backlog of memory and disk.
                            assertThat(bufferAndBacklog.buffersInBacklog())
                                    .isEqualTo(memoryBacklog);
                            // other field is not changed.
                            assertThat(bufferAndBacklog.buffer())
                                    .isEqualTo(targetBufferAndBacklog.buffer());
                            assertThat(bufferAndBacklog.getNextDataType())
                                    .isEqualTo(targetBufferAndBacklog.getNextDataType());
                            assertThat(bufferAndBacklog.getSequenceNumber())
                                    .isEqualTo(targetBufferAndBacklog.getSequenceNumber());
                        }));
    }

    @Test
    void testNotifyDataAvailableNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        HsSubpartitionConsumer subpartitionView =
                createSubpartitionView(() -> notifyAvailableFuture.complete(null));

        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) ->
                                        Optional.of(createBufferAndBacklog(0, DataType.NONE, 0)))
                        .build();
        subpartitionView.setMemoryDataView(memoryDataView);
        subpartitionView.setDiskDataView(TestingHsDataView.NO_OP);

        subpartitionView.getNextBuffer();
        subpartitionView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testNotifyDataAvailableNotNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        HsSubpartitionConsumer subpartitionView =
                createSubpartitionView(() -> notifyAvailableFuture.complete(null));

        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (bufferToConsume) ->
                                        Optional.of(
                                                createBufferAndBacklog(0, DataType.DATA_BUFFER, 0)))
                        .build();
        subpartitionView.setMemoryDataView(memoryDataView);
        subpartitionView.setDiskDataView(TestingHsDataView.NO_OP);

        subpartitionView.getNextBuffer();
        subpartitionView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isNotCompleted();
    }

    @Test
    void testGetZeroBacklogNeedNotify() {
        CompletableFuture<Void> notifyAvailableFuture = new CompletableFuture<>();
        HsSubpartitionConsumer subpartitionView =
                createSubpartitionView(() -> notifyAvailableFuture.complete(null));
        subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP);
        subpartitionView.setDiskDataView(
                TestingHsDataView.builder().setGetBacklogSupplier(() -> 0).build());

        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isZero();

        assertThat(notifyAvailableFuture).isNotCompleted();
        subpartitionView.notifyDataAvailable();
        assertThat(notifyAvailableFuture).isCompleted();
    }

    @Test
    void testGetAvailabilityAndBacklogPositiveCredit() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP);

        final int backlog = 2;
        subpartitionView.setDiskDataView(
                TestingHsDataView.builder().setGetBacklogSupplier(() -> backlog).build());
        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(1);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // positive credit always available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsData() {
        final int backlog = 2;

        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        subpartitionView.setMemoryDataView(
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        backlog, DataType.DATA_BUFFER, 0)))
                        .build());
        subpartitionView.setDiskDataView(
                TestingHsDataView.builder().setGetBacklogSupplier(() -> backlog).build());

        subpartitionView.getNextBuffer();

        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isFalse();
    }

    @Test
    void testGetAvailabilityAndBacklogNonPositiveCreditNextIsEvent() {
        final int backlog = 2;

        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        subpartitionView.setMemoryDataView(
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (nextToConsume) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        backlog, DataType.EVENT_BUFFER, 0)))
                        .build());
        subpartitionView.setDiskDataView(
                TestingHsDataView.builder().setGetBacklogSupplier(() -> backlog).build());

        subpartitionView.getNextBuffer();

        AvailabilityWithBacklog availabilityAndBacklog =
                subpartitionView.getAvailabilityAndBacklog(0);
        assertThat(availabilityAndBacklog.getBacklog()).isEqualTo(backlog);
        // if credit is non-positive, only event can be available.
        assertThat(availabilityAndBacklog.isAvailable()).isTrue();
    }

    @Test
    void testRelease() throws Exception {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        CompletableFuture<Void> releaseDiskViewFuture = new CompletableFuture<>();
        CompletableFuture<Void> releaseMemoryViewFuture = new CompletableFuture<>();
        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setReleaseDataViewRunnable(() -> releaseDiskViewFuture.complete(null))
                        .build();
        TestingHsDataView memoryDataView =
                TestingHsDataView.builder()
                        .setReleaseDataViewRunnable(() -> releaseMemoryViewFuture.complete(null))
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);
        subpartitionView.releaseAllResources();
        assertThat(subpartitionView.isReleased()).isTrue();
        assertThat(releaseDiskViewFuture).isCompleted();
        assertThat(releaseMemoryViewFuture).isCompleted();
    }

    @Test
    void testGetConsumingOffset() {
        AtomicInteger nextBufferIndex = new AtomicInteger(0);
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();
        TestingHsDataView diskDataView =
                TestingHsDataView.builder()
                        .setConsumeBufferFunction(
                                (toConsumeBuffer) ->
                                        Optional.of(
                                                createBufferAndBacklog(
                                                        0,
                                                        DataType.DATA_BUFFER,
                                                        nextBufferIndex.getAndIncrement())))
                        .build();
        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP);

        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(-1);
        subpartitionView.getNextBuffer();
        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(0);
        subpartitionView.getNextBuffer();
        assertThat(subpartitionView.getConsumingOffset(true)).isEqualTo(1);
    }

    @Test
    void testSetDataViewRepeatedly() {
        HsSubpartitionConsumer subpartitionView = createSubpartitionView();

        subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP);
        assertThatThrownBy(() -> subpartitionView.setMemoryDataView(TestingHsDataView.NO_OP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeatedly set memory data view is not allowed.");

        subpartitionView.setDiskDataView(TestingHsDataView.NO_OP);
        assertThatThrownBy(() -> subpartitionView.setDiskDataView(TestingHsDataView.NO_OP))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("repeatedly set disk data view is not allowed.");
    }

    private static HsSubpartitionConsumer createSubpartitionView() {
        return new HsSubpartitionConsumer(new NoOpBufferAvailablityListener());
    }

    private static HsSubpartitionConsumer createSubpartitionView(
            BufferAvailabilityListener bufferAvailabilityListener) {
        return new HsSubpartitionConsumer(bufferAvailabilityListener);
    }

    private static BufferAndBacklog createBufferAndBacklog(
            int buffersInBacklog, DataType nextDataType, int sequenceNumber) {
        final int bufferSize = 8;
        Buffer buffer = HybridShuffleTestUtils.createBuffer(bufferSize, true);
        return new BufferAndBacklog(buffer, buffersInBacklog, nextDataType, sequenceNumber);
    }
}

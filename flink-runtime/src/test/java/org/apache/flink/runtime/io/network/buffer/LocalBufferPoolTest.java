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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.fs.AutoCloseableRegistry;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link LocalBufferPool}. */
class LocalBufferPoolTest {

    private static final int numBuffers = 1024;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool networkBufferPool;

    private BufferPool localBufferPool;

    @RegisterExtension
    public static final TestExecutorExtension<ExecutorService> EXECUTOR_RESOURCE =
            new TestExecutorExtension<>(Executors::newCachedThreadPool);

    @BeforeEach
    void setupLocalBufferPool() {
        networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
        localBufferPool = new LocalBufferPool(networkBufferPool, 1);

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(1);
    }

    @AfterEach
    void destroyAndVerifyAllBuffersReturned() {
        if (!localBufferPool.isDestroyed()) {
            localBufferPool.lazyDestroy();
        }

        String msg = "Did not return all buffers to memory segment pool after test.";
        assertThat(networkBufferPool.getNumberOfAvailableMemorySegments())
                .withFailMessage(msg)
                .isEqualTo(numBuffers);
        // no other local buffer pools used than the one above, but call just in case
        networkBufferPool.destroyAllBufferPools();
        networkBufferPool.destroy();
    }

    @Test
    void testReserveSegments() throws Exception {
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(2, memorySegmentSize, Duration.ofSeconds(2));
        try {
            BufferPool bufferPool1 = networkBufferPool.createBufferPool(1, 2);
            assertThatThrownBy(() -> bufferPool1.reserveSegments(2))
                    .isInstanceOf(IllegalArgumentException.class);

            // request all buffers
            ArrayList<Buffer> buffers = new ArrayList<>(2);
            buffers.add(bufferPool1.requestBuffer());
            buffers.add(bufferPool1.requestBuffer());
            assertThat(buffers).hasSize(2);

            BufferPool bufferPool2 = networkBufferPool.createBufferPool(1, 10);
            assertThatThrownBy(() -> bufferPool2.reserveSegments(1))
                    .isInstanceOf(IOException.class);
            assertThat(bufferPool2.isAvailable()).isFalse();

            buffers.forEach(Buffer::recycleBuffer);
            bufferPool1.lazyDestroy();
            bufferPool2.lazyDestroy();

            BufferPool bufferPool3 = networkBufferPool.createBufferPool(2, 10);
            assertThat(bufferPool3.getNumberOfAvailableMemorySegments()).isEqualTo(1);
            bufferPool3.reserveSegments(2);
            assertThat(bufferPool3.getNumberOfAvailableMemorySegments()).isEqualTo(2);

            bufferPool3.lazyDestroy();
            assertThatThrownBy(() -> bufferPool3.reserveSegments(1))
                    .isInstanceOf(CancelTaskException.class);
        } finally {
            networkBufferPool.destroy();
        }
    }

    @Test
    @Timeout(10) // timeout can indicate a potential deadlock
    void testReserveSegmentsAndCancel() throws Exception {
        int totalSegments = 4;
        int segmentsToReserve = 2;

        NetworkBufferPool globalPool = new NetworkBufferPool(totalSegments, memorySegmentSize);
        BufferPool localPool1 = globalPool.createBufferPool(segmentsToReserve, totalSegments);
        List<MemorySegment> segments = new ArrayList<>();

        try {
            for (int i = 0; i < totalSegments; ++i) {
                segments.add(localPool1.requestMemorySegmentBlocking());
            }

            BufferPool localPool2 = globalPool.createBufferPool(segmentsToReserve, totalSegments);
            // the segment reserve thread will be blocked for no buffer is available
            Thread reserveThread =
                    new Thread(
                            () -> {
                                try {
                                    localPool2.reserveSegments(segmentsToReserve);
                                } catch (Throwable ignored) {
                                }
                            });
            reserveThread.start();
            Thread.sleep(100); // wait to be blocked

            // the cancel thread can be blocked when redistributing buffers
            Thread cancelThread =
                    new Thread(
                            () -> {
                                localPool1.lazyDestroy();
                                localPool2.lazyDestroy();
                            });
            cancelThread.start();

            // it is expected that the segment reserve thread can be cancelled successfully
            Thread interruptThread =
                    new Thread(
                            () -> {
                                try {
                                    do {
                                        reserveThread.interrupt();
                                        Thread.sleep(100);
                                    } while (reserveThread.isAlive() || cancelThread.isAlive());
                                } catch (Throwable ignored) {
                                }
                            });
            interruptThread.start();

            interruptThread.join();
        } finally {
            segments.forEach(localPool1::recycle);
            localPool1.lazyDestroy();
            assertThat(globalPool.getNumberOfUsedMemorySegments()).isZero();
            globalPool.destroy();
        }
    }

    @Test
    void testRequestMoreThanAvailable() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<>(numBuffers);

        for (int i = 1; i <= numBuffers; i++) {
            Buffer buffer = localBufferPool.requestBuffer();

            assertThat(getNumRequestedFromMemorySegmentPool())
                    .isEqualTo(Math.min(i + 1, numBuffers));
            assertThat(buffer).isNotNull();

            requests.add(buffer);
        }

        {
            // One more...
            Buffer buffer = localBufferPool.requestBuffer();
            assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);
            assertThat(buffer).isNull();
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    void testSetNumAfterDestroyDoesNotProactivelyFetchSegments() {
        localBufferPool.setNumBuffers(2);
        assertThat(localBufferPool.getNumBuffers()).isEqualTo(2);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();

        localBufferPool.lazyDestroy();
        localBufferPool.setNumBuffers(3);
        assertThat(localBufferPool.getNumBuffers()).isEqualTo(3);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
    }

    @Test
    void testRecycleAfterDestroy() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<>(numBuffers);

        for (int i = 0; i < numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        localBufferPool.lazyDestroy();

        // All buffers have been requested, but can not be returned yet.
        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        // Recycle should return buffers to memory segment pool
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    void testDecreasePoolSize() throws Exception {
        final int maxMemorySegments = 10;
        final int requiredMemorySegments = 4;

        // requested buffers is equal to small pool size.
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 7, 5, 2, 5, 0, 5, 0);
        // requested buffers is less than small pool size.
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 6, 4, 2, 2, 0, 3, 1);
        // exceed buffers is equal to maxOverdraftBuffers
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 7, 5, 2, 7, 2, 5, 0);
        // exceed buffers is greater than maxOverdraftBuffers
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 9, 5, 3, 9, 4, 5, 0);
        // exceed buffers is less than maxOverdraftBuffers
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 7, 5, 4, 7, 2, 5, 0);
        // decrease pool size with overdraft buffer.
        testDecreasePoolSizeInternal(
                maxMemorySegments, requiredMemorySegments, 7, 5, 6, 9, 4, 5, 0);
    }

    void testDecreasePoolSizeInternal(
            int maxMemorySegments,
            int requiredMemorySegments,
            int largePoolSize,
            int smallPoolSize,
            int maxOverdraftBuffers,
            int numBuffersToRequest,
            int numRequestedOverdraftBuffersAfterDecreasing,
            int numRequestedOrdinaryBuffersAfterDecreasing,
            int numAvailableBuffersAfterDecreasing)
            throws Exception {
        LocalBufferPool bufferPool =
                new LocalBufferPool(
                        networkBufferPool,
                        requiredMemorySegments,
                        maxMemorySegments,
                        0,
                        Integer.MAX_VALUE,
                        maxOverdraftBuffers);
        List<MemorySegment> buffers = new ArrayList<>();

        // set a larger pool size.
        bufferPool.setNumBuffers(largePoolSize);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(largePoolSize);

        // request buffers.
        for (int i = 0; i < numBuffersToRequest; i++) {
            buffers.add(bufferPool.requestMemorySegmentBlocking());
        }

        // set a small pool size.
        bufferPool.setNumBuffers(smallPoolSize);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(smallPoolSize);
        assertThat(getNumberRequestedOverdraftBuffers(bufferPool))
                .isEqualTo(numRequestedOverdraftBuffersAfterDecreasing);
        assertThat(getNumberRequestedOrdinaryBuffers(bufferPool))
                .isEqualTo(numRequestedOrdinaryBuffersAfterDecreasing);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(numAvailableBuffersAfterDecreasing);
        assertThat(bufferPool.isAvailable()).isEqualTo(numAvailableBuffersAfterDecreasing > 0);

        for (MemorySegment buffer : buffers) {
            bufferPool.recycle(buffer);
        }
        bufferPool.lazyDestroy();
    }

    @Test
    void testIncreasePoolSize() throws Exception {
        final int maxMemorySegments = 100;
        final int requiredMemorySegments = 5;
        final int smallPoolSize = 5;
        final int maxOverdraftBuffers = 2;
        // test increase pool size not exceed total buffers.
        testIncreasePoolSizeInternal(
                maxMemorySegments,
                requiredMemorySegments,
                smallPoolSize,
                6,
                maxOverdraftBuffers,
                1,
                0,
                false);
        // test increase pool size to total buffers.
        testIncreasePoolSizeInternal(
                maxMemorySegments,
                requiredMemorySegments,
                smallPoolSize,
                7,
                maxOverdraftBuffers,
                0,
                0,
                false);
        // test increase pool size exceed total buffers.
        testIncreasePoolSizeInternal(
                maxMemorySegments,
                requiredMemorySegments,
                smallPoolSize,
                8,
                maxOverdraftBuffers,
                0,
                1,
                true);
        // test increase pool size exceed total buffers and reach maxMemorySegments.
        testIncreasePoolSizeInternal(10, 8, 8, 10, maxOverdraftBuffers, 0, 0, false);
    }

    void testIncreasePoolSizeInternal(
            int maxMemorySegments,
            int requiredMemorySegments,
            int smallPoolSize,
            int largePoolSize,
            int maxOverdraftBuffers,
            int numOverdraftBuffersAfterIncreasePoolSize,
            int numAvailableBuffersAfterIncreasePoolSize,
            boolean isAvailableAfterIncreasePoolSize)
            throws Exception {
        LocalBufferPool bufferPool =
                new LocalBufferPool(
                        networkBufferPool,
                        requiredMemorySegments,
                        maxMemorySegments,
                        0,
                        Integer.MAX_VALUE,
                        maxOverdraftBuffers);
        List<MemorySegment> buffers = new ArrayList<>();

        // set a small pool size.
        bufferPool.setNumBuffers(smallPoolSize);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(smallPoolSize);

        // request all buffer.
        for (int i = 0; i < smallPoolSize; i++) {
            buffers.add(bufferPool.requestMemorySegmentBlocking());
        }
        assertThat(bufferPool.isAvailable()).isFalse();

        // request all overdraft buffers.
        for (int i = 0; i < maxOverdraftBuffers; i++) {
            buffers.add(bufferPool.requestMemorySegmentBlocking());
        }
        assertThat(bufferPool.requestMemorySegment()).isNull();
        assertThat(getNumberRequestedOverdraftBuffers(bufferPool)).isEqualTo(maxOverdraftBuffers);
        assertThat(bufferPool.isAvailable()).isFalse();

        // set a large pool size.
        bufferPool.setNumBuffers(largePoolSize);
        assertThat(bufferPool.getNumBuffers()).isEqualTo(largePoolSize);
        assertThat(bufferPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(numAvailableBuffersAfterIncreasePoolSize);
        assertThat(getNumberRequestedOverdraftBuffers(bufferPool))
                .isEqualTo(numOverdraftBuffersAfterIncreasePoolSize);
        assertThat(bufferPool.isAvailable()).isEqualTo(isAvailableAfterIncreasePoolSize);

        for (MemorySegment buffer : buffers) {
            bufferPool.recycle(buffer);
        }
        bufferPool.lazyDestroy();
    }

    @Test
    @Timeout(30)
    void testRequestBufferOnRecycleWithOverdraft() throws Exception {
        testRequestBuffersOnRecycle(true);
    }

    @Test
    @Timeout(30)
    void testRequestBufferOnRecycleWithoutOverdraft() throws Exception {
        testRequestBuffersOnRecycle(false);
    }

    private void testRequestBuffersOnRecycle(boolean supportOverdraftBuffer) throws Exception {
        BufferPool bufferPool1 =
                networkBufferPool.createBufferPool(
                        512, 2048, 0, Integer.MAX_VALUE, supportOverdraftBuffer ? 5 : 0);
        List<MemorySegment> segments = new ArrayList<>();
        for (int i = 0; i < 1023; i++) {
            segments.add(bufferPool1.requestMemorySegmentBlocking());
        }
        BufferPool bufferPool2 =
                networkBufferPool.createBufferPool(
                        512, 512, 0, Integer.MAX_VALUE, supportOverdraftBuffer ? 5 : 0);
        List<MemorySegment> segments2 = new ArrayList<>();
        CheckedThread checkedThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        for (int i = 0; i < 512; i++) {
                            segments2.add(bufferPool2.requestMemorySegmentBlocking());
                        }
                    }
                };
        checkedThread.start();
        for (MemorySegment segment : segments) {
            bufferPool1.recycle(segment);
        }
        bufferPool1.lazyDestroy();
        checkedThread.sync();
        for (MemorySegment segment : segments2) {
            bufferPool2.recycle(segment);
        }
        bufferPool2.lazyDestroy();
    }

    @Test
    void testRecycleExcessBuffersAfterRecycling() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        // Reduce the number of buffers in the local pool
        localBufferPool.setNumBuffers(numBuffers / 2);

        // Need to wait until we recycle the buffers
        assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers);

        for (int i = 1; i < numBuffers / 2; i++) {
            requests.remove(0).recycleBuffer();
            assertThat(getNumRequestedFromMemorySegmentPool()).isEqualTo(numBuffers - i);
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    void testRecycleExcessBuffersAfterChangingNumBuffers() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        // Recycle all
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);

        localBufferPool.setNumBuffers(numBuffers / 2);

        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers / 2);
    }

    @Test
    void testSetLessThanRequiredNumBuffers() {
        localBufferPool.setNumBuffers(1);

        assertThatThrownBy(() -> localBufferPool.setNumBuffers(0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ------------------------------------------------------------------------
    // Pending requests and integration with buffer futures
    // ------------------------------------------------------------------------

    @Test
    void testPendingRequestWithListenersAfterRecycle() {
        CountBufferListener listener1 = new CountBufferListener();
        CountBufferListener listener2 = new CountBufferListener();

        Buffer available = localBufferPool.requestBuffer();

        assertThat(localBufferPool.requestBuffer()).isNull();

        assertThat(localBufferPool.addBufferListener(listener1)).isTrue();
        assertThat(localBufferPool.addBufferListener(listener2)).isTrue();

        // Recycle the buffer to notify both of the above listeners once
        checkNotNull(available).recycleBuffer();

        assertThat(listener1.getCount()).isOne();
        assertThat(listener1.getCount()).isOne();

        assertThat(localBufferPool.addBufferListener(listener1)).isFalse();
        assertThat(localBufferPool.addBufferListener(listener2)).isFalse();
    }

    @Test
    void testCancelPendingRequestsAfterDestroy() {
        AtomicInteger invokeNotifyBufferDestroyedCounter = new AtomicInteger(0);

        BufferListener listener =
                TestingBufferListener.builder()
                        .setNotifyBufferDestroyedRunnable(
                                invokeNotifyBufferDestroyedCounter::incrementAndGet)
                        .build();

        localBufferPool.setNumBuffers(1);

        Buffer available = localBufferPool.requestBuffer();
        Buffer unavailable = localBufferPool.requestBuffer();

        assertThat(available).isNotNull();
        assertThat(unavailable).isNull();

        localBufferPool.addBufferListener(listener);

        localBufferPool.lazyDestroy();

        available.recycleBuffer();

        assertThat(invokeNotifyBufferDestroyedCounter).hasValue(1);
    }

    // ------------------------------------------------------------------------
    // Concurrent requests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void testConcurrentRequestRecycle() throws ExecutionException, InterruptedException {
        int numConcurrentTasks = 128;
        int numBuffersToRequestPerTask = 1024;

        localBufferPool.setNumBuffers(numConcurrentTasks);

        Future<Boolean>[] taskResults = new Future[numConcurrentTasks];
        for (int i = 0; i < numConcurrentTasks; i++) {
            taskResults[i] =
                    EXECUTOR_RESOURCE
                            .getExecutor()
                            .submit(
                                    new BufferRequesterTask(
                                            localBufferPool, numBuffersToRequestPerTask));
        }

        for (int i = 0; i < numConcurrentTasks; i++) {
            assertThat(taskResults[i].get()).isTrue();
        }
    }

    @Test
    void testBoundedBuffer() {
        localBufferPool.lazyDestroy();

        localBufferPool = new LocalBufferPool(networkBufferPool, 1, 2);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(localBufferPool.getMaxNumberOfMemorySegments()).isEqualTo(2);

        Buffer buffer1, buffer2;

        // check min number of buffers:
        localBufferPool.setNumBuffers(1);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();

        // check max number of buffers:
        localBufferPool.setNumBuffers(2);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(buffer2 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        buffer2.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // try to set too large buffer size:
        localBufferPool.setNumBuffers(3);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(buffer2 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        assertThat(localBufferPool.requestBuffer()).isNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        buffer2.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isEqualTo(2);

        // decrease size again
        localBufferPool.setNumBuffers(1);
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
        assertThat(buffer1 = localBufferPool.requestBuffer()).isNotNull();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isZero();
        assertThat(localBufferPool.requestBuffer()).isNull();
        buffer1.recycleBuffer();
        assertThat(localBufferPool.getNumberOfAvailableMemorySegments()).isOne();
    }

    /** Moves around availability of a {@link LocalBufferPool} with varying capacity. */
    @Test
    void testMaxBuffersPerChannelAndAvailability() throws Exception {
        localBufferPool.lazyDestroy();
        localBufferPool = new LocalBufferPool(networkBufferPool, 1, Integer.MAX_VALUE, 3, 2, 0);
        localBufferPool.setNumBuffers(10);

        assertThat(localBufferPool.getAvailableFuture()).isDone();

        // request one segment from subpartition-0 and subpartition-1 respectively
        final BufferBuilder bufferBuilder01 = localBufferPool.requestBufferBuilderBlocking(0);
        final BufferBuilder bufferBuilder11 = localBufferPool.requestBufferBuilderBlocking(1);
        assertThat(localBufferPool.getAvailableFuture()).isDone();

        // request one segment from subpartition-0
        final BufferBuilder bufferBuilder02 = localBufferPool.requestBufferBuilderBlocking(0);
        assertThat(localBufferPool.getAvailableFuture()).isNotDone();

        // When this channel reaches maxBuffersPerChannel, LocalBufferPool will be unavailable. But
        // when a memory segment is requested from LocalBufferPool and availableMemorySegments isn't
        // empty, maxBuffersPerChannel will be ignored and buffers will continue to be allocated.
        // Check FLINK-27522 for more information.
        final BufferBuilder bufferBuilder03 = localBufferPool.requestBufferBuilderBlocking(0);
        final BufferBuilder bufferBuilder21 = localBufferPool.requestBufferBuilderBlocking(2);
        final BufferBuilder bufferBuilder22 = localBufferPool.requestBufferBuilderBlocking(2);
        assertThat(localBufferPool.getAvailableFuture()).isNotDone();

        // recycle segments
        bufferBuilder11.close();
        assertThat(localBufferPool.getAvailableFuture()).isNotDone();
        bufferBuilder21.close();
        assertThat(localBufferPool.getAvailableFuture()).isNotDone();
        bufferBuilder02.close();
        assertThat(localBufferPool.getAvailableFuture()).isNotDone();
        bufferBuilder01.close();
        assertThat(localBufferPool.getAvailableFuture()).isDone();
        bufferBuilder03.close();
        assertThat(localBufferPool.getAvailableFuture()).isDone();
        bufferBuilder22.close();
        assertThat(localBufferPool.getAvailableFuture()).isDone();
    }

    @Test
    void testIsAvailableOrNot() throws InterruptedException {

        // the local buffer pool should be in available state initially
        assertThat(localBufferPool.isAvailable()).isTrue();

        // request one buffer
        try (BufferBuilder bufferBuilder =
                checkNotNull(localBufferPool.requestBufferBuilderBlocking())) {
            CompletableFuture<?> availableFuture = localBufferPool.getAvailableFuture();
            assertThat(availableFuture).isNotDone();

            // set the pool size
            final int numLocalBuffers = 5;
            localBufferPool.setNumBuffers(numLocalBuffers);
            assertThat(availableFuture).isDone();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // drain the local buffer pool
            final Deque<Buffer> buffers = new ArrayDeque<>(numBuffers);
            for (int i = 0; i < numLocalBuffers - 1; i++) {
                assertThat(localBufferPool.isAvailable()).isTrue();
                buffers.add(checkNotNull(localBufferPool.requestBuffer()));
            }
            assertThat(localBufferPool.isAvailable()).isFalse();

            buffers.pop().recycleBuffer();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // recycle the requested segments to global buffer pool
            for (final Buffer buffer : buffers) {
                buffer.recycleBuffer();
            }
            assertThat(localBufferPool.isAvailable()).isTrue();

            // scale down (first buffer still taken), but there should still be one segment locally
            // available
            localBufferPool.setNumBuffers(2);
            assertThat(localBufferPool.isAvailable()).isTrue();

            final Buffer buffer2 = checkNotNull(localBufferPool.requestBuffer());
            assertThat(localBufferPool.isAvailable()).isFalse();

            buffer2.recycleBuffer();
            assertThat(localBufferPool.isAvailable()).isTrue();

            // reset the pool size
            localBufferPool.setNumBuffers(1);
            assertThat(localBufferPool.getAvailableFuture()).isNotDone();
            // recycle the requested buffer
        }

        assertThat(localBufferPool.isAvailable()).isTrue();
        assertThat(localBufferPool.getAvailableFuture()).isDone();
    }

    /** For FLINK-20547: https://issues.apache.org/jira/browse/FLINK-20547. */
    @Test
    void testConsistentAvailability() throws Exception {
        NetworkBufferPool globalPool = new TestNetworkBufferPool(numBuffers, memorySegmentSize);
        try {
            BufferPool localPool = new LocalBufferPool(globalPool, 1);
            MemorySegment segment = localPool.requestMemorySegmentBlocking();
            localPool.setNumBuffers(2);

            localPool.recycle(segment);
            localPool.lazyDestroy();
        } finally {
            globalPool.destroy();
        }
    }

    @Test
    void testOverdraftBufferAndAvailability() throws Exception {
        for (int maxOverdraftBuffers = 0; maxOverdraftBuffers < 3; maxOverdraftBuffers++) {
            useAllOverdraftBuffersAndCheckIsLegal(4, 3, maxOverdraftBuffers, 2, 1);
            useAllOverdraftBuffersAndCheckIsLegal(4, 3, maxOverdraftBuffers, 2, 2);
            useAllOverdraftBuffersAndCheckIsLegal(4, 3, maxOverdraftBuffers, 3, 2);

            useAllOverdraftBuffersAndCheckIsLegal(8, 5, maxOverdraftBuffers, 2, 1);
            useAllOverdraftBuffersAndCheckIsLegal(8, 5, maxOverdraftBuffers, 2, 2);
            useAllOverdraftBuffersAndCheckIsLegal(8, 5, maxOverdraftBuffers, 3, 2);

            useAllOverdraftBuffersAndCheckIsLegal(12, 10, maxOverdraftBuffers, 2, 1);
            useAllOverdraftBuffersAndCheckIsLegal(12, 10, maxOverdraftBuffers, 2, 2);
            useAllOverdraftBuffersAndCheckIsLegal(12, 10, maxOverdraftBuffers, 3, 2);
        }
    }

    private void useAllOverdraftBuffersAndCheckIsLegal(
            int poolSize,
            int maxBuffersPerChannel,
            int maxOverdraftBuffers,
            int numberOfChannels,
            int availableChannels)
            throws Exception {
        checkArgument(maxBuffersPerChannel > poolSize / numberOfChannels);
        checkArgument(numberOfChannels >= availableChannels);
        LocalBufferPool bufferPool =
                new LocalBufferPool(
                        networkBufferPool,
                        1,
                        Integer.MAX_VALUE,
                        numberOfChannels,
                        maxBuffersPerChannel,
                        maxOverdraftBuffers);
        bufferPool.setNumBuffers(poolSize);

        // Request all buffers inside the buffer pool
        Map<Integer, AutoCloseableRegistry> closeableRegistryMap = new HashMap<>();
        for (int i = 0; i < poolSize; i++) {
            int targetChannel = i % availableChannels;
            BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetChannel);
            assertThat(bufferBuilder).isNotNull();
            closeableRegistryMap
                    .computeIfAbsent(targetChannel, channel -> new AutoCloseableRegistry())
                    .registerCloseable(bufferBuilder);
            boolean isAvailable =
                    (i + 1 < poolSize) && i < availableChannels * (maxBuffersPerChannel - 1);
            assertRequestedBufferAndIsAvailable(bufferPool, 0, i + 1, isAvailable);
        }

        // request overdraft buffer
        AutoCloseableRegistry overdraftCloseableRegistry = new AutoCloseableRegistry();
        for (int i = 0; i < maxOverdraftBuffers; i++) {
            int targetChannel = i % availableChannels;
            BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetChannel);
            assertThat(bufferBuilder).isNotNull();
            overdraftCloseableRegistry.registerCloseable(bufferBuilder);
            int numberOfRequestedOverdraftBuffer = i + 1;
            assertRequestedBufferAndIsAvailable(
                    bufferPool,
                    numberOfRequestedOverdraftBuffer,
                    poolSize + numberOfRequestedOverdraftBuffer,
                    false);
        }

        for (int i = 0; i < numberOfChannels; i++) {
            assertThat(bufferPool.requestBufferBuilder(i)).isNull();
            assertRequestedBufferAndIsAvailable(
                    bufferPool, maxOverdraftBuffers, poolSize + maxOverdraftBuffers, false);
        }

        // release all bufferBuilder
        overdraftCloseableRegistry.close();
        assertRequestedBufferAndIsAvailable(bufferPool, 0, poolSize, false);
        int numberOfRequestedBuffer = poolSize;
        for (AutoCloseableRegistry closeableRegistry : closeableRegistryMap.values()) {
            numberOfRequestedBuffer =
                    numberOfRequestedBuffer - closeableRegistry.getNumberOfRegisteredCloseables();
            closeableRegistry.close();
            assertRequestedBufferAndIsAvailable(bufferPool, 0, numberOfRequestedBuffer, true);
        }
        bufferPool.lazyDestroy();
    }

    private void assertRequestedBufferAndIsAvailable(
            LocalBufferPool bufferPool,
            int numberOfRequestedOverdraftBuffer,
            int numberOfRequestedBuffer,
            boolean isAvailable) {
        if (numberOfRequestedOverdraftBuffer > 0) {
            checkArgument(!isAvailable);
        }
        assertThat(getNumberRequestedOverdraftBuffers(bufferPool))
                .isEqualTo(numberOfRequestedOverdraftBuffer);

        assertThat(bufferPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(numberOfRequestedBuffer);
        assertThat(bufferPool.getAvailableFuture().isDone()).isEqualTo(isAvailable);
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private static int getNumberRequestedOverdraftBuffers(LocalBufferPool bufferPool) {
        return Math.max(
                bufferPool.getNumberOfRequestedMemorySegments() - bufferPool.getNumBuffers(), 0);
    }

    private static int getNumberRequestedOrdinaryBuffers(LocalBufferPool bufferPool) {
        return Math.min(
                bufferPool.getNumBuffers(), bufferPool.getNumberOfRequestedMemorySegments());
    }

    private int getNumRequestedFromMemorySegmentPool() {
        return networkBufferPool.getTotalNumberOfMemorySegments()
                - networkBufferPool.getNumberOfAvailableMemorySegments();
    }

    private static class CountBufferListener implements BufferListener {

        private final AtomicInteger times = new AtomicInteger(0);

        @Override
        public boolean notifyBufferAvailable(Buffer buffer) {
            times.incrementAndGet();
            buffer.recycleBuffer();
            return true;
        }

        @Override
        public void notifyBufferDestroyed() {}

        int getCount() {
            return times.get();
        }
    }

    private static class BufferRequesterTask implements Callable<Boolean> {

        private final BufferProvider bufferProvider;

        private final int numBuffersToRequest;

        private BufferRequesterTask(BufferProvider bufferProvider, int numBuffersToRequest) {
            this.bufferProvider = bufferProvider;
            this.numBuffersToRequest = numBuffersToRequest;
        }

        @Override
        public Boolean call() throws Exception {
            try {
                for (int i = 0; i < numBuffersToRequest; i++) {
                    Buffer buffer = checkNotNull(bufferProvider.requestBuffer());
                    buffer.recycleBuffer();
                }
            } catch (Throwable t) {
                return false;
            }

            return true;
        }
    }

    private static class TestNetworkBufferPool extends NetworkBufferPool {

        private int requestCounter;

        public TestNetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
            super(numberOfSegmentsToAllocate, segmentSize);
        }

        @Nullable
        @Override
        public MemorySegment requestPooledMemorySegment() {
            if (requestCounter++ == 1) {
                return null;
            }
            return super.requestPooledMemorySegment();
        }
    }
}

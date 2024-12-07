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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.execution.CancelTaskException;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NetworkBufferPool}. */
class NetworkBufferPoolTest {

    @Test
    void testCreatePoolAfterDestroy() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);
        assertThat(globalPool.getNumberOfRegisteredBufferPools()).isZero();

        globalPool.destroy();

        assertThat(globalPool.isDestroyed()).isTrue();

        assertThatThrownBy(() -> globalPool.createBufferPool(2, 2))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> globalPool.createBufferPool(2, 10))
                .isInstanceOf(IllegalStateException.class);

        assertThatThrownBy(() -> globalPool.createBufferPool(2, Integer.MAX_VALUE))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testMemoryUsageInTheContextOfMemoryPoolCreation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isZero();

        assertThat(globalPool.getTotalMemory()).isEqualTo(numBuffers * bufferSize);
        assertThat(globalPool.getAvailableMemory()).isEqualTo(numBuffers * bufferSize);
        assertThat(globalPool.getUsedMemory()).isZero();
    }

    @Test
    void testMemoryUsageInTheContextOfMemorySegmentAllocation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertThat(segment).isNotNull();

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isEqualTo(numBuffers);
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers - 1);
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isOne();

        assertThat(globalPool.getTotalMemory()).isEqualTo(numBuffers * bufferSize);
        assertThat(globalPool.getAvailableMemory()).isEqualTo((numBuffers - 1) * bufferSize);
        assertThat(globalPool.getUsedMemory()).isEqualTo(bufferSize);
    }

    @Test
    void testMemoryUsageInTheContextOfMemoryPoolDestruction() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        globalPool.destroy();

        assertThat(globalPool.getTotalNumberOfMemorySegments()).isZero();
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isZero();
        assertThat(globalPool.getNumberOfUsedMemorySegments()).isZero();

        assertThat(globalPool.getTotalMemory()).isZero();
        assertThat(globalPool.getAvailableMemory()).isZero();
        assertThat(globalPool.getUsedMemory()).isZero();
    }

    @Test
    void testDestroyAll() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);

        BufferPool fixedPool = globalPool.createBufferPool(2, 2);
        BufferPool boundedPool = globalPool.createBufferPool(1, 1);
        BufferPool nonFixedPool = globalPool.createBufferPool(5, Integer.MAX_VALUE);

        assertThat(fixedPool.getNumberOfRequiredMemorySegments()).isEqualTo(2);
        assertThat(boundedPool.getNumberOfRequiredMemorySegments()).isOne();
        assertThat(nonFixedPool.getNumberOfRequiredMemorySegments()).isEqualTo(5);

        // actually, the buffer pool sizes may be different due to rounding and based on the
        // internal order of
        // the buffer pools - the total number of retrievable buffers should be equal to the number
        // of buffers
        // in the NetworkBufferPool though

        ArrayList<Buffer> buffers = new ArrayList<>(globalPool.getTotalNumberOfMemorySegments());
        collectBuffers:
        for (int i = 0; i < 10; ++i) {
            for (BufferPool bp : new BufferPool[] {fixedPool, boundedPool, nonFixedPool}) {
                Buffer buffer = bp.requestBuffer();
                if (buffer != null) {
                    assertThat(buffer.getMemorySegment()).isNotNull();
                    buffers.add(buffer);
                    continue collectBuffers;
                }
            }
        }

        assertThat(buffers).hasSize(globalPool.getTotalNumberOfMemorySegments());

        assertThat(fixedPool.requestBuffer()).isNull();
        assertThat(boundedPool.requestBuffer()).isNull();
        assertThat(nonFixedPool.requestBuffer()).isNull();

        // destroy all allocated ones
        globalPool.destroyAllBufferPools();

        // check the destroyed status
        assertThat(globalPool.isDestroyed()).isFalse();
        assertThat(fixedPool.isDestroyed()).isTrue();
        assertThat(boundedPool.isDestroyed()).isTrue();
        assertThat(nonFixedPool.isDestroyed()).isTrue();

        assertThat(globalPool.getNumberOfRegisteredBufferPools()).isZero();

        // buffers are not yet recycled
        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isZero();

        // the recycled buffers should go to the global pool
        for (Buffer b : buffers) {
            b.recycleBuffer();
        }
        assertThat(globalPool.getNumberOfAvailableMemorySegments())
                .isEqualTo(globalPool.getTotalNumberOfMemorySegments());

        // can request no more buffers
        assertThatThrownBy(fixedPool::requestBuffer).isInstanceOf(CancelTaskException.class);

        assertThatThrownBy(boundedPool::requestBuffer).isInstanceOf(CancelTaskException.class);

        assertThatThrownBy(nonFixedPool::requestBuffer).isInstanceOf(CancelTaskException.class);

        // can create a new pool now
        assertThat(globalPool.createBufferPool(10, Integer.MAX_VALUE)).isNotNull();
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the {@link
     * NetworkBufferPool} currently containing the number of required free segments.
     */
    @Test
    void testRequestMemorySegmentsLessThanTotalBuffers() throws IOException {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        List<MemorySegment> memorySegments = Collections.emptyList();
        try {
            memorySegments = globalPool.requestUnpooledMemorySegments(numBuffers / 2);
            assertThat(memorySegments).hasSize(numBuffers / 2);

            globalPool.recycleUnpooledMemorySegments(memorySegments);
            memorySegments.clear();
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);
        } finally {
            globalPool.recycleUnpooledMemorySegments(memorySegments); // just in case
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the number of
     * required buffers exceeding the capacity of {@link NetworkBufferPool}.
     */
    @Test
    void testRequestMemorySegmentsMoreThanTotalBuffers() {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            assertThatThrownBy(() -> globalPool.requestUnpooledMemorySegments(numBuffers + 1))
                    .isInstanceOf(IOException.class);
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).isEqualTo(numBuffers);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the total number of
     * allocated buffers for several requests exceeding the capacity of {@link NetworkBufferPool}.
     */
    @Test
    void testInsufficientNumberOfBuffers() throws Exception {
        final int numberOfSegmentsToRequest = 5;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numberOfSegmentsToRequest, 128);

        try {
            // the global pool should be in available state initially
            assertThat(globalPool.getAvailableFuture()).isDone();

            // request 5 segments
            List<MemorySegment> segments1 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture()).isNotDone();
            assertThat(segments1).hasSize(numberOfSegmentsToRequest);

            // request only 1 segment
            assertThatThrownBy(() -> globalPool.requestUnpooledMemorySegments(1))
                    .hasMessageContaining("Insufficient number of network buffers")
                    .isInstanceOf(IOException.class);

            // recycle 5 segments
            CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
            globalPool.recycleUnpooledMemorySegments(segments1);
            assertThat(availableFuture).isDone();

            List<MemorySegment> segments2 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture()).isNotDone();
            assertThat(segments2).hasSize(numberOfSegmentsToRequest);
        } finally {
            globalPool.destroy();
        }
    }

    @Test
    void testEmptyPoolSegmentsUsage() throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            NetworkBufferPool globalPool = new NetworkBufferPool(0, 128);
            closeableRegistry.registerCloseable(globalPool::destroy);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isZero();
        }
    }

    @Test
    void testSegmentsUsage() throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            NetworkBufferPool globalPool = new NetworkBufferPool(50, 128);
            closeableRegistry.registerCloseable(globalPool::destroy);

            BufferPool bufferPool1 = globalPool.createBufferPool(10, 20);

            assertThat(globalPool.getEstimatedNumberOfRequestedMemorySegments()).isEqualTo(20);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isEqualTo(40);
            assertThat(globalPool.getUsageWarning()).isEmpty();

            closeableRegistry.registerCloseable(
                    (globalPool.createBufferPool(5, Integer.MAX_VALUE))::lazyDestroy);

            assertThat(globalPool.getEstimatedNumberOfRequestedMemorySegments()).isEqualTo(30);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isEqualTo(60);
            assertThat(globalPool.getUsageWarning()).isEmpty();

            closeableRegistry.registerCloseable((globalPool.createBufferPool(10, 30))::lazyDestroy);

            assertThat(globalPool.getEstimatedNumberOfRequestedMemorySegments()).isEqualTo(60);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isEqualTo(120);
            assertThat(globalPool.getUsageWarning())
                    .hasValue(
                            "Memory usage [120%] is too high to satisfy all of the requests. "
                                    + "This can severely impact network throughput. "
                                    + "Please consider increasing available network memory, "
                                    + "or decreasing configured size of network buffer pools. ("
                                    + "totalMemory=6.250kb (6400 bytes), "
                                    + "requestedMemory=7.500kb (7680 bytes), "
                                    + "missingMemory=1.250kb (1280 bytes))");
            assertThat(globalPool.getUsageWarning()).isEmpty();

            BufferPool bufferPool2 = globalPool.createBufferPool(10, 20);

            assertThat(globalPool.getEstimatedNumberOfRequestedMemorySegments()).isEqualTo(80);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isEqualTo(160);
            assertThat(globalPool.getUsageWarning())
                    .hasValue(
                            "Memory usage [160%] is too high to satisfy all of the requests. "
                                    + "This can severely impact network throughput. "
                                    + "Please consider increasing available network memory, "
                                    + "or decreasing configured size of network buffer pools. ("
                                    + "totalMemory=6.250kb (6400 bytes), "
                                    + "requestedMemory=10.000kb (10240 bytes), "
                                    + "missingMemory=3.750kb (3840 bytes))");

            bufferPool2.lazyDestroy();
            bufferPool1.lazyDestroy();

            assertThat(globalPool.getEstimatedNumberOfRequestedMemorySegments()).isEqualTo(40);
            assertThat(globalPool.getEstimatedRequestedMemory()).isEqualTo(40 * 128);
            assertThat(globalPool.getEstimatedRequestedSegmentsUsage()).isEqualTo(80);
            assertThat(globalPool.getUsageWarning())
                    .hasValue("Memory usage [80%] went back to normal");
            assertThat(globalPool.getUsageWarning()).isEmpty();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the invalid argument
     * to cause exception.
     */
    @Test
    void testRequestMemorySegmentsWithInvalidArgument() {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        // the number of requested buffers should be non-negative
        assertThatThrownBy(() -> globalPool.requestUnpooledMemorySegments(-1))
                .isInstanceOf(IllegalArgumentException.class);
        globalPool.destroy();
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the {@link
     * NetworkBufferPool} currently not containing the number of required free segments (currently
     * occupied by a buffer pool).
     */
    @Test
    void testRequestMemorySegmentsWithBuffersTaken() throws IOException, InterruptedException {
        final int numBuffers = 10;

        NetworkBufferPool networkBufferPool = new NetworkBufferPool(numBuffers, 128);

        final List<Buffer> buffers = new ArrayList<>(numBuffers);
        List<MemorySegment> memorySegments = Collections.emptyList();
        Thread bufferRecycler = null;
        BufferPool lbp1 = null;
        try {
            lbp1 = networkBufferPool.createBufferPool(numBuffers / 2, numBuffers);

            // take all buffers (more than the minimum required)
            for (int i = 0; i < numBuffers; ++i) {
                Buffer buffer = lbp1.requestBuffer();
                buffers.add(buffer);
                assertThat(buffer).isNotNull();
            }

            // requestMemorySegments() below will wait for buffers
            // this will make sure that enough buffers are freed eventually for it to continue
            final OneShotLatch isRunning = new OneShotLatch();
            bufferRecycler =
                    new Thread(
                            () -> {
                                try {
                                    isRunning.trigger();
                                    Thread.sleep(100);
                                } catch (InterruptedException ignored) {
                                }

                                for (Buffer buffer : buffers) {
                                    buffer.recycleBuffer();
                                }
                            });
            bufferRecycler.start();

            // take more buffers than are freely available at the moment via requestMemorySegments()
            isRunning.await();
            memorySegments = networkBufferPool.requestUnpooledMemorySegments(numBuffers / 2);
            assertThat(memorySegments).doesNotContainNull();
        } finally {
            if (bufferRecycler != null) {
                bufferRecycler.join();
            }
            if (lbp1 != null) {
                lbp1.lazyDestroy();
            }
            networkBufferPool.recycleUnpooledMemorySegments(memorySegments);
            networkBufferPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)}, verifying it may be
     * aborted in case of a concurrent {@link NetworkBufferPool#destroy()} call.
     */
    @Test
    void testRequestMemorySegmentsInterruptable() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertThat(segment).isNotNull();

        final OneShotLatch isRunning = new OneShotLatch();
        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws IOException {
                        isRunning.trigger();
                        globalPool.requestUnpooledMemorySegments(10);
                    }
                };
        asyncRequest.start();

        // We want the destroy call inside the blocking part of the
        // globalPool.requestMemorySegments()
        // call above. We cannot guarantee this though but make it highly probable:
        isRunning.await();
        Thread.sleep(10);
        globalPool.destroy();

        segment.free();

        try {
            assertThatThrownBy(asyncRequest::sync)
                    .hasMessageContaining("destroyed")
                    .isInstanceOf(IllegalStateException.class);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)}, verifying it may be
     * aborted and remains in a defined state even if the waiting is interrupted.
     */
    @Test
    void testRequestMemorySegmentsInterruptable2() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertThat(segment).isNotNull();

        final OneShotLatch isRunning = new OneShotLatch();
        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws IOException {
                        isRunning.trigger();
                        globalPool.requestUnpooledMemorySegments(10);
                    }
                };
        asyncRequest.start();

        // We want the destroy call inside the blocking part of the
        // globalPool.requestMemorySegments()
        // call above. We cannot guarantee this though but make it highly probable:
        isRunning.await();
        Thread.sleep(10);
        asyncRequest.interrupt();

        globalPool.recyclePooledMemorySegment(segment);

        try {
            asyncRequest.sync();
        } catch (IOException e) {
            assertThat(e).hasCauseInstanceOf(InterruptedException.class);

            // test indirectly for NetworkBufferPool#numTotalRequiredBuffers being correct:
            // -> creating a new buffer pool should not fail
            globalPool.createBufferPool(10, 10);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} and verifies it will end
     * exceptionally when failing to acquire all the segments in the specific timeout.
     */
    @Test
    void testRequestMemorySegmentsTimeout() throws Exception {
        final int numBuffers = 10;
        final int numberOfSegmentsToRequest = 2;
        final Duration requestSegmentsTimeout = Duration.ofMillis(50L);

        NetworkBufferPool globalPool =
                new NetworkBufferPool(numBuffers, 128, requestSegmentsTimeout);

        BufferPool localBufferPool = globalPool.createBufferPool(1, numBuffers);
        for (int i = 0; i < numBuffers; ++i) {
            localBufferPool.requestBuffer();
        }

        assertThat(globalPool.getNumberOfAvailableMemorySegments()).isZero();

        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
                    }
                };

        asyncRequest.start();

        try {
            assertThatThrownBy(asyncRequest::sync)
                    .hasMessageContaining("Timeout")
                    .isInstanceOf(IOException.class);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is
     * correctly maintained after memory segments are requested by {@link
     * NetworkBufferPool#requestPooledMemorySegment()} and recycled by {@link
     * NetworkBufferPool#recyclePooledMemorySegment(MemorySegment)}.
     */
    @Test
    void testIsAvailableOrNotAfterRequestAndRecycleSingleSegment() {
        final int numBuffers = 2;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertThat(globalPool.getAvailableFuture()).isDone();

            // request the first segment
            final MemorySegment segment1 = checkNotNull(globalPool.requestPooledMemorySegment());
            assertThat(globalPool.getAvailableFuture()).isDone();

            // request the second segment
            final MemorySegment segment2 = checkNotNull(globalPool.requestPooledMemorySegment());
            assertThat(globalPool.getAvailableFuture()).isNotDone();

            final CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();

            // recycle the first segment
            globalPool.recyclePooledMemorySegment(segment1);
            assertThat(availableFuture).isDone();
            assertThat(globalPool.getAvailableFuture()).isDone();

            // recycle the second segment
            globalPool.recyclePooledMemorySegment(segment2);
            assertThat(globalPool.getAvailableFuture()).isDone();

        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#isAvailable()}, verifying that the buffer availability is
     * correctly maintained after memory segments are requested by {@link
     * NetworkBufferPool#requestUnpooledMemorySegments(int)} and recycled by {@link
     * NetworkBufferPool#recycleUnpooledMemorySegments(Collection)}.
     */
    @Test
    void testIsAvailableOrNotAfterRequestAndRecycleMultiSegments() throws Exception {
        final int numberOfSegmentsToRequest = 5;
        final int numBuffers = 2 * numberOfSegmentsToRequest;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertThat(globalPool.getAvailableFuture()).isDone();

            // request 5 segments
            List<MemorySegment> segments1 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture()).isDone();
            assertThat(segments1).hasSize(numberOfSegmentsToRequest);

            // request another 5 segments
            List<MemorySegment> segments2 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture()).isNotDone();
            assertThat(segments2).hasSize(numberOfSegmentsToRequest);

            // recycle 5 segments
            CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
            globalPool.recycleUnpooledMemorySegments(segments1);
            assertThat(availableFuture).isDone();

            // request another 5 segments
            final List<MemorySegment> segments3 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertThat(globalPool.getAvailableFuture()).isNotDone();
            assertThat(segments3).hasSize(numberOfSegmentsToRequest);

            // recycle another 5 segments
            globalPool.recycleUnpooledMemorySegments(segments2);
            assertThat(globalPool.getAvailableFuture()).isDone();

            // recycle the last 5 segments
            globalPool.recycleUnpooledMemorySegments(segments3);
            assertThat(globalPool.getAvailableFuture()).isDone();

        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests that blocking request of multi local buffer pools can be fulfilled by recycled segments
     * to the global network buffer pool.
     */
    @Test
    void testBlockingRequestFromMultiLocalBufferPool() throws IOException, InterruptedException {
        final int localPoolRequiredSize = 5;
        final int localPoolMaxSize = 10;
        final int numLocalBufferPool = 2;
        final int numberOfSegmentsToRequest = 10;
        final int numBuffers = numLocalBufferPool * localPoolMaxSize;

        final ExecutorService executorService = Executors.newFixedThreadPool(numLocalBufferPool);
        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        final List<BufferPool> localBufferPools = new ArrayList<>(numLocalBufferPool);

        try {
            // create local buffer pools
            for (int i = 0; i < numLocalBufferPool; ++i) {
                final BufferPool localPool =
                        globalPool.createBufferPool(localPoolRequiredSize, localPoolMaxSize);
                localBufferPools.add(localPool);
                assertThat(localPool.getAvailableFuture()).isDone();
            }

            // request some segments from the global pool in two different ways
            final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest - 1);
            for (int i = 0; i < numberOfSegmentsToRequest - 1; ++i) {
                segments.add(globalPool.requestPooledMemorySegment());
            }
            final List<MemorySegment> exclusiveSegments =
                    globalPool.requestUnpooledMemorySegments(
                            globalPool.getNumberOfAvailableMemorySegments() - 1);
            assertThat(globalPool.getAvailableFuture()).isDone();
            for (final BufferPool localPool : localBufferPools) {
                assertThat(localPool.getAvailableFuture()).isDone();
            }

            // blocking request buffers form local buffer pools
            final CountDownLatch latch = new CountDownLatch(numLocalBufferPool);
            final BlockingQueue<BufferBuilder> segmentsRequested =
                    new ArrayBlockingQueue<>(numBuffers);
            final AtomicReference<Throwable> cause = new AtomicReference<>();
            for (final BufferPool localPool : localBufferPools) {
                executorService.submit(
                        () -> {
                            try {
                                for (int num = localPoolMaxSize; num > 0; --num) {
                                    segmentsRequested.add(localPool.requestBufferBuilderBlocking());
                                }
                            } catch (Exception e) {
                                cause.set(e);
                            } finally {
                                latch.countDown();
                            }
                        });
            }

            // wait until all available buffers are requested
            while (segmentsRequested.size() + segments.size() + exclusiveSegments.size()
                    < numBuffers) {
                Thread.sleep(10);
                assertThat(cause.get()).isNull();
            }

            final CompletableFuture<?> globalPoolAvailableFuture = globalPool.getAvailableFuture();
            assertThat(globalPoolAvailableFuture).isNotDone();

            final List<CompletableFuture<?>> localPoolAvailableFutures =
                    new ArrayList<>(numLocalBufferPool);
            for (BufferPool localPool : localBufferPools) {
                CompletableFuture<?> localPoolAvailableFuture = localPool.getAvailableFuture();
                localPoolAvailableFutures.add(localPoolAvailableFuture);
                assertThat(localPoolAvailableFuture).isNotDone();
            }

            // recycle the previously requested segments
            for (MemorySegment segment : segments) {
                globalPool.recyclePooledMemorySegment(segment);
            }
            globalPool.recycleUnpooledMemorySegments(exclusiveSegments);

            assertThat(globalPoolAvailableFuture).isDone();
            for (CompletableFuture<?> localPoolAvailableFuture : localPoolAvailableFutures) {
                assertThat(localPoolAvailableFuture).isDone();
            }

            // wait until all blocking buffer requests finish
            latch.await();

            assertThat(cause.get()).isNull();
            assertThat(globalPool.getNumberOfAvailableMemorySegments()).isZero();
            assertThat(globalPool.getAvailableFuture()).isNotDone();
            for (BufferPool localPool : localBufferPools) {
                assertThat(localPool.getAvailableFuture()).isNotDone();
                assertThat(localPool.bestEffortGetNumOfUsedBuffers()).isEqualTo(localPoolMaxSize);
            }

            // recycle all the requested buffers
            for (BufferBuilder bufferBuilder : segmentsRequested) {
                bufferBuilder.close();
            }

        } finally {
            for (BufferPool bufferPool : localBufferPools) {
                bufferPool.lazyDestroy();
            }
            executorService.shutdown();
            globalPool.destroy();
        }
    }
}

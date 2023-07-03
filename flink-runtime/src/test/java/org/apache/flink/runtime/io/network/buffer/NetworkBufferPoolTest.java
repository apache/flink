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
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link NetworkBufferPool}. */
public class NetworkBufferPoolTest extends TestLogger {

    @Test
    public void testCreatePoolAfterDestroy() {
        try {
            final int bufferSize = 128;
            final int numBuffers = 10;

            NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);
            assertThat(globalPool.getNumberOfRegisteredBufferPools(), is(0));

            globalPool.destroy();

            assertTrue(globalPool.isDestroyed());

            try {
                globalPool.createBufferPool(2, 2);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }

            try {
                globalPool.createBufferPool(2, 10);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }

            try {
                globalPool.createBufferPool(2, Integer.MAX_VALUE);
                fail("Should throw an IllegalStateException");
            } catch (IllegalStateException e) {
                // yippie!
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMemoryUsageInTheContextOfMemoryPoolCreation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        assertThat(globalPool.getTotalNumberOfMemorySegments(), is(numBuffers));
        assertThat(globalPool.getNumberOfAvailableMemorySegments(), is(numBuffers));
        assertThat(globalPool.getNumberOfUsedMemorySegments(), is(0));

        assertThat(globalPool.getTotalMemory(), is((long) numBuffers * bufferSize));
        assertThat(globalPool.getAvailableMemory(), is((long) numBuffers * bufferSize));
        assertThat(globalPool.getUsedMemory(), is(0L));
    }

    @Test
    public void testMemoryUsageInTheContextOfMemorySegmentAllocation() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertThat(segment, is(notNullValue()));

        assertThat(globalPool.getTotalNumberOfMemorySegments(), is(numBuffers));
        assertThat(globalPool.getNumberOfAvailableMemorySegments(), is(numBuffers - 1));
        assertThat(globalPool.getNumberOfUsedMemorySegments(), is(1));

        assertThat(globalPool.getTotalMemory(), is((long) numBuffers * bufferSize));
        assertThat(globalPool.getAvailableMemory(), is((long) (numBuffers - 1) * bufferSize));
        assertThat(globalPool.getUsedMemory(), is((long) bufferSize));
    }

    @Test
    public void testMemoryUsageInTheContextOfMemoryPoolDestruction() {
        final int bufferSize = 128;
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, bufferSize);

        globalPool.destroy();

        assertThat(globalPool.getTotalNumberOfMemorySegments(), is(0));
        assertThat(globalPool.getNumberOfAvailableMemorySegments(), is(0));
        assertThat(globalPool.getNumberOfUsedMemorySegments(), is(0));

        assertThat(globalPool.getTotalMemory(), is(0L));
        assertThat(globalPool.getAvailableMemory(), is(0L));
        assertThat(globalPool.getUsedMemory(), is(0L));
    }

    @Test
    public void testDestroyAll() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);

        BufferPool fixedPool = globalPool.createBufferPool(2, 2);
        BufferPool boundedPool = globalPool.createBufferPool(1, 1);
        BufferPool nonFixedPool = globalPool.createBufferPool(5, Integer.MAX_VALUE);

        assertEquals(2, fixedPool.getNumberOfRequiredMemorySegments());
        assertEquals(1, boundedPool.getNumberOfRequiredMemorySegments());
        assertEquals(5, nonFixedPool.getNumberOfRequiredMemorySegments());

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
                    assertNotNull(buffer.getMemorySegment());
                    buffers.add(buffer);
                    continue collectBuffers;
                }
            }
        }

        assertEquals(globalPool.getTotalNumberOfMemorySegments(), buffers.size());

        assertNull(fixedPool.requestBuffer());
        assertNull(boundedPool.requestBuffer());
        assertNull(nonFixedPool.requestBuffer());

        // destroy all allocated ones
        globalPool.destroyAllBufferPools();

        // check the destroyed status
        assertFalse(globalPool.isDestroyed());
        assertTrue(fixedPool.isDestroyed());
        assertTrue(boundedPool.isDestroyed());
        assertTrue(nonFixedPool.isDestroyed());

        assertEquals(0, globalPool.getNumberOfRegisteredBufferPools());

        // buffers are not yet recycled
        assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

        // the recycled buffers should go to the global pool
        for (Buffer b : buffers) {
            b.recycleBuffer();
        }
        assertEquals(
                globalPool.getTotalNumberOfMemorySegments(),
                globalPool.getNumberOfAvailableMemorySegments());

        // can request no more buffers
        try {
            fixedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // yippie!
        }

        try {
            boundedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // that's the way we like it, aha, aha
        }

        try {
            nonFixedPool.requestBuffer();
            fail("Should fail with an CancelTaskException");
        } catch (CancelTaskException e) {
            // stayin' alive
        }

        // can create a new pool now
        assertNotNull(globalPool.createBufferPool(10, Integer.MAX_VALUE));
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the {@link
     * NetworkBufferPool} currently containing the number of required free segments.
     */
    @Test
    public void testRequestMemorySegmentsLessThanTotalBuffers() throws IOException {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        List<MemorySegment> memorySegments = Collections.emptyList();
        try {
            memorySegments = globalPool.requestUnpooledMemorySegments(numBuffers / 2);
            assertEquals(memorySegments.size(), numBuffers / 2);

            globalPool.recycleUnpooledMemorySegments(memorySegments);
            memorySegments.clear();
            assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
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
    public void testRequestMemorySegmentsMoreThanTotalBuffers() {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            globalPool.requestUnpooledMemorySegments(numBuffers + 1);
            fail("Should throw an IOException");
        } catch (IOException e) {
            assertEquals(globalPool.getNumberOfAvailableMemorySegments(), numBuffers);
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the total number of
     * allocated buffers for several requests exceeding the capacity of {@link NetworkBufferPool}.
     */
    @Test
    public void testInsufficientNumberOfBuffers() throws Exception {
        final int numberOfSegmentsToRequest = 5;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numberOfSegmentsToRequest, 128);

        try {
            // the global pool should be in available state initially
            assertTrue(globalPool.getAvailableFuture().isDone());

            // request 5 segments
            List<MemorySegment> segments1 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertFalse(globalPool.getAvailableFuture().isDone());
            assertEquals(numberOfSegmentsToRequest, segments1.size());

            // request only 1 segment
            IOException ioException =
                    assertThrows(
                            IOException.class, () -> globalPool.requestUnpooledMemorySegments(1));

            assertTrue(ioException.getMessage().contains("Insufficient number of network buffers"));

            // recycle 5 segments
            CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
            globalPool.recycleUnpooledMemorySegments(segments1);
            assertTrue(availableFuture.isDone());

            List<MemorySegment> segments2 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertFalse(globalPool.getAvailableFuture().isDone());
            assertEquals(numberOfSegmentsToRequest, segments2.size());
        } finally {
            globalPool.destroy();
        }
    }

    @Test
    public void testEmptyPoolSegmentsUsage() throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            NetworkBufferPool globalPool = new NetworkBufferPool(0, 128);
            closeableRegistry.registerCloseable(globalPool::destroy);
            assertEquals(0, globalPool.getEstimatedRequestedSegmentsUsage());
        }
    }

    @Test
    public void testSegmentsUsage() throws IOException {
        try (CloseableRegistry closeableRegistry = new CloseableRegistry()) {
            NetworkBufferPool globalPool = new NetworkBufferPool(50, 128);
            closeableRegistry.registerCloseable(globalPool::destroy);

            BufferPool bufferPool1 = globalPool.createBufferPool(10, 20);

            assertEquals(20, globalPool.getEstimatedNumberOfRequestedMemorySegments());
            assertEquals(40, globalPool.getEstimatedRequestedSegmentsUsage());
            assertThat(globalPool.getUsageWarning(), equalTo(Optional.empty()));

            closeableRegistry.registerCloseable(
                    (globalPool.createBufferPool(5, Integer.MAX_VALUE))::lazyDestroy);

            assertEquals(30, globalPool.getEstimatedNumberOfRequestedMemorySegments());
            assertEquals(60, globalPool.getEstimatedRequestedSegmentsUsage());
            assertThat(globalPool.getUsageWarning(), equalTo(Optional.empty()));

            closeableRegistry.registerCloseable((globalPool.createBufferPool(10, 30))::lazyDestroy);

            assertEquals(60, globalPool.getEstimatedNumberOfRequestedMemorySegments());
            assertEquals(120, globalPool.getEstimatedRequestedSegmentsUsage());
            assertThat(
                    globalPool.getUsageWarning(),
                    equalTo(
                            Optional.of(
                                    "Memory usage [120%] is too high to satisfy all of the requests. "
                                            + "This can severely impact network throughput. "
                                            + "Please consider increasing available network memory, "
                                            + "or decreasing configured size of network buffer pools. ("
                                            + "totalMemory=6.250kb (6400 bytes), "
                                            + "requestedMemory=7.500kb (7680 bytes), "
                                            + "missingMemory=1.250kb (1280 bytes))")));
            assertThat(globalPool.getUsageWarning(), equalTo(Optional.empty()));

            BufferPool bufferPool2 = globalPool.createBufferPool(10, 20);

            assertEquals(80, globalPool.getEstimatedNumberOfRequestedMemorySegments());
            assertEquals(160, globalPool.getEstimatedRequestedSegmentsUsage());
            assertThat(
                    globalPool.getUsageWarning(),
                    equalTo(
                            Optional.of(
                                    "Memory usage [160%] is too high to satisfy all of the requests. "
                                            + "This can severely impact network throughput. "
                                            + "Please consider increasing available network memory, "
                                            + "or decreasing configured size of network buffer pools. ("
                                            + "totalMemory=6.250kb (6400 bytes), "
                                            + "requestedMemory=10.000kb (10240 bytes), "
                                            + "missingMemory=3.750kb (3840 bytes))")));

            bufferPool2.lazyDestroy();
            bufferPool1.lazyDestroy();

            assertEquals(40, globalPool.getEstimatedNumberOfRequestedMemorySegments());
            assertEquals(40 * 128, globalPool.getEstimatedRequestedMemory());
            assertEquals(80, globalPool.getEstimatedRequestedSegmentsUsage());
            assertThat(
                    globalPool.getUsageWarning(),
                    equalTo(Optional.of("Memory usage [80%] went back to normal")));
            assertThat(globalPool.getUsageWarning(), equalTo(Optional.empty()));
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the invalid argument
     * to cause exception.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testRequestMemorySegmentsWithInvalidArgument() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        // the number of requested buffers should be non-negative
        globalPool.requestUnpooledMemorySegments(-1);
        globalPool.destroy();
        fail("Should throw an IllegalArgumentException");
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)} with the {@link
     * NetworkBufferPool} currently not containing the number of required free segments (currently
     * occupied by a buffer pool).
     */
    @Test
    public void testRequestMemorySegmentsWithBuffersTaken()
            throws IOException, InterruptedException {
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
                assertNotNull(buffer);
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
            assertThat(memorySegments, not(hasItem(nullValue())));
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
    public void testRequestMemorySegmentsInterruptable() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertNotNull(segment);

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
            Exception ex = assertThrows(IllegalStateException.class, asyncRequest::sync);
            assertTrue(ex.getMessage().contains("destroyed"));
        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests {@link NetworkBufferPool#requestUnpooledMemorySegments(int)}, verifying it may be
     * aborted and remains in a defined state even if the waiting is interrupted.
     */
    @Test
    public void testRequestMemorySegmentsInterruptable2() throws Exception {
        final int numBuffers = 10;

        NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);
        MemorySegment segment = globalPool.requestPooledMemorySegment();
        assertNotNull(segment);

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
            assertThat(e, hasProperty("cause", instanceOf(InterruptedException.class)));

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
    public void testRequestMemorySegmentsTimeout() throws Exception {
        final int numBuffers = 10;
        final int numberOfSegmentsToRequest = 2;
        final Duration requestSegmentsTimeout = Duration.ofMillis(50L);

        NetworkBufferPool globalPool =
                new NetworkBufferPool(numBuffers, 128, requestSegmentsTimeout);

        BufferPool localBufferPool = globalPool.createBufferPool(1, numBuffers);
        for (int i = 0; i < numBuffers; ++i) {
            localBufferPool.requestBuffer();
        }

        assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());

        CheckedThread asyncRequest =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
                    }
                };

        asyncRequest.start();

        try {
            Exception ex = assertThrows(IOException.class, asyncRequest::sync);
            assertTrue(ex.getMessage().contains("Timeout"));
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
    public void testIsAvailableOrNotAfterRequestAndRecycleSingleSegment() {
        final int numBuffers = 2;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertTrue(globalPool.getAvailableFuture().isDone());

            // request the first segment
            final MemorySegment segment1 = checkNotNull(globalPool.requestPooledMemorySegment());
            assertTrue(globalPool.getAvailableFuture().isDone());

            // request the second segment
            final MemorySegment segment2 = checkNotNull(globalPool.requestPooledMemorySegment());
            assertFalse(globalPool.getAvailableFuture().isDone());

            final CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();

            // recycle the first segment
            globalPool.recyclePooledMemorySegment(segment1);
            assertTrue(availableFuture.isDone());
            assertTrue(globalPool.getAvailableFuture().isDone());

            // recycle the second segment
            globalPool.recyclePooledMemorySegment(segment2);
            assertTrue(globalPool.getAvailableFuture().isDone());

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
    public void testIsAvailableOrNotAfterRequestAndRecycleMultiSegments() throws Exception {
        final int numberOfSegmentsToRequest = 5;
        final int numBuffers = 2 * numberOfSegmentsToRequest;

        final NetworkBufferPool globalPool = new NetworkBufferPool(numBuffers, 128);

        try {
            // the global pool should be in available state initially
            assertTrue(globalPool.getAvailableFuture().isDone());

            // request 5 segments
            List<MemorySegment> segments1 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertTrue(globalPool.getAvailableFuture().isDone());
            assertEquals(numberOfSegmentsToRequest, segments1.size());

            // request another 5 segments
            List<MemorySegment> segments2 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertFalse(globalPool.getAvailableFuture().isDone());
            assertEquals(numberOfSegmentsToRequest, segments2.size());

            // recycle 5 segments
            CompletableFuture<?> availableFuture = globalPool.getAvailableFuture();
            globalPool.recycleUnpooledMemorySegments(segments1);
            assertTrue(availableFuture.isDone());

            // request another 5 segments
            final List<MemorySegment> segments3 =
                    globalPool.requestUnpooledMemorySegments(numberOfSegmentsToRequest);
            assertFalse(globalPool.getAvailableFuture().isDone());
            assertEquals(numberOfSegmentsToRequest, segments3.size());

            // recycle another 5 segments
            globalPool.recycleUnpooledMemorySegments(segments2);
            assertTrue(globalPool.getAvailableFuture().isDone());

            // recycle the last 5 segments
            globalPool.recycleUnpooledMemorySegments(segments3);
            assertTrue(globalPool.getAvailableFuture().isDone());

        } finally {
            globalPool.destroy();
        }
    }

    /**
     * Tests that blocking request of multi local buffer pools can be fulfilled by recycled segments
     * to the global network buffer pool.
     */
    @Test
    public void testBlockingRequestFromMultiLocalBufferPool()
            throws IOException, InterruptedException {
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
                assertTrue(localPool.getAvailableFuture().isDone());
            }

            // request some segments from the global pool in two different ways
            final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest - 1);
            for (int i = 0; i < numberOfSegmentsToRequest - 1; ++i) {
                segments.add(globalPool.requestPooledMemorySegment());
            }
            final List<MemorySegment> exclusiveSegments =
                    globalPool.requestUnpooledMemorySegments(
                            globalPool.getNumberOfAvailableMemorySegments() - 1);
            assertTrue(globalPool.getAvailableFuture().isDone());
            for (final BufferPool localPool : localBufferPools) {
                assertTrue(localPool.getAvailableFuture().isDone());
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
                assertNull(cause.get());
            }

            final CompletableFuture<?> globalPoolAvailableFuture = globalPool.getAvailableFuture();
            assertFalse(globalPoolAvailableFuture.isDone());

            final List<CompletableFuture<?>> localPoolAvailableFutures =
                    new ArrayList<>(numLocalBufferPool);
            for (BufferPool localPool : localBufferPools) {
                CompletableFuture<?> localPoolAvailableFuture = localPool.getAvailableFuture();
                localPoolAvailableFutures.add(localPoolAvailableFuture);
                assertFalse(localPoolAvailableFuture.isDone());
            }

            // recycle the previously requested segments
            for (MemorySegment segment : segments) {
                globalPool.recyclePooledMemorySegment(segment);
            }
            globalPool.recycleUnpooledMemorySegments(exclusiveSegments);

            assertTrue(globalPoolAvailableFuture.isDone());
            for (CompletableFuture<?> localPoolAvailableFuture : localPoolAvailableFutures) {
                assertTrue(localPoolAvailableFuture.isDone());
            }

            // wait until all blocking buffer requests finish
            latch.await();

            assertNull(cause.get());
            assertEquals(0, globalPool.getNumberOfAvailableMemorySegments());
            assertFalse(globalPool.getAvailableFuture().isDone());
            for (BufferPool localPool : localBufferPools) {
                assertFalse(localPool.getAvailableFuture().isDone());
                assertEquals(localPoolMaxSize, localPool.bestEffortGetNumOfUsedBuffers());
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

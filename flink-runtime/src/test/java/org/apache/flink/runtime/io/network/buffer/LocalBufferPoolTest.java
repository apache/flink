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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the {@link LocalBufferPool}. */
public class LocalBufferPoolTest extends TestLogger {

    private static final int numBuffers = 1024;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool networkBufferPool;

    private BufferPool localBufferPool;

    private static final ExecutorService executor = Executors.newCachedThreadPool();

    @Rule public Timeout timeout = new Timeout(10, TimeUnit.SECONDS);

    @Before
    public void setupLocalBufferPool() throws Exception {
        networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
        localBufferPool = new LocalBufferPool(networkBufferPool, 1);

        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
    }

    @After
    public void destroyAndVerifyAllBuffersReturned() {
        if (!localBufferPool.isDestroyed()) {
            localBufferPool.lazyDestroy();
        }

        String msg = "Did not return all buffers to memory segment pool after test.";
        assertEquals(msg, numBuffers, networkBufferPool.getNumberOfAvailableMemorySegments());
        // no other local buffer pools used than the one above, but call just in case
        networkBufferPool.destroyAllBufferPools();
        networkBufferPool.destroy();
    }

    @AfterClass
    public static void shutdownExecutor() {
        executor.shutdownNow();
    }

    @Test
    public void testReserveSegments() throws Exception {
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(2, memorySegmentSize, Duration.ofSeconds(2));
        try {
            BufferPool bufferPool1 = networkBufferPool.createBufferPool(1, 2);
            assertThrows(IllegalArgumentException.class, () -> bufferPool1.reserveSegments(2));

            // request all buffers
            ArrayList<Buffer> buffers = new ArrayList<>(2);
            buffers.add(bufferPool1.requestBuffer());
            buffers.add(bufferPool1.requestBuffer());
            assertEquals(2, buffers.size());

            BufferPool bufferPool2 = networkBufferPool.createBufferPool(1, 10);
            assertThrows(IOException.class, () -> bufferPool2.reserveSegments(1));
            assertFalse(bufferPool2.isAvailable());

            buffers.forEach(Buffer::recycleBuffer);
            bufferPool1.lazyDestroy();
            bufferPool2.lazyDestroy();

            BufferPool bufferPool3 = networkBufferPool.createBufferPool(2, 10);
            assertEquals(1, bufferPool3.getNumberOfAvailableMemorySegments());
            bufferPool3.reserveSegments(2);
            assertEquals(2, bufferPool3.getNumberOfAvailableMemorySegments());

            bufferPool3.lazyDestroy();
            assertThrows(IllegalStateException.class, () -> bufferPool3.reserveSegments(1));
        } finally {
            networkBufferPool.destroy();
        }
    }

    @Test(timeout = 10000) // timeout can indicate a potential deadlock
    public void testReserveSegmentsAndCancel() throws Exception {
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
            assertEquals(0, globalPool.getNumberOfUsedMemorySegments());
            globalPool.destroy();
        }
    }

    @Test
    public void testRequestMoreThanAvailable() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        for (int i = 1; i <= numBuffers; i++) {
            Buffer buffer = localBufferPool.requestBuffer();

            assertEquals(Math.min(i + 1, numBuffers), getNumRequestedFromMemorySegmentPool());
            assertNotNull(buffer);

            requests.add(buffer);
        }

        {
            // One more...
            Buffer buffer = localBufferPool.requestBuffer();
            assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());
            assertNull(buffer);
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testRequestAfterDestroy() {
        localBufferPool.lazyDestroy();

        try {
            localBufferPool.requestBuffer();
            fail("Call should have failed with an IllegalStateException");
        } catch (IllegalStateException e) {
            // we expect exactly that
        }
    }

    @Test
    public void testSetNumAfterDestroyDoesNotProactivelyFetchSegments() {
        localBufferPool.setNumBuffers(2);
        assertEquals(2L, localBufferPool.getNumBuffers());
        assertEquals(1L, localBufferPool.getNumberOfAvailableMemorySegments());

        localBufferPool.lazyDestroy();
        localBufferPool.setNumBuffers(3);
        assertEquals(3L, localBufferPool.getNumBuffers());
        assertEquals(0L, localBufferPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    public void testRecycleAfterDestroy() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        for (int i = 0; i < numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        localBufferPool.lazyDestroy();

        // All buffers have been requested, but can not be returned yet.
        assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

        // Recycle should return buffers to memory segment pool
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testRecycleExcessBuffersAfterRecycling() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

        // Reduce the number of buffers in the local pool
        localBufferPool.setNumBuffers(numBuffers / 2);

        // Need to wait until we recycle the buffers
        assertEquals(numBuffers, getNumRequestedFromMemorySegmentPool());

        for (int i = 1; i < numBuffers / 2; i++) {
            requests.remove(0).recycleBuffer();
            assertEquals(numBuffers - i, getNumRequestedFromMemorySegmentPool());
        }

        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testRecycleExcessBuffersAfterChangingNumBuffers() {
        localBufferPool.setNumBuffers(numBuffers);

        List<Buffer> requests = new ArrayList<Buffer>(numBuffers);

        // Request all buffers
        for (int i = 1; i <= numBuffers; i++) {
            requests.add(localBufferPool.requestBuffer());
        }

        // Recycle all
        for (Buffer buffer : requests) {
            buffer.recycleBuffer();
        }

        assertEquals(numBuffers, localBufferPool.getNumberOfAvailableMemorySegments());

        localBufferPool.setNumBuffers(numBuffers / 2);

        assertEquals(numBuffers / 2, localBufferPool.getNumberOfAvailableMemorySegments());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSetLessThanRequiredNumBuffers() {
        localBufferPool.setNumBuffers(1);

        localBufferPool.setNumBuffers(0);
    }

    // ------------------------------------------------------------------------
    // Pending requests and integration with buffer futures
    // ------------------------------------------------------------------------

    @Test
    public void testPendingRequestWithListenersAfterRecycle() {
        CountBufferListener listener1 = new CountBufferListener();
        CountBufferListener listener2 = new CountBufferListener();

        Buffer available = localBufferPool.requestBuffer();

        assertNull(localBufferPool.requestBuffer());

        assertTrue(localBufferPool.addBufferListener(listener1));
        assertTrue(localBufferPool.addBufferListener(listener2));

        // Recycle the buffer to notify both of the above listeners once
        checkNotNull(available).recycleBuffer();

        assertEquals(1, listener1.getCount());
        assertEquals(1, listener1.getCount());

        assertFalse(localBufferPool.addBufferListener(listener1));
        assertFalse(localBufferPool.addBufferListener(listener2));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testCancelPendingRequestsAfterDestroy() {
        BufferListener listener = Mockito.mock(BufferListener.class);

        localBufferPool.setNumBuffers(1);

        Buffer available = localBufferPool.requestBuffer();
        Buffer unavailable = localBufferPool.requestBuffer();

        assertNull(unavailable);

        localBufferPool.addBufferListener(listener);

        localBufferPool.lazyDestroy();

        available.recycleBuffer();

        verify(listener, times(1)).notifyBufferDestroyed();
    }

    // ------------------------------------------------------------------------
    // Concurrent requests
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    public void testConcurrentRequestRecycle() throws ExecutionException, InterruptedException {
        int numConcurrentTasks = 128;
        int numBuffersToRequestPerTask = 1024;

        localBufferPool.setNumBuffers(numConcurrentTasks);

        Future<Boolean>[] taskResults = new Future[numConcurrentTasks];
        for (int i = 0; i < numConcurrentTasks; i++) {
            taskResults[i] =
                    executor.submit(
                            new BufferRequesterTask(localBufferPool, numBuffersToRequestPerTask));
        }

        for (int i = 0; i < numConcurrentTasks; i++) {
            assertTrue(taskResults[i].get());
        }
    }

    @Test
    public void testDestroyDuringBlockingRequest() throws Exception {
        // Config
        final int numberOfBuffers = 1;

        localBufferPool.setNumBuffers(numberOfBuffers);

        final CountDownLatch sync = new CountDownLatch(1);

        final Callable<List<Buffer>> requester =
                new Callable<List<Buffer>>() {

                    // Request all buffers in a blocking manner.
                    @Override
                    public List<Buffer> call() throws Exception {
                        final List<Buffer> requested = Lists.newArrayList();

                        // Request all available buffers
                        for (int i = 0; i < numberOfBuffers; i++) {
                            final Buffer buffer = checkNotNull(localBufferPool.requestBuffer());
                            requested.add(buffer);
                        }

                        // Notify that we've requested all buffers
                        sync.countDown();

                        // Try to request the next buffer (but pool should be destroyed either right
                        // before
                        // the request or more likely during the request).
                        try {
                            localBufferPool.requestBufferBuilderBlocking();
                            fail("Call should have failed with an IllegalStateException");
                        } catch (IllegalStateException e) {
                            // we expect exactly that
                        }

                        return requested;
                    }
                };

        Future<List<Buffer>> f = executor.submit(requester);

        sync.await();

        localBufferPool.lazyDestroy();

        // Increase the likelihood that the requested is currently in the request call
        Thread.sleep(50);

        // This should return immediately if everything works as expected
        List<Buffer> requestedBuffers = f.get(60, TimeUnit.SECONDS);

        for (Buffer buffer : requestedBuffers) {
            buffer.recycleBuffer();
        }
    }

    @Test
    public void testBoundedBuffer() throws Exception {
        localBufferPool.lazyDestroy();

        localBufferPool = new LocalBufferPool(networkBufferPool, 1, 2);
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertEquals(2, localBufferPool.getMaxNumberOfMemorySegments());

        Buffer buffer1, buffer2;

        // check min number of buffers:
        localBufferPool.setNumBuffers(1);
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer1 = localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNull(localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        buffer1.recycleBuffer();
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());

        // check max number of buffers:
        localBufferPool.setNumBuffers(2);
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer1 = localBufferPool.requestBuffer());
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer2 = localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNull(localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        buffer1.recycleBuffer();
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        buffer2.recycleBuffer();
        assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());

        // try to set too large buffer size:
        localBufferPool.setNumBuffers(3);
        assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer1 = localBufferPool.requestBuffer());
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer2 = localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNull(localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        buffer1.recycleBuffer();
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        buffer2.recycleBuffer();
        assertEquals(2, localBufferPool.getNumberOfAvailableMemorySegments());

        // decrease size again
        localBufferPool.setNumBuffers(1);
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNotNull(buffer1 = localBufferPool.requestBuffer());
        assertEquals(0, localBufferPool.getNumberOfAvailableMemorySegments());
        assertNull(localBufferPool.requestBuffer());
        buffer1.recycleBuffer();
        assertEquals(1, localBufferPool.getNumberOfAvailableMemorySegments());
    }

    /** Moves around availability of a {@link LocalBufferPool} with varying capacity. */
    @Test
    public void testMaxBuffersPerChannelAndAvailability() throws Exception {
        localBufferPool.lazyDestroy();
        localBufferPool = new LocalBufferPool(networkBufferPool, 1, Integer.MAX_VALUE, 3, 2);
        localBufferPool.setNumBuffers(10);

        assertTrue(localBufferPool.getAvailableFuture().isDone());

        // request one segment from subpartitin-0 and subpartition-1 respectively
        final BufferBuilder bufferBuilder01 = localBufferPool.requestBufferBuilderBlocking(0);
        final BufferBuilder bufferBuilder11 = localBufferPool.requestBufferBuilderBlocking(1);
        assertTrue(localBufferPool.getAvailableFuture().isDone());

        // request one segment from subpartition-0
        final BufferBuilder bufferBuilder02 = localBufferPool.requestBufferBuilderBlocking(0);
        assertFalse(localBufferPool.getAvailableFuture().isDone());

        assertNull(localBufferPool.requestBufferBuilder(0));
        final BufferBuilder bufferBuilder21 = localBufferPool.requestBufferBuilderBlocking(2);
        final BufferBuilder bufferBuilder22 = localBufferPool.requestBufferBuilderBlocking(2);
        assertFalse(localBufferPool.getAvailableFuture().isDone());

        // recycle segments
        bufferBuilder11.close();
        assertFalse(localBufferPool.getAvailableFuture().isDone());
        bufferBuilder21.close();
        assertFalse(localBufferPool.getAvailableFuture().isDone());
        bufferBuilder02.close();
        assertTrue(localBufferPool.getAvailableFuture().isDone());
        bufferBuilder01.close();
        assertTrue(localBufferPool.getAvailableFuture().isDone());
        bufferBuilder22.close();
        assertTrue(localBufferPool.getAvailableFuture().isDone());
    }

    @Test
    public void testIsAvailableOrNot() throws InterruptedException {

        // the local buffer pool should be in available state initially
        assertTrue(localBufferPool.isAvailable());

        // request one buffer
        try (BufferBuilder bufferBuilder =
                checkNotNull(localBufferPool.requestBufferBuilderBlocking())) {
            CompletableFuture<?> availableFuture = localBufferPool.getAvailableFuture();
            assertFalse(availableFuture.isDone());

            // set the pool size
            final int numLocalBuffers = 5;
            localBufferPool.setNumBuffers(numLocalBuffers);
            assertTrue(availableFuture.isDone());
            assertTrue(localBufferPool.isAvailable());

            // drain the local buffer pool
            final Deque<Buffer> buffers = new ArrayDeque<>(LocalBufferPoolTest.numBuffers);
            for (int i = 0; i < numLocalBuffers - 1; i++) {
                assertTrue(localBufferPool.isAvailable());
                buffers.add(checkNotNull(localBufferPool.requestBuffer()));
            }
            assertFalse(localBufferPool.isAvailable());

            buffers.pop().recycleBuffer();
            assertTrue(localBufferPool.isAvailable());

            // recycle the requested segments to global buffer pool
            for (final Buffer buffer : buffers) {
                buffer.recycleBuffer();
            }
            assertTrue(localBufferPool.isAvailable());

            // scale down (first buffer still taken), but there should still be one segment locally
            // available
            localBufferPool.setNumBuffers(2);
            assertTrue(localBufferPool.isAvailable());

            final Buffer buffer2 = checkNotNull(localBufferPool.requestBuffer());
            assertFalse(localBufferPool.isAvailable());

            buffer2.recycleBuffer();
            assertTrue(localBufferPool.isAvailable());

            // reset the pool size
            localBufferPool.setNumBuffers(1);
            assertFalse(localBufferPool.getAvailableFuture().isDone());
            // recycle the requested buffer
        }

        assertTrue(localBufferPool.isAvailable());
        assertTrue(localBufferPool.getAvailableFuture().isDone());
    }

    /** For FLINK-20547: https://issues.apache.org/jira/browse/FLINK-20547. */
    @Test
    public void testConsistentAvailability() throws Exception {
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

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

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

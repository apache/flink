/*
 * Copyright 2012 The Netty Project
 * Copy from netty 4.1.32.Final
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BatchShuffleReadBufferPool}. */
public class BatchShuffleReadBufferPoolTest {

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalTotalBytes() {
        createBufferPool(0, 1024);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalBufferSize() {
        createBufferPool(32 * 1024 * 1024, 0);
    }

    @Test
    public void testLargeTotalBytes() {
        BatchShuffleReadBufferPool bufferPool = createBufferPool(Long.MAX_VALUE, 1024);
        assertEquals(Integer.MAX_VALUE, bufferPool.getNumTotalBuffers());
        bufferPool.destroy();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTotalBytesSmallerThanBufferSize() {
        createBufferPool(4096, 32 * 1024);
    }

    @Test
    public void testBufferCalculation() {
        long totalBytes = 32 * 1024 * 1024;
        for (int bufferSize = 4 * 1024; bufferSize <= totalBytes; bufferSize += 1024) {
            BatchShuffleReadBufferPool bufferPool = createBufferPool(totalBytes, bufferSize);

            assertEquals(totalBytes, bufferPool.getTotalBytes());
            assertEquals(totalBytes / bufferSize, bufferPool.getNumTotalBuffers());
            assertTrue(bufferPool.getNumBuffersPerRequest() <= bufferPool.getNumTotalBuffers());
            assertTrue(bufferPool.getNumBuffersPerRequest() > 0);
        }
    }

    @Test
    public void testRequestBuffers() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = new ArrayList<>();

        try {
            buffers.addAll(bufferPool.requestBuffers());
            assertEquals(bufferPool.getNumBuffersPerRequest(), buffers.size());
        } finally {
            bufferPool.recycle(buffers);
            bufferPool.destroy();
        }
    }

    @Test
    public void testRecycle() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();

        bufferPool.recycle(buffers);
        assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
    }

    @Test
    public void testBufferFulfilledByRecycledBuffers() throws Exception {
        int numRequestThreads = 2;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        Map<Object, List<MemorySegment>> buffers = new ConcurrentHashMap<>();

        try {
            Object[] owners = new Object[] {new Object(), new Object(), new Object(), new Object()};
            for (int i = 0; i < 4; ++i) {
                buffers.put(owners[i], bufferPool.requestBuffers());
            }
            assertEquals(0, bufferPool.getAvailableBuffers());

            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        Object owner = new Object();
                                        List<MemorySegment> allocated = null;
                                        while (allocated == null || allocated.isEmpty()) {
                                            allocated = bufferPool.requestBuffers();
                                        }
                                        buffers.put(owner, allocated);
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            // recycle one by one
            for (MemorySegment segment : buffers.remove(owners[0])) {
                bufferPool.recycle(segment);
            }

            // bulk recycle
            bufferPool.recycle(buffers.remove(owners[1]));

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertNull(exception.get());
            assertEquals(0, bufferPool.getAvailableBuffers());
            assertEquals(4, buffers.size());
        } finally {
            for (Object owner : buffers.keySet()) {
                bufferPool.recycle(buffers.remove(owner));
            }
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    public void testMultipleThreadRequestAndRecycle() throws Exception {
        int numRequestThreads = 10;
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        try {
            Thread[] requestThreads = new Thread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new Thread(
                                () -> {
                                    try {
                                        for (int j = 0; j < 100; ++j) {
                                            List<MemorySegment> buffers =
                                                    bufferPool.requestBuffers();
                                            Thread.sleep(10);
                                            if (j % 2 == 0) {
                                                bufferPool.recycle(buffers);
                                            } else {
                                                for (MemorySegment segment : buffers) {
                                                    bufferPool.recycle(segment);
                                                }
                                            }
                                        }
                                    } catch (Throwable throwable) {
                                        exception.set(throwable);
                                    }
                                });
                requestThreads[i].start();
            }

            for (Thread requestThread : requestThreads) {
                requestThread.join();
            }

            assertNull(exception.get());
            assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());
        } finally {
            bufferPool.destroy();
        }
    }

    @Test
    public void testDestroy() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.recycle(buffers);

        assertFalse(bufferPool.isDestroyed());
        assertEquals(bufferPool.getNumTotalBuffers(), bufferPool.getAvailableBuffers());

        buffers = bufferPool.requestBuffers();
        assertEquals(
                bufferPool.getNumTotalBuffers() - buffers.size(), bufferPool.getAvailableBuffers());

        bufferPool.destroy();
        assertTrue(bufferPool.isDestroyed());
        assertEquals(0, bufferPool.getAvailableBuffers());
    }

    @Test(expected = IllegalStateException.class)
    public void testRequestBuffersAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        bufferPool.requestBuffers();

        bufferPool.destroy();
        bufferPool.requestBuffers();
    }

    @Test
    public void testRecycleAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.destroy();

        bufferPool.recycle(buffers);
        assertEquals(0, bufferPool.getAvailableBuffers());
    }

    @Test
    public void testDestroyWhileBlockingRequest() throws Exception {
        AtomicReference<Throwable> exception = new AtomicReference<>();
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        Thread requestThread =
                new Thread(
                        () -> {
                            try {
                                while (true) {
                                    bufferPool.requestBuffers();
                                }
                            } catch (Throwable throwable) {
                                exception.set(throwable);
                            }
                        });
        requestThread.start();

        Thread.sleep(1000);
        bufferPool.destroy();
        requestThread.join();

        assertTrue(exception.get() instanceof IllegalStateException);
    }

    private BatchShuffleReadBufferPool createBufferPool(long totalBytes, int bufferSize) {
        return new BatchShuffleReadBufferPool(totalBytes, bufferSize);
    }

    private BatchShuffleReadBufferPool createBufferPool() {
        return createBufferPool(32 * 1024 * 1024, 32 * 1024);
    }
}

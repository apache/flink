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
import org.apache.flink.core.testutils.CheckedThread;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BatchShuffleReadBufferPool}. */
@Timeout(value = 60)
class BatchShuffleReadBufferPoolTest {

    @Test
    void testIllegalTotalBytes() {
        assertThatThrownBy(() -> createBufferPool(0, 1024))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIllegalBufferSize() {
        assertThatThrownBy(() -> createBufferPool(32 * 1024 * 1024, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testLargeTotalBytes() {
        BatchShuffleReadBufferPool bufferPool = createBufferPool(Long.MAX_VALUE, 1024);
        assertThat(bufferPool.getNumTotalBuffers()).isEqualTo(Integer.MAX_VALUE);
        bufferPool.destroy();
    }

    @Test
    void testTotalBytesSmallerThanBufferSize() {
        assertThatThrownBy(() -> createBufferPool(4096, 32 * 1024))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBufferCalculation() {
        long totalBytes = 32 * 1024 * 1024;
        for (int bufferSize = 4 * 1024; bufferSize <= totalBytes; bufferSize += 1024) {
            BatchShuffleReadBufferPool bufferPool = createBufferPool(totalBytes, bufferSize);

            assertThat(bufferPool.getTotalBytes()).isEqualTo(totalBytes);
            assertThat(bufferPool.getNumTotalBuffers()).isEqualTo(totalBytes / bufferSize);
            assertThat(bufferPool.getNumBuffersPerRequest())
                    .isLessThanOrEqualTo(bufferPool.getNumTotalBuffers());
            assertThat(bufferPool.getNumBuffersPerRequest()).isGreaterThan(0);
        }
    }

    @Test
    void testRequestBuffers() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = new ArrayList<>();

        try {
            buffers.addAll(bufferPool.requestBuffers());
            assertThat(buffers).hasSize(bufferPool.getNumBuffersPerRequest());
        } finally {
            bufferPool.recycle(buffers);
            bufferPool.destroy();
        }
    }

    @Test
    void testRecycle() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();

        bufferPool.recycle(buffers);
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
    }

    @Test
    void testBufferOperationTimestampUpdated() throws Exception {
        BatchShuffleReadBufferPool bufferPool = new BatchShuffleReadBufferPool(1024, 1024);
        long oldTimestamp = bufferPool.getLastBufferOperationTimestamp();
        Thread.sleep(100);
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        assertThat(buffers).hasSize(1);
        // The timestamp is updated when requesting buffers successfully
        assertThat(bufferPool.getLastBufferOperationTimestamp()).isGreaterThan(oldTimestamp);

        oldTimestamp = bufferPool.getLastBufferOperationTimestamp();
        Thread.sleep(100);
        bufferPool.recycle(buffers);
        // The timestamp is updated when recycling buffers
        assertThat(bufferPool.getLastBufferOperationTimestamp()).isGreaterThan(oldTimestamp);

        buffers = bufferPool.requestBuffers();

        oldTimestamp = bufferPool.getLastBufferOperationTimestamp();
        Thread.sleep(100);
        assertThat(bufferPool.requestBuffers()).isEmpty();
        // The timestamp is not updated when requesting buffers is failed
        assertThat(bufferPool.getLastBufferOperationTimestamp()).isEqualTo(oldTimestamp);

        bufferPool.recycle(buffers);
        bufferPool.destroy();
    }

    @Test
    void testBufferFulfilledByRecycledBuffers() throws Exception {
        int numRequestThreads = 2;
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        Map<Object, List<MemorySegment>> buffers = new ConcurrentHashMap<>();

        try {
            Object[] owners = new Object[8];
            for (int i = 0; i < 8; ++i) {
                owners[i] = new Object();
                buffers.put(owners[i], bufferPool.requestBuffers());
            }
            assertThat(bufferPool.getAvailableBuffers()).isZero();

            CheckedThread[] requestThreads = new CheckedThread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new CheckedThread() {
                            @Override
                            public void go() throws Exception {
                                Object owner = new Object();
                                List<MemorySegment> allocated = null;
                                while (allocated == null || allocated.isEmpty()) {
                                    allocated = bufferPool.requestBuffers();
                                }
                                buffers.put(owner, allocated);
                            }
                        };
                requestThreads[i].start();
            }

            // recycle one by one
            for (MemorySegment segment : buffers.remove(owners[0])) {
                bufferPool.recycle(segment);
            }

            // bulk recycle
            bufferPool.recycle(buffers.remove(owners[1]));

            for (CheckedThread requestThread : requestThreads) {
                requestThread.sync();
            }

            assertThat(bufferPool.getAvailableBuffers()).isZero();
            assertThat(buffers).hasSize(8);
        } finally {
            for (Object owner : buffers.keySet()) {
                bufferPool.recycle(buffers.remove(owner));
            }
            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
            bufferPool.destroy();
        }
    }

    @Test
    void testMultipleThreadRequestAndRecycle() throws Exception {
        int numRequestThreads = 10;
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        try {
            CheckedThread[] requestThreads = new CheckedThread[numRequestThreads];
            for (int i = 0; i < numRequestThreads; ++i) {
                requestThreads[i] =
                        new CheckedThread() {
                            @Override
                            public void go() throws Exception {
                                for (int j = 0; j < 100; ++j) {
                                    List<MemorySegment> buffers = bufferPool.requestBuffers();
                                    Thread.sleep(10);
                                    if (j % 2 == 0) {
                                        bufferPool.recycle(buffers);
                                    } else {
                                        for (MemorySegment segment : buffers) {
                                            bufferPool.recycle(segment);
                                        }
                                    }
                                }
                            }
                        };
                requestThreads[i].start();
            }

            for (CheckedThread requestThread : requestThreads) {
                requestThread.sync();
            }

            assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());
        } finally {
            bufferPool.destroy();
        }
    }

    @Test
    void testDestroy() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.recycle(buffers);

        assertThat(bufferPool.isDestroyed()).isFalse();
        assertThat(bufferPool.getAvailableBuffers()).isEqualTo(bufferPool.getNumTotalBuffers());

        buffers = bufferPool.requestBuffers();
        assertThat(bufferPool.getAvailableBuffers())
                .isEqualTo(bufferPool.getNumTotalBuffers() - buffers.size());

        bufferPool.destroy();
        assertThat(bufferPool.isDestroyed()).isTrue();
        assertThat(bufferPool.getAvailableBuffers()).isZero();
    }

    @Test
    void testRequestBuffersAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        bufferPool.requestBuffers();

        bufferPool.destroy();
        assertThatThrownBy(bufferPool::requestBuffers).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRecycleAfterDestroyed() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();
        List<MemorySegment> buffers = bufferPool.requestBuffers();
        bufferPool.destroy();

        bufferPool.recycle(buffers);
        assertThat(bufferPool.getAvailableBuffers()).isZero();
    }

    @Test
    void testDestroyWhileBlockingRequest() throws Exception {
        BatchShuffleReadBufferPool bufferPool = createBufferPool();

        CheckedThread requestThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        while (true) {
                            bufferPool.requestBuffers();
                        }
                    }
                };
        requestThread.start();

        Thread.sleep(1000);
        bufferPool.destroy();

        assertThatThrownBy(requestThread::sync).isInstanceOf(IllegalStateException.class);
    }

    private BatchShuffleReadBufferPool createBufferPool(long totalBytes, int bufferSize) {
        return new BatchShuffleReadBufferPool(totalBytes, bufferSize);
    }

    private BatchShuffleReadBufferPool createBufferPool() {
        return createBufferPool(32 * 1024 * 1024, 32 * 1024);
    }
}

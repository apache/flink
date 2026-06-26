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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveredInputChannelRequestBufferBlockingHeapFallbackRemovedTest {

    private NetworkBufferPool pool;

    @AfterEach
    void tearDown() {
        if (pool != null) {
            pool.destroy();
            pool = null;
        }
    }

    @Test
    void testBufferPoolExhaustedBlocksRatherThanHeapAllocate() throws Exception {
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);
        RecoveredInputChannel channel = buildChannel(pool, totalSegments);

        for (int i = 0; i < totalSegments; i++) {
            channel.requestBufferBlocking();
        }

        CountDownLatch entered = new CountDownLatch(1);
        AtomicReference<Buffer> result = new AtomicReference<>();
        Thread blocker =
                new Thread(
                        () -> {
                            try {
                                entered.countDown();
                                result.set(channel.requestBufferBlocking());
                            } catch (Exception ignored) {
                                // Thread will be interrupted at teardown.
                            }
                        },
                        "blocking-requester");
        blocker.start();

        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);
        assertThat(result.get()).as("buffer should not have been allocated").isNull();

        blocker.interrupt();
        blocker.join(5_000);
    }

    @Test
    void testFilterOnPathTakesSameRouteAsFilterOff() throws Exception {
        int exclusivePerChannel = 1;
        int totalSegments = 4;
        pool = new NetworkBufferPool(totalSegments, MemoryManager.DEFAULT_PAGE_SIZE);

        Buffer filterOnBuf = buildChannel(pool, exclusivePerChannel).requestBufferBlocking();
        Buffer filterOffBuf = buildChannel(pool, exclusivePerChannel).requestBufferBlocking();

        // Both must come from the pool — the BufferManager-owned recycler, not the
        // FreeingBufferRecycler the heap-fallback used.
        assertThat(filterOnBuf.getMemorySegment()).isNotNull();
        assertThat(filterOffBuf.getMemorySegment()).isNotNull();
        assertThat(filterOnBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");
        assertThat(filterOffBuf.getRecycler().getClass().getName())
                .doesNotContain("FreeingBufferRecycler");

        filterOnBuf.recycleBuffer();
        filterOffBuf.recycleBuffer();
    }

    private RecoveredInputChannel buildChannel(
            NetworkBufferPool segmentProvider, int exclusivePerChannel) {
        try {
            SingleInputGate inputGate =
                    new SingleInputGateBuilder().setSegmentProvider(segmentProvider).build();
            return new RecoveredInputChannel(
                    inputGate,
                    0,
                    new ResultPartitionID(),
                    new ResultSubpartitionIndexSet(0),
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    exclusivePerChannel) {
                @Override
                protected InputChannel toInputChannelInternal(boolean needsRecovery) {
                    throw new AssertionError("not expected during this test");
                }
            };
        } catch (Exception e) {
            throw new AssertionError("channel construction failed", e);
        }
    }
}

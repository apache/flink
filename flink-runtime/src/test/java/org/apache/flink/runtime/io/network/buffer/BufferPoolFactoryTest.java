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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests for the creation of {@link LocalBufferPool} instances from the {@link NetworkBufferPool}
 * factory.
 */
public class BufferPoolFactoryTest {

    private static final int numBuffers = 1024;

    private static final int memorySegmentSize = 128;

    private NetworkBufferPool networkBufferPool;

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setupNetworkBufferPool() {
        networkBufferPool = new NetworkBufferPool(numBuffers, memorySegmentSize);
    }

    @After
    public void verifyAllBuffersReturned() {
        String msg = "Did not return all buffers to network buffer pool after test.";
        try {
            assertEquals(msg, numBuffers, networkBufferPool.getNumberOfAvailableMemorySegments());
        } finally {
            // in case buffers have actually been requested, we must release them again
            networkBufferPool.destroyAllBufferPools();
            networkBufferPool.destroy();
        }
    }

    /** Tests creating one buffer pool which requires more buffers than available. */
    @Test
    public void testRequireMoreThanPossible1() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Insufficient number of network buffers");

        networkBufferPool.createBufferPool(
                networkBufferPool.getTotalNumberOfMemorySegments() + 1, Integer.MAX_VALUE);
    }

    /** Tests creating two buffer pools which together require more buffers than available. */
    @Test
    public void testRequireMoreThanPossible2() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage("Insufficient number of network buffers");

        BufferPool bufferPool = null;
        try {
            bufferPool = networkBufferPool.createBufferPool(numBuffers / 2 + 1, numBuffers);
            networkBufferPool.createBufferPool(numBuffers / 2 + 1, numBuffers);
        } finally {
            if (bufferPool != null) {
                bufferPool.lazyDestroy();
            }
        }
    }

    /**
     * Tests creating two buffer pools which together require as many buffers as available but where
     * there are less buffers available to the {@link NetworkBufferPool} at the time of the second
     * {@link LocalBufferPool} creation.
     */
    @Test
    public void testOverprovisioned() throws IOException {
        // note: this is also the minimum number of buffers reserved for pool2
        int buffersToTakeFromPool1 = numBuffers / 2 + 1;
        // note: this is also the minimum number of buffers reserved for pool1
        int buffersToTakeFromPool2 = numBuffers - buffersToTakeFromPool1;

        List<Buffer> buffers = new ArrayList<>(numBuffers);
        BufferPool bufferPool1 = null, bufferPool2 = null;
        try {
            bufferPool1 = networkBufferPool.createBufferPool(buffersToTakeFromPool2, numBuffers);

            // take more buffers than the minimum required
            for (int i = 0; i < buffersToTakeFromPool1; ++i) {
                Buffer buffer = bufferPool1.requestBuffer();
                assertNotNull(buffer);
                buffers.add(buffer);
            }
            assertEquals(buffersToTakeFromPool1, bufferPool1.bestEffortGetNumOfUsedBuffers());
            assertEquals(numBuffers, bufferPool1.getNumBuffers());

            // create a second pool which requires more buffers than are available at the moment
            bufferPool2 = networkBufferPool.createBufferPool(buffersToTakeFromPool1, numBuffers);

            assertEquals(
                    bufferPool2.getNumberOfRequiredMemorySegments(), bufferPool2.getNumBuffers());
            assertEquals(
                    bufferPool1.getNumberOfRequiredMemorySegments(), bufferPool1.getNumBuffers());
            assertNull(bufferPool1.requestBuffer());

            // take all remaining buffers
            for (int i = 0; i < buffersToTakeFromPool2; ++i) {
                Buffer buffer = bufferPool2.requestBuffer();
                assertNotNull(buffer);
                buffers.add(buffer);
            }
            assertEquals(buffersToTakeFromPool2, bufferPool2.bestEffortGetNumOfUsedBuffers());

            // we should be able to get one more but this is currently given out to bufferPool1 and
            // taken by buffer1
            assertNull(bufferPool2.requestBuffer());

            // as soon as one excess buffer of bufferPool1 is recycled, it should be available for
            // bufferPool2
            buffers.remove(0).recycleBuffer();
            // recycle returns the excess buffer to the network buffer pool from where it's eagerly
            // fetched by pool 2
            assertEquals(0, networkBufferPool.getNumberOfAvailableMemorySegments());
            // verify the number of buffers taken from the pools
            assertEquals(
                    buffersToTakeFromPool1 - 1,
                    bufferPool1.bestEffortGetNumOfUsedBuffers()
                            + bufferPool1.getNumberOfAvailableMemorySegments());
            assertEquals(
                    buffersToTakeFromPool2 + 1,
                    bufferPool2.bestEffortGetNumOfUsedBuffers()
                            + bufferPool2.getNumberOfAvailableMemorySegments());
        } finally {
            for (Buffer buffer : buffers) {
                buffer.recycleBuffer();
            }
            if (bufferPool1 != null) {
                bufferPool1.lazyDestroy();
            }
            if (bufferPool2 != null) {
                bufferPool2.lazyDestroy();
            }
        }
    }

    @Test
    public void testBoundedPools() throws IOException {
        BufferPool bufferPool1 = networkBufferPool.createBufferPool(1, 1);
        assertEquals(1, bufferPool1.getNumBuffers());

        BufferPool bufferPool2 = networkBufferPool.createBufferPool(1, 2);
        assertEquals(2, bufferPool2.getNumBuffers());

        bufferPool1.lazyDestroy();
        bufferPool2.lazyDestroy();
    }

    @Test
    public void testSingleManagedPoolGetsAll() throws IOException {
        BufferPool bufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments(), bufferPool.getNumBuffers());

        bufferPool.lazyDestroy();
    }

    @Test
    public void testSingleManagedPoolGetsAllExceptFixedOnes() throws IOException {
        BufferPool fixedBufferPool = networkBufferPool.createBufferPool(24, 24);

        BufferPool flexibleBufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertEquals(24, fixedBufferPool.getNumBuffers());
        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments()
                        - fixedBufferPool.getNumBuffers(),
                flexibleBufferPool.getNumBuffers());

        fixedBufferPool.lazyDestroy();
        flexibleBufferPool.lazyDestroy();
    }

    @Test
    public void testUniformDistribution() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

        BufferPool second = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

        first.lazyDestroy();
        second.lazyDestroy();
    }

    /**
     * Tests that buffers, once given to an initial buffer pool, get re-distributed to a second one
     * in case both buffer pools request half of the available buffer count.
     */
    @Test
    public void testUniformDistributionAllBuffers() throws IOException {
        BufferPool first =
                networkBufferPool.createBufferPool(
                        networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

        BufferPool second =
                networkBufferPool.createBufferPool(
                        networkBufferPool.getTotalNumberOfMemorySegments() / 2, Integer.MAX_VALUE);
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded1() throws IOException {
        BufferPool first =
                networkBufferPool.createBufferPool(
                        1, networkBufferPool.getTotalNumberOfMemorySegments());
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

        BufferPool second =
                networkBufferPool.createBufferPool(
                        1, networkBufferPool.getTotalNumberOfMemorySegments());
        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());
        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded2() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, 10);
        assertEquals(10, first.getNumBuffers());

        BufferPool second = networkBufferPool.createBufferPool(1, 10);
        assertEquals(10, first.getNumBuffers());
        assertEquals(10, second.getNumBuffers());

        first.lazyDestroy();
        second.lazyDestroy();
    }

    @Test
    public void testUniformDistributionBounded3() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(3, 128);
        try {
            BufferPool first = globalPool.createBufferPool(1, 10);
            assertEquals(3, first.getNumBuffers());

            BufferPool second = globalPool.createBufferPool(1, 10);
            // the order of which buffer pool received 2 or 1 buffer is undefined
            assertEquals(3, first.getNumBuffers() + second.getNumBuffers());
            assertNotEquals(3, first.getNumBuffers());
            assertNotEquals(3, second.getNumBuffers());

            BufferPool third = globalPool.createBufferPool(1, 10);
            assertEquals(1, first.getNumBuffers());
            assertEquals(1, second.getNumBuffers());
            assertEquals(1, third.getNumBuffers());

            // similar to #verifyAllBuffersReturned()
            String msg = "Wrong number of available segments after creating buffer pools.";
            assertEquals(msg, 0, globalPool.getNumberOfAvailableMemorySegments());
        } finally {
            // in case buffers have actually been requested, we must release them again
            globalPool.destroyAllBufferPools();
            globalPool.destroy();
        }
    }

    /**
     * Tests the interaction of requesting memory segments and creating local buffer pool and
     * verifies the number of assigned buffers match after redistributing buffers because of newly
     * requested memory segments or new buffer pools created.
     */
    @Test
    public void testUniformDistributionBounded4() throws IOException {
        NetworkBufferPool globalPool = new NetworkBufferPool(10, 128);
        try {
            BufferPool first = globalPool.createBufferPool(1, 10);
            assertEquals(10, first.getNumBuffers());

            List<MemorySegment> segmentList1 = globalPool.requestMemorySegments(2);
            assertEquals(2, segmentList1.size());
            assertEquals(8, first.getNumBuffers());

            BufferPool second = globalPool.createBufferPool(1, 10);
            assertEquals(4, first.getNumBuffers());
            assertEquals(4, second.getNumBuffers());

            List<MemorySegment> segmentList2 = globalPool.requestMemorySegments(2);
            assertEquals(2, segmentList2.size());
            assertEquals(3, first.getNumBuffers());
            assertEquals(3, second.getNumBuffers());

            List<MemorySegment> segmentList3 = globalPool.requestMemorySegments(2);
            assertEquals(2, segmentList3.size());
            assertEquals(2, first.getNumBuffers());
            assertEquals(2, second.getNumBuffers());

            String msg =
                    "Wrong number of available segments after creating buffer pools and requesting segments.";
            assertEquals(msg, 2, globalPool.getNumberOfAvailableMemorySegments());

            globalPool.recycleMemorySegments(segmentList1);
            assertEquals(msg, 4, globalPool.getNumberOfAvailableMemorySegments());
            assertEquals(3, first.getNumBuffers());
            assertEquals(3, second.getNumBuffers());

            globalPool.recycleMemorySegments(segmentList2);
            assertEquals(msg, 6, globalPool.getNumberOfAvailableMemorySegments());
            assertEquals(4, first.getNumBuffers());
            assertEquals(4, second.getNumBuffers());

            globalPool.recycleMemorySegments(segmentList3);
            assertEquals(msg, 8, globalPool.getNumberOfAvailableMemorySegments());
            assertEquals(5, first.getNumBuffers());
            assertEquals(5, second.getNumBuffers());

            first.lazyDestroy();
            assertEquals(msg, 9, globalPool.getNumberOfAvailableMemorySegments());
            assertEquals(10, second.getNumBuffers());
        } finally {
            globalPool.destroyAllBufferPools();
            globalPool.destroy();
        }
    }

    @Test
    public void testBufferRedistributionMixed1() throws IOException {
        // try multiple times for various orders during redistribution
        for (int i = 0; i < 1_000; ++i) {
            BufferPool first = networkBufferPool.createBufferPool(1, 10);
            assertEquals(10, first.getNumBuffers());

            BufferPool second = networkBufferPool.createBufferPool(1, 10);
            assertEquals(10, first.getNumBuffers());
            assertEquals(10, second.getNumBuffers());

            BufferPool third = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
            // note: exact buffer distribution depends on the order during the redistribution
            for (BufferPool bp : new BufferPool[] {first, second, third}) {
                final int avail = numBuffers - 3;
                int size =
                        avail
                                        * Math.min(avail, bp.getMaxNumberOfMemorySegments() - 1)
                                        / (avail + 20 - 2)
                                + 1;
                assertThat(
                        "Wrong buffer pool size after redistribution",
                        bp.getNumBuffers(),
                        isOneOf(size, size + 1));
            }

            BufferPool fourth = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
            // note: exact buffer distribution depends on the order during the redistribution
            for (BufferPool bp : new BufferPool[] {first, second, third, fourth}) {
                final int avail = numBuffers - 4;
                int size =
                        avail
                                        * Math.min(avail, bp.getMaxNumberOfMemorySegments() - 1)
                                        / (2 * avail + 20 - 2)
                                + 1;
                assertThat(
                        "Wrong buffer pool size after redistribution",
                        bp.getNumBuffers(),
                        isOneOf(size, size + 1));
            }

            Stream.of(first, second, third, fourth).forEach(BufferPool::lazyDestroy);
            verifyAllBuffersReturned();
            setupNetworkBufferPool();
        }
    }

    @Test
    public void testAllDistributed() throws IOException {
        // try multiple times for various orders during redistribution
        for (int i = 0; i < 1_000; ++i) {
            Random random = new Random();

            List<BufferPool> pools = new ArrayList<>();

            int numPools = numBuffers / 32;
            long maxTotalUsed = 0;
            for (int j = 0; j < numPools; j++) {
                int numRequiredBuffers = random.nextInt(7) + 1;
                // make unbounded buffers more likely:
                int maxUsedBuffers =
                        random.nextBoolean()
                                ? Integer.MAX_VALUE
                                : Math.max(1, random.nextInt(10) + numRequiredBuffers);
                pools.add(networkBufferPool.createBufferPool(numRequiredBuffers, maxUsedBuffers));
                maxTotalUsed = Math.min(numBuffers, maxTotalUsed + maxUsedBuffers);

                // after every iteration, all buffers (up to maxTotalUsed) must be distributed
                int numDistributedBuffers = 0;
                for (BufferPool pool : pools) {
                    numDistributedBuffers += pool.getNumBuffers();
                }
                assertEquals(maxTotalUsed, numDistributedBuffers);
            }

            pools.forEach(BufferPool::lazyDestroy);
            verifyAllBuffersReturned();
            setupNetworkBufferPool();
        }
    }

    @Test
    public void testCreateDestroy() throws IOException {
        BufferPool first = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), first.getNumBuffers());

        BufferPool second = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);

        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments() / 2, first.getNumBuffers());

        assertEquals(
                networkBufferPool.getTotalNumberOfMemorySegments() / 2, second.getNumBuffers());

        first.lazyDestroy();

        assertEquals(networkBufferPool.getTotalNumberOfMemorySegments(), second.getNumBuffers());

        second.lazyDestroy();
    }
}

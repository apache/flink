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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link NettyBufferPool} wrapper. */
public class NettyBufferPoolTest {

    private final List<ByteBuf> needReleasing = new ArrayList<>();

    @After
    public void tearDown() {
        try {
            // Release all of the buffers.
            for (ByteBuf buf : needReleasing) {
                buf.release();
            }

            // Checks in a separate loop in case we have sliced buffers.
            for (ByteBuf buf : needReleasing) {
                assertEquals(0, buf.refCnt());
            }
        } finally {
            needReleasing.clear();
        }
    }

    @Test
    public void testNoHeapAllocations() throws Exception {
        final NettyBufferPool nettyBufferPool = new NettyBufferPool(1);

        // Buffers should prefer to be direct
        assertTrue(releaseLater(nettyBufferPool.buffer()).isDirect());
        assertTrue(releaseLater(nettyBufferPool.buffer(128)).isDirect());
        assertTrue(releaseLater(nettyBufferPool.buffer(128, 256)).isDirect());

        // IO buffers should prefer to be direct
        assertTrue(releaseLater(nettyBufferPool.ioBuffer()).isDirect());
        assertTrue(releaseLater(nettyBufferPool.ioBuffer(128)).isDirect());
        assertTrue(releaseLater(nettyBufferPool.ioBuffer(128, 256)).isDirect());

        // Currently we fakes the heap buffer allocation with direct buffers
        assertTrue(releaseLater(nettyBufferPool.heapBuffer()).isDirect());
        assertTrue(releaseLater(nettyBufferPool.heapBuffer(128)).isDirect());
        assertTrue(releaseLater(nettyBufferPool.heapBuffer(128, 256)).isDirect());

        // Composite buffers allocates the corresponding type of buffers when extending its capacity
        assertTrue(releaseLater(nettyBufferPool.compositeHeapBuffer()).capacity(1024).isDirect());
        assertTrue(releaseLater(nettyBufferPool.compositeHeapBuffer(10)).capacity(1024).isDirect());

        // Is direct buffer pooled!
        assertTrue(nettyBufferPool.isDirectBufferPooled());
    }

    @Test
    public void testAllocationsStatistics() throws Exception {
        NettyBufferPool nettyBufferPool = new NettyBufferPool(1);
        int chunkSize = nettyBufferPool.getChunkSize();

        {
            // Single large buffer allocates one chunk
            releaseLater(nettyBufferPool.directBuffer(chunkSize - 64));
            long allocated = nettyBufferPool.getNumberOfAllocatedBytes().get();
            assertEquals(chunkSize, allocated);
        }

        {
            // Allocate a little more (one more chunk required)
            releaseLater(nettyBufferPool.directBuffer(128));
            long allocated = nettyBufferPool.getNumberOfAllocatedBytes().get();
            assertEquals(2 * chunkSize, allocated);
        }
    }

    private ByteBuf releaseLater(ByteBuf buf) {
        needReleasing.add(buf);
        return buf;
    }
}

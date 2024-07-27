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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link NettyBufferPool} wrapper. */
class NettyBufferPoolTest {

    private final List<ByteBuf> needReleasing = new ArrayList<>();

    @AfterEach
    void tearDown() {
        try {
            // Release all of the buffers.
            for (ByteBuf buf : needReleasing) {
                buf.release();
            }

            // Checks in a separate loop in case we have sliced buffers.
            for (ByteBuf buf : needReleasing) {
                assertThat(buf.refCnt()).isZero();
            }
        } finally {
            needReleasing.clear();
        }
    }

    @Test
    void testNoHeapAllocations() {
        final NettyBufferPool nettyBufferPool = new NettyBufferPool(1);

        // Buffers should prefer to be direct
        assertThat(releaseLater(nettyBufferPool.buffer()).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.buffer(128)).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.buffer(128, 256)).isDirect()).isTrue();

        // IO buffers should prefer to be direct
        assertThat(releaseLater(nettyBufferPool.ioBuffer()).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.ioBuffer(128)).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.ioBuffer(128, 256)).isDirect()).isTrue();

        // Currently we fakes the heap buffer allocation with direct buffers
        assertThat(releaseLater(nettyBufferPool.heapBuffer()).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.heapBuffer(128)).isDirect()).isTrue();
        assertThat(releaseLater(nettyBufferPool.heapBuffer(128, 256)).isDirect()).isTrue();

        // Composite buffers allocates the corresponding type of buffers when extending its capacity
        assertThat(releaseLater(nettyBufferPool.compositeHeapBuffer()).capacity(1024).isDirect())
                .isTrue();
        assertThat(releaseLater(nettyBufferPool.compositeHeapBuffer(10)).capacity(1024).isDirect())
                .isTrue();

        // Is direct buffer pooled!
        assertThat(nettyBufferPool.isDirectBufferPooled()).isTrue();
    }

    @Test
    void testAllocationsStatistics() throws Exception {
        NettyBufferPool nettyBufferPool = new NettyBufferPool(1);
        int chunkSize = nettyBufferPool.getChunkSize();

        {
            // Single large buffer allocates one chunk
            releaseLater(nettyBufferPool.directBuffer(chunkSize - 64));
            assertThat(nettyBufferPool.getNumberOfAllocatedBytes()).hasValue((long) chunkSize);
        }

        {
            // Allocate a little more (one more chunk required)
            releaseLater(nettyBufferPool.directBuffer(128));
            assertThat(nettyBufferPool.getNumberOfAllocatedBytes()).hasValue(2L * chunkSize);
        }
    }

    private ByteBuf releaseLater(ByteBuf buf) {
        needReleasing.add(buf);
        return buf;
    }
}

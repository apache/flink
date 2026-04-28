/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.Nullable;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Test-only {@link BufferRequester} backed by two queues: one drawn by the non-blocking fast path
 * (P1 / P3) and one drawn by the blocking close() drain. Tests provide concrete queues and control
 * which buffers are available on each path.
 */
final class TestBufferPool implements BufferRequester {

    private final Queue<Buffer> writePool;
    private final Queue<Buffer> drainPool;

    /** Uses the same queue for both paths. */
    TestBufferPool(Queue<Buffer> pool) {
        this(pool, pool);
    }

    /** Uses separate queues for the non-blocking and blocking paths. */
    TestBufferPool(Queue<Buffer> writePool, Queue<Buffer> drainPool) {
        this.writePool = writePool;
        this.drainPool = drainPool;
    }

    /** Shorthand for a requester that only supplies buffers to the blocking drain path. */
    static TestBufferPool drainOnly(Queue<Buffer> drainPool) {
        return new TestBufferPool(new LinkedList<>(), drainPool);
    }

    /** Shorthand for a requester whose queues are both empty (no buffer available at all). */
    static TestBufferPool empty() {
        return new TestBufferPool(new LinkedList<>(), new LinkedList<>());
    }

    @Override
    @Nullable
    public Buffer requestBuffer(InputChannelInfo channelInfo) {
        return writePool.poll();
    }

    @Override
    @Nullable
    public Buffer requestBufferBlocking(InputChannelInfo channelInfo) {
        return drainPool.poll();
    }

    @Override
    public void releaseExclusiveBuffers() {
        // No-op: tests pre-supply queues; nothing to return to a global pool.
    }
}

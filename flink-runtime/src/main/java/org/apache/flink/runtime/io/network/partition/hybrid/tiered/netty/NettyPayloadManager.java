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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;

import javax.annotation.concurrent.GuardedBy;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

/** {@link NettyPayloadManager} is used to contain all netty payloads from a storage tier. */
public class NettyPayloadManager {

    private final Object lock = new Object();

    private final Queue<NettyPayload> queue = new LinkedList<>();

    /** Number of buffers whose {@link Buffer.DataType} is buffer in the queue. */
    @GuardedBy("lock")
    private int backlog = 0;

    public void add(NettyPayload nettyPayload) {
        synchronized (lock) {
            queue.add(nettyPayload);
            Optional<Buffer> buffer = nettyPayload.getBuffer();
            if (buffer.isPresent() && buffer.get().isBuffer()) {
                backlog++;
            }
        }
    }

    public NettyPayload peek() {
        synchronized (lock) {
            return queue.peek();
        }
    }

    public NettyPayload poll() {
        synchronized (lock) {
            NettyPayload nettyPayload = queue.poll();
            if (nettyPayload != null
                    && nettyPayload.getBuffer().isPresent()
                    && nettyPayload.getBuffer().get().isBuffer()) {
                backlog--;
            }
            return nettyPayload;
        }
    }

    public int getBacklog() {
        synchronized (lock) {
            return backlog;
        }
    }

    public int getSize() {
        synchronized (lock) {
            return queue.size();
        }
    }
}

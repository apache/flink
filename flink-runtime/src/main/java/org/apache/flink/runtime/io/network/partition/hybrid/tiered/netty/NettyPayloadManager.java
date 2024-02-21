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
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.GuardedBy;

import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link NettyPayloadManager} is used to contain all netty payloads from a storage tier. */
public class NettyPayloadManager {

    private final Object lock = new Object();

    private final Queue<NettyPayload> queue = new LinkedList<>();

    /**
     * The queue contains a collection of numbers. Each number represents the number of buffers that
     * belongs to consecutive segment ids.
     */
    @GuardedBy("lock")
    private final Deque<Integer> backlogs = new LinkedList<>();

    @GuardedBy("lock")
    private int lastSegmentId = -1;

    public void add(NettyPayload nettyPayload) {
        synchronized (lock) {
            queue.add(nettyPayload);
            int segmentId = nettyPayload.getSegmentId();
            if (segmentId != -1 && segmentId != lastSegmentId) {
                if (segmentId == 0 || segmentId != (lastSegmentId + 1)) {
                    addNewBacklog();
                }
                lastSegmentId = segmentId;
            }
            Optional<Buffer> buffer = nettyPayload.getBuffer();
            if (buffer.isPresent() && buffer.get().isBuffer()) {
                addBacklog();
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
                decreaseBacklog();
            }
            return nettyPayload;
        }
    }

    public int getBacklog() {
        synchronized (lock) {
            Integer backlog = backlogs.peekFirst();
            return backlog == null ? 0 : backlog;
        }
    }

    public int getSize() {
        synchronized (lock) {
            return queue.size();
        }
    }

    @GuardedBy("lock")
    private void addNewBacklog() {
        backlogs.addLast(0);
    }

    @GuardedBy("lock")
    private void addBacklog() {
        Integer backlog = backlogs.pollLast();
        if (backlog == null) {
            backlogs.addLast(1);
        } else {
            backlogs.addLast(backlog + 1);
        }
    }

    @GuardedBy("lock")
    private void decreaseBacklog() {
        int backlog = checkNotNull(backlogs.pollFirst());
        Preconditions.checkState(backlog > 0);
        if (backlog > 1) {
            backlogs.addFirst(backlog - 1);
        }
    }
}

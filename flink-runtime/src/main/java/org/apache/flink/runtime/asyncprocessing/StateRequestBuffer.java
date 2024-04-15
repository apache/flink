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

package org.apache.flink.runtime.asyncprocessing;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A buffer to hold state requests to execute state requests in batch, which can only be manipulated
 * within task thread.
 *
 * @param <K> the type of the key
 */
@NotThreadSafe
public class StateRequestBuffer<K> {
    /**
     * The state requests in this buffer could be executed when the buffer is full or configured
     * batch size is reached. All operations on this buffer must be invoked in task thread.
     */
    final LinkedList<StateRequest<K, ?, ?>> activeQueue;

    /**
     * The requests in that should wait until all preceding records with identical key finishing its
     * execution. After which the queueing requests will move into the active buffer. All operations
     * on this buffer must be invoked in task thread.
     */
    final Map<K, Deque<StateRequest<K, ?, ?>>> blockingQueue;

    /** The number of state requests in blocking queue. */
    int blockingQueueSize;

    public StateRequestBuffer() {
        this.activeQueue = new LinkedList<>();
        this.blockingQueue = new HashMap<>();
        this.blockingQueueSize = 0;
    }

    void enqueueToActive(StateRequest<K, ?, ?> request) {
        if (request.getRequestType() == StateRequestType.SYNC_POINT) {
            request.getFuture().complete(null);
        } else {
            activeQueue.add(request);
        }
    }

    void enqueueToBlocking(StateRequest<K, ?, ?> request) {
        blockingQueue
                .computeIfAbsent(request.getRecordContext().getKey(), k -> new LinkedList<>())
                .add(request);
        blockingQueueSize++;
    }

    /**
     * Try to pull one state request with specific key from blocking queue to active queue.
     *
     * @param key The key to release, the other records with this key is no longer blocking.
     * @return The first record context with the same key in blocking queue, null if no such record.
     */
    @Nullable
    RecordContext<K> tryActivateOneByKey(K key) {
        if (!blockingQueue.containsKey(key)) {
            return null;
        }

        StateRequest<K, ?, ?> stateRequest = blockingQueue.get(key).removeFirst();
        enqueueToActive(stateRequest);
        if (blockingQueue.get(key).isEmpty()) {
            blockingQueue.remove(key);
        }
        blockingQueueSize--;
        return stateRequest.getRecordContext();
    }

    /**
     * Get the number of state requests of blocking queue in constant-time.
     *
     * @return the number of state requests of blocking queue.
     */
    int blockingQueueSize() {
        return blockingQueueSize;
    }

    /**
     * Get the number of state requests of active queue in constant-time.
     *
     * @return the number of state requests of active queue.
     */
    int activeQueueSize() {
        return activeQueue.size();
    }

    /**
     * Try to pop state requests from active queue, if the size of active queue is less than N,
     * return all the requests in active queue.
     *
     * @param n the number of state requests to pop.
     * @return A list of state requests.
     */
    List<StateRequest<?, ?, ?>> popActive(int n) {
        final int count = Math.min(n, activeQueue.size());
        if (count <= 0) {
            return Collections.emptyList();
        }
        ArrayList<StateRequest<?, ?, ?>> ret = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ret.add(activeQueue.pop());
        }
        return ret;
    }
}

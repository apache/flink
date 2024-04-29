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

import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A buffer to hold state requests to execute state requests in batch, which can only be manipulated
 * within task thread.
 *
 * @param <K> the type of the key
 */
@NotThreadSafe
public class StateRequestBuffer<K> {

    /** All StateRequestBuffer in the same task manager share one ScheduledExecutorService. */
    private static final ScheduledThreadPoolExecutor DELAYER =
            new ScheduledThreadPoolExecutor(
                    1, new ExecutorThreadFactory("StateRequestBuffer-timeout-scheduler"));

    static {
        DELAYER.setRemoveOnCancelPolicy(true);
        DELAYER.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        DELAYER.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

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

    /** The timeout of {@link #activeQueue} triggering in milliseconds. */
    final long bufferTimeout;

    /** The handler to trigger when timeout. */
    final Consumer<Long> timeoutHandler;

    /** The executor service that schedules and calls the triggers of this task. */
    ScheduledExecutorService scheduledExecutor;

    /**
     * The current scheduled future, when the next scheduling occurs, the previous one that has not
     * yet been executed will be canceled.
     */
    ScheduledFuture<Void> currentScheduledFuture;

    /**
     * The current scheduled trigger sequence number, a timeout trigger is scheduled only if {@code
     * scheduledSeq} is less than {@code currentSeq}.
     */
    AtomicLong scheduledSeq;

    /**
     * The current trigger sequence number, used to distinguish different triggers. Every time a
     * trigger occurs, {@code currentSeq} increases by 1.
     */
    AtomicLong currentSeq;

    public StateRequestBuffer(long bufferTimeout, Consumer<Long> timeoutHandler) {
        this.activeQueue = new LinkedList<>();
        this.blockingQueue = new HashMap<>();
        this.blockingQueueSize = 0;
        this.bufferTimeout = bufferTimeout;
        this.timeoutHandler = timeoutHandler;
        this.scheduledSeq = new AtomicLong(-1);
        this.currentSeq = new AtomicLong(0);
        if (bufferTimeout > 0) {
            this.scheduledExecutor = DELAYER;
        }
    }

    void advanceSeq() {
        currentSeq.incrementAndGet();
    }

    boolean checkCurrentSeq(long seq) {
        return currentSeq.get() == seq;
    }

    void enqueueToActive(StateRequest<K, ?, ?> request) {
        if (request.getRequestType() == StateRequestType.SYNC_POINT) {
            request.getFuture().complete(null);
        } else {
            activeQueue.add(request);
            if (bufferTimeout > 0 && currentSeq.get() > scheduledSeq.get()) {
                if (currentScheduledFuture != null
                        && !currentScheduledFuture.isDone()
                        && !currentScheduledFuture.isCancelled()) {
                    currentScheduledFuture.cancel(false);
                }
                final long thisScheduledSeq = currentSeq.get();
                scheduledSeq.set(thisScheduledSeq);
                currentScheduledFuture =
                        (ScheduledFuture<Void>)
                                scheduledExecutor.schedule(
                                        () -> {
                                            if (thisScheduledSeq == currentSeq.get()) {
                                                timeoutHandler.accept(thisScheduledSeq);
                                            }
                                        },
                                        bufferTimeout,
                                        TimeUnit.MILLISECONDS);
            }
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
     * @param n The number of state requests to pop.
     * @param requestContainerInitializer Initializer for the stateRequest container
     * @return A StateRequestContainer which holds the popped state requests.
     */
    Optional<StateRequestContainer> popActive(
            int n, Supplier<StateRequestContainer> requestContainerInitializer) {
        final int count = Math.min(n, activeQueue.size());
        if (count <= 0) {
            return Optional.empty();
        }
        StateRequestContainer stateRequestContainer = requestContainerInitializer.get();
        for (int i = 0; i < count; i++) {
            stateRequestContainer.offer(activeQueue.pop());
        }
        return Optional.of(stateRequestContainer);
    }
}

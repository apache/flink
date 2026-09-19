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

package org.apache.flink.connector.base.source.reader.synchronization;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Test-only access to the internal state of {@link FutureCompletingBlockingQueue}.
 *
 * <p>Reflection is used deliberately rather than adding accessors to the queue: a {@code
 * VisibleForTesting} method in connector production code registers as a dependency on non-public
 * Flink API and would need a new entry in the frozen architecture-violation store.
 *
 * <p>The producer-state probes handle both a map and an array representation so that they stay
 * meaningful against an implementation that releases state by nulling an array slot. Such an
 * implementation bounds {@link #liveProducerStates} but not {@link #producerStateStorageSize},
 * which is why both are exposed.
 */
public final class QueueProbe {

    private static final Field PRODUCER_STATES_FIELD = field("putConditionAndFlags");
    private static final Field NOT_FULL_FIELD = field("notFull");
    private static final Field LOCK_FIELD = field("lock");

    private QueueProbe() {}

    /** Returns the number of producers the queue currently holds state for. */
    public static int liveProducerStates(FutureCompletingBlockingQueue<?> queue) {
        return withQueueLock(
                queue,
                () -> {
                    final Object states = producerStates(queue);
                    if (states instanceof Map) {
                        return ((Map<?, ?>) states).size();
                    }

                    int liveStates = 0;
                    for (int index = 0; index < Array.getLength(states); index++) {
                        if (Array.get(states, index) != null) {
                            liveStates++;
                        }
                    }
                    return liveStates;
                });
    }

    /**
     * Returns the size of the backing storage: map entries for a map implementation, allocated
     * slots for an array. Unlike {@link #liveProducerStates} this does not fall back to zero when
     * an array implementation nulls its released slots.
     */
    public static int producerStateStorageSize(FutureCompletingBlockingQueue<?> queue) {
        return withQueueLock(
                queue,
                () -> {
                    final Object states = producerStates(queue);
                    if (states instanceof Map) {
                        return ((Map<?, ?>) states).size();
                    }
                    return Array.getLength(states);
                });
    }

    /** Returns whether the queue holds state for {@code producerIndex} specifically. */
    public static boolean containsProducerState(
            FutureCompletingBlockingQueue<?> queue, int producerIndex) {
        return withQueueLock(
                queue,
                () -> {
                    final Object states = producerStates(queue);
                    if (states instanceof Map) {
                        return ((Map<?, ?>) states).containsKey(producerIndex);
                    }
                    return producerIndex >= 0
                            && producerIndex < Array.getLength(states)
                            && Array.get(states, producerIndex) != null;
                });
    }

    /**
     * Returns the number of conditions in the queue's {@code notFull} waiter set. Note this drops a
     * waiter as soon as it is signalled, before it has left {@code put()}.
     */
    public static int queuedPutters(FutureCompletingBlockingQueue<?> queue) {
        return withQueueLock(queue, () -> ((Queue<?>) read(NOT_FULL_FIELD, queue)).size());
    }

    /**
     * Returns the queue's own lock, so a test can hold it to drive an interleaving
     * deterministically instead of racing for it.
     */
    public static ReentrantLock queueLock(FutureCompletingBlockingQueue<?> queue) {
        return (ReentrantLock) read(LOCK_FIELD, queue);
    }

    private static Object producerStates(FutureCompletingBlockingQueue<?> queue) {
        return read(PRODUCER_STATES_FIELD, queue);
    }

    private static <T> T withQueueLock(FutureCompletingBlockingQueue<?> queue, Supplier<T> action) {
        final ReentrantLock lock = queueLock(queue);
        lock.lock();
        try {
            return action.get();
        } finally {
            lock.unlock();
        }
    }

    private static Field field(String name) {
        try {
            final Field field = FutureCompletingBlockingQueue.class.getDeclaredField(name);
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private static Object read(Field field, FutureCompletingBlockingQueue<?> queue) {
        try {
            return field.get(queue);
        } catch (IllegalAccessException e) {
            throw new AssertionError(e);
        }
    }
}

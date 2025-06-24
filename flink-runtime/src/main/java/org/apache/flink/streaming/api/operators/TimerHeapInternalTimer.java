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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link InternalTimer} to use with a {@link HeapPriorityQueueSet}.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public final class TimerHeapInternalTimer<K, N>
        implements InternalTimer<K, N>, HeapPriorityQueueElement {

    /** The key for which the timer is scoped. */
    @Nonnull private final K key;

    /** The namespace for which the timer is scoped. */
    @Nonnull private final N namespace;

    /** The expiration timestamp. */
    private final long timestamp;

    /**
     * This field holds the current physical index of this timer when it is managed by a timer heap
     * so that we can support fast deletes.
     */
    private transient int timerHeapIndex;

    public TimerHeapInternalTimer(long timestamp, @Nonnull K key, @Nonnull N namespace) {
        this.timestamp = timestamp;
        this.key = key;
        this.namespace = namespace;
        this.timerHeapIndex = NOT_CONTAINED;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Nonnull
    @Override
    public K getKey() {
        return key;
    }

    @Nonnull
    @Override
    public N getNamespace() {
        return namespace;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof InternalTimer) {
            InternalTimer<?, ?> timer = (InternalTimer<?, ?>) o;
            return timestamp == timer.getTimestamp()
                    && key.equals(timer.getKey())
                    && namespace.equals(timer.getNamespace());
        }

        return false;
    }

    @Override
    public int getInternalIndex() {
        return timerHeapIndex;
    }

    @Override
    public void setInternalIndex(int newIndex) {
        this.timerHeapIndex = newIndex;
    }

    /**
     * This method can be called to indicate that the timer is no longer managed be a timer heap,
     * e.g. because it as removed.
     */
    void removedFromTimerQueue() {
        setInternalIndex(NOT_CONTAINED);
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + key.hashCode();
        result = 31 * result + namespace.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Timer{"
                + "timestamp="
                + timestamp
                + ", key="
                + key
                + ", namespace="
                + namespace
                + '}';
    }

    @Override
    public int comparePriorityTo(@Nonnull InternalTimer<?, ?> other) {
        return Long.compare(timestamp, other.getTimestamp());
    }
}

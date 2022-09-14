/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * A snapshot of internal timers, containing event and processing timers and the serializers to use
 * to write / read them.
 */
public class InternalTimersSnapshot<K, N> {

    private TypeSerializerSnapshot<K> keySerializerSnapshot;
    private TypeSerializerSnapshot<N> namespaceSerializerSnapshot;

    private Set<TimerHeapInternalTimer<K, N>> eventTimeTimers;
    private Set<TimerHeapInternalTimer<K, N>> processingTimeTimers;

    /** Empty constructor used when restoring the timers. */
    public InternalTimersSnapshot() {}

    /** Constructor to use when snapshotting the timers. */
    public InternalTimersSnapshot(
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            @Nullable Set<TimerHeapInternalTimer<K, N>> eventTimeTimers,
            @Nullable Set<TimerHeapInternalTimer<K, N>> processingTimeTimers) {

        Preconditions.checkNotNull(keySerializer);
        this.keySerializerSnapshot = TypeSerializerUtils.snapshotBackwardsCompatible(keySerializer);
        Preconditions.checkNotNull(namespaceSerializer);
        this.namespaceSerializerSnapshot =
                TypeSerializerUtils.snapshotBackwardsCompatible(namespaceSerializer);

        this.eventTimeTimers = eventTimeTimers;
        this.processingTimeTimers = processingTimeTimers;
    }

    public TypeSerializerSnapshot<K> getKeySerializerSnapshot() {
        return keySerializerSnapshot;
    }

    public void setKeySerializerSnapshot(TypeSerializerSnapshot<K> keySerializerConfigSnapshot) {
        this.keySerializerSnapshot = keySerializerConfigSnapshot;
    }

    public TypeSerializerSnapshot<N> getNamespaceSerializerSnapshot() {
        return namespaceSerializerSnapshot;
    }

    public void setNamespaceSerializerSnapshot(
            TypeSerializerSnapshot<N> namespaceSerializerConfigSnapshot) {
        this.namespaceSerializerSnapshot = namespaceSerializerConfigSnapshot;
    }

    public Set<TimerHeapInternalTimer<K, N>> getEventTimeTimers() {
        return eventTimeTimers;
    }

    public void setEventTimeTimers(Set<TimerHeapInternalTimer<K, N>> eventTimeTimers) {
        this.eventTimeTimers = eventTimeTimers;
    }

    public Set<TimerHeapInternalTimer<K, N>> getProcessingTimeTimers() {
        return processingTimeTimers;
    }

    public void setProcessingTimeTimers(Set<TimerHeapInternalTimer<K, N>> processingTimeTimers) {
        this.processingTimeTimers = processingTimeTimers;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}

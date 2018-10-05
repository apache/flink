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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * A snapshot of internal timers, containing event and processing timers and
 * the serializers to use to write / read them.
 */
public class InternalTimersSnapshot<K, N> {

	private TypeSerializer<K> keySerializer;
	private TypeSerializerSnapshot<K> keySerializerConfigSnapshot;
	private TypeSerializer<N> namespaceSerializer;
	private TypeSerializerSnapshot<N> namespaceSerializerConfigSnapshot;

	private Set<TimerHeapInternalTimer<K, N>> eventTimeTimers;
	private Set<TimerHeapInternalTimer<K, N>> processingTimeTimers;

	/** Empty constructor used when restoring the timers. */
	public InternalTimersSnapshot() {}

	/** Constructor to use when snapshotting the timers. */
	public InternalTimersSnapshot(
			TypeSerializer<K> keySerializer,
			TypeSerializerSnapshot<K> keySerializerConfigSnapshot,
			TypeSerializer<N> namespaceSerializer,
			TypeSerializerSnapshot<N> namespaceSerializerConfigSnapshot,
			@Nullable Set<TimerHeapInternalTimer<K, N>> eventTimeTimers,
			@Nullable Set<TimerHeapInternalTimer<K, N>> processingTimeTimers) {

		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializerConfigSnapshot);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializerConfigSnapshot);
		this.eventTimeTimers = eventTimeTimers;
		this.processingTimeTimers = processingTimeTimers;
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	public void setKeySerializer(TypeSerializer<K> keySerializer) {
		this.keySerializer = keySerializer;
	}

	public TypeSerializerSnapshot<K> getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	public void setKeySerializerConfigSnapshot(TypeSerializerSnapshot<K> keySerializerConfigSnapshot) {
		this.keySerializerConfigSnapshot = keySerializerConfigSnapshot;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public void setNamespaceSerializer(TypeSerializer<N> namespaceSerializer) {
		this.namespaceSerializer = namespaceSerializer;
	}

	public TypeSerializerSnapshot<N> getNamespaceSerializerConfigSnapshot() {
		return namespaceSerializerConfigSnapshot;
	}

	public void setNamespaceSerializerConfigSnapshot(TypeSerializerSnapshot<N> namespaceSerializerConfigSnapshot) {
		this.namespaceSerializerConfigSnapshot = namespaceSerializerConfigSnapshot;
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

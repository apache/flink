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
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Set;

/**
 * A snapshot of internal timers, containing event and processing timers and
 * the serializers to use to write / read them.
 */
public class InternalTimersSnapshot<K, N> {

	private TypeSerializerConfigSnapshot<K> keySerializerConfigSnapshot;
	private TypeSerializerConfigSnapshot<N> namespaceSerializerConfigSnapshot;

	private Set<InternalTimer<K, N>> eventTimeTimers;
	private Set<InternalTimer<K, N>> processingTimeTimers;

	/** Empty constructor used when restoring the timers. */
	public InternalTimersSnapshot() {}

	/** Constructor to use when snapshotting the timers. */
	public InternalTimersSnapshot(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			@Nullable Set<InternalTimer<K, N>> eventTimeTimers,
			@Nullable Set<InternalTimer<K, N>> processingTimeTimers) {

		Preconditions.checkNotNull(keySerializer);
		this.keySerializerConfigSnapshot = Preconditions.checkNotNull(keySerializer.snapshotConfiguration());
		Preconditions.checkNotNull(namespaceSerializer);
		this.namespaceSerializerConfigSnapshot = Preconditions.checkNotNull(namespaceSerializer.snapshotConfiguration());
		this.eventTimeTimers = eventTimeTimers;
		this.processingTimeTimers = processingTimeTimers;
	}

	public TypeSerializerConfigSnapshot getKeySerializerConfigSnapshot() {
		return keySerializerConfigSnapshot;
	}

	public void setKeySerializerConfigSnapshot(TypeSerializerConfigSnapshot<K> keySerializerConfigSnapshot) {
		this.keySerializerConfigSnapshot = keySerializerConfigSnapshot;
	}

	public TypeSerializerConfigSnapshot getNamespaceSerializerConfigSnapshot() {
		return namespaceSerializerConfigSnapshot;
	}

	public void setNamespaceSerializerConfigSnapshot(TypeSerializerConfigSnapshot<N> namespaceSerializerConfigSnapshot) {
		this.namespaceSerializerConfigSnapshot = namespaceSerializerConfigSnapshot;
	}

	public Set<InternalTimer<K, N>> getEventTimeTimers() {
		return eventTimeTimers;
	}

	public void setEventTimeTimers(Set<InternalTimer<K, N>> eventTimeTimers) {
		this.eventTimeTimers = eventTimeTimers;
	}

	public Set<InternalTimer<K, N>> getProcessingTimeTimers() {
		return processingTimeTimers;
	}

	public void setProcessingTimeTimers(Set<InternalTimer<K, N>> processingTimeTimers) {
		this.processingTimeTimers = processingTimeTimers;
	}

	public TimerHeapInternalTimer.TimerSerializer<K, N> createTimerSerializer() {
		Preconditions.checkState(keySerializerConfigSnapshot != null && namespaceSerializerConfigSnapshot != null,
			"Key / namespace serializer config snapshots are null; if the timer snapshot" +
				" was restored from a checkpoint, the serializer config snapshots must be restored first before" +
				" attempting to create the timer serializer.");

		return new TimerHeapInternalTimer.TimerSerializer<>(
			keySerializerConfigSnapshot.restoreSerializer(),
			namespaceSerializerConfigSnapshot.restoreSerializer());
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

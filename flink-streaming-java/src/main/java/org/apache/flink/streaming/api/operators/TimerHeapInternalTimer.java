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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Implementation of {@link InternalTimer} for the {@link InternalTimerHeap}.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public final class TimerHeapInternalTimer<K, N> implements InternalTimer<K, N> {

	/** The index that indicates that a tracked internal timer is not tracked. */
	private static final int NOT_MANAGED_BY_TIMER_QUEUE_INDEX = Integer.MIN_VALUE;

	@Nonnull
	private final K key;

	@Nonnull
	private final N namespace;

	private final long timestamp;

	/**
	 * This field holds the current physical index of this timer when it is managed by a timer heap so that we can
	 * support fast deletes.
	 */
	private transient int timerHeapIndex;

	TimerHeapInternalTimer(long timestamp, @Nonnull K key, @Nonnull N namespace) {
		this.timestamp = timestamp;
		this.key = key;
		this.namespace = namespace;
		this.timerHeapIndex = NOT_MANAGED_BY_TIMER_QUEUE_INDEX;
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

	/**
	 * Returns the current index of this timer in the owning timer heap.
	 */
	int getTimerHeapIndex() {
		return timerHeapIndex;
	}

	/**
	 * Sets the current index of this timer in the owning timer heap and should only be called by the managing heap.
	 * @param timerHeapIndex the new index in the timer heap.
	 */
	void setTimerHeapIndex(int timerHeapIndex) {
		this.timerHeapIndex = timerHeapIndex;
	}

	/**
	 * This method can be called to indicate that the timer is no longer managed be a timer heap, e.g. because it as
	 * removed.
	 */
	void removedFromTimerQueue() {
		setTimerHeapIndex(NOT_MANAGED_BY_TIMER_QUEUE_INDEX);
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
		return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", namespace=" + namespace +
				'}';
	}

	/**
	 * A {@link TypeSerializer} used to serialize/deserialize a {@link TimerHeapInternalTimer}.
	 */
	public static class TimerSerializer<K, N> extends TypeSerializer<InternalTimer<K, N>> {

		private static final long serialVersionUID = 1119562170939152304L;

		@Nonnull
		private final TypeSerializer<K> keySerializer;

		@Nonnull
		private final TypeSerializer<N> namespaceSerializer;

		TimerSerializer(@Nonnull TypeSerializer<K> keySerializer, @Nonnull TypeSerializer<N> namespaceSerializer) {
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<InternalTimer<K, N>> duplicate() {

			final TypeSerializer<K> keySerializerDuplicate = keySerializer.duplicate();
			final TypeSerializer<N> namespaceSerializerDuplicate = namespaceSerializer.duplicate();

			if (keySerializerDuplicate == keySerializer &&
				namespaceSerializerDuplicate == namespaceSerializer) {
				// all delegate serializers seem stateless, so this is also stateless.
				return this;
			} else {
				// at least one delegate serializer seems to be stateful, so we return a new instance.
				return new TimerSerializer<>(keySerializerDuplicate, namespaceSerializerDuplicate);
			}
		}

		@Override
		public InternalTimer<K, N> createInstance() {
			throw new UnsupportedOperationException();
		}

		@Override
		public InternalTimer<K, N> copy(InternalTimer<K, N> from) {
			return new TimerHeapInternalTimer<>(from.getTimestamp(), from.getKey(), from.getNamespace());
		}

		@Override
		public InternalTimer<K, N> copy(InternalTimer<K, N> from, InternalTimer<K, N> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			// we do not have fixed length
			return -1;
		}

		@Override
		public void serialize(InternalTimer<K, N> record, DataOutputView target) throws IOException {
			keySerializer.serialize(record.getKey(), target);
			namespaceSerializer.serialize(record.getNamespace(), target);
			LongSerializer.INSTANCE.serialize(record.getTimestamp(), target);
		}

		@Override
		public InternalTimer<K, N> deserialize(DataInputView source) throws IOException {
			K key = keySerializer.deserialize(source);
			N namespace = namespaceSerializer.deserialize(source);
			Long timestamp = LongSerializer.INSTANCE.deserialize(source);
			return new TimerHeapInternalTimer<>(timestamp, key, namespace);
		}

		@Override
		public InternalTimer<K, N> deserialize(InternalTimer<K, N> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			keySerializer.copy(source, target);
			namespaceSerializer.copy(source, target);
			LongSerializer.INSTANCE.copy(source, target);
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
				(obj != null && obj.getClass() == getClass() &&
					keySerializer.equals(((TimerSerializer) obj).keySerializer) &&
					namespaceSerializer.equals(((TimerSerializer) obj).namespaceSerializer));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot<InternalTimer<K, N>> snapshotConfiguration() {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}

		@Override
		public TypeSerializerSchemaCompatibility<InternalTimer<K, N>> ensureCompatibility(TypeSerializerConfigSnapshot<?> configSnapshot) {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}
	}
}

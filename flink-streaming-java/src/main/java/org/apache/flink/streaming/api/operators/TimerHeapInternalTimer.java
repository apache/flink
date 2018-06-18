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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Comparator;

/**
 * Implementation of {@link InternalTimer} to use with a {@link HeapPriorityQueueSet}.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public final class TimerHeapInternalTimer<K, N> implements InternalTimer<K, N>, HeapPriorityQueueElement {

	/** Function to extract the key from a {@link TimerHeapInternalTimer}. */
	private static final KeyExtractorFunction<TimerHeapInternalTimer<?, ?>> KEY_EXTRACTOR_FUNCTION =
		TimerHeapInternalTimer::getKey;

	/** Function to compare instances of {@link TimerHeapInternalTimer}. */
	private static final Comparator<TimerHeapInternalTimer<?, ?>> TIMER_COMPARATOR =
		(o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp());

	/** The key for which the timer is scoped. */
	@Nonnull
	private final K key;

	/** The namespace for which the timer is scoped. */
	@Nonnull
	private final N namespace;

	/** The expiration timestamp. */
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
	 * This method can be called to indicate that the timer is no longer managed be a timer heap, e.g. because it as
	 * removed.
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
		return "Timer{" +
				"timestamp=" + timestamp +
				", key=" + key +
				", namespace=" + namespace +
				'}';
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	static <T extends TimerHeapInternalTimer> Comparator<T> getTimerComparator() {
		return (Comparator<T>) TIMER_COMPARATOR;
	}

	@SuppressWarnings("unchecked")
	@VisibleForTesting
	static <T extends TimerHeapInternalTimer> KeyExtractorFunction<T> getKeyExtractorFunction() {
		return (KeyExtractorFunction<T>) KEY_EXTRACTOR_FUNCTION;
	}

	/**
	 * A {@link TypeSerializer} used to serialize/deserialize a {@link TimerHeapInternalTimer}.
	 */
	public static class TimerSerializer<K, N> extends TypeSerializer<TimerHeapInternalTimer<K, N>> {

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
		public TypeSerializer<TimerHeapInternalTimer<K, N>> duplicate() {

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
		public TimerHeapInternalTimer<K, N> createInstance() {
			throw new UnsupportedOperationException();
		}

		@Override
		public TimerHeapInternalTimer<K, N> copy(TimerHeapInternalTimer<K, N> from) {
			return new TimerHeapInternalTimer<>(from.getTimestamp(), from.getKey(), from.getNamespace());
		}

		@Override
		public TimerHeapInternalTimer<K, N> copy(TimerHeapInternalTimer<K, N> from, TimerHeapInternalTimer<K, N> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			// we do not have fixed length
			return -1;
		}

		@Override
		public void serialize(TimerHeapInternalTimer<K, N> record, DataOutputView target) throws IOException {
			keySerializer.serialize(record.getKey(), target);
			namespaceSerializer.serialize(record.getNamespace(), target);
			LongSerializer.INSTANCE.serialize(record.getTimestamp(), target);
		}

		@Override
		public TimerHeapInternalTimer<K, N> deserialize(DataInputView source) throws IOException {
			K key = keySerializer.deserialize(source);
			N namespace = namespaceSerializer.deserialize(source);
			Long timestamp = LongSerializer.INSTANCE.deserialize(source);
			return new TimerHeapInternalTimer<>(timestamp, key, namespace);
		}

		@Override
		public TimerHeapInternalTimer<K, N> deserialize(TimerHeapInternalTimer<K, N> reuse, DataInputView source) throws IOException {
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
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}

		@Override
		public CompatibilityResult<TimerHeapInternalTimer<K, N>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}
	}
}

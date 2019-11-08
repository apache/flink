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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.MathUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

/**
 * A serializer for {@link TimerHeapInternalTimer} objects that produces a serialization format that is
 * lexicographically aligned the priority of the timers.
 *
 * @param <K> type of the timer key.
 * @param <N> type of the timer namespace.
 */
@Internal
public class TimerSerializer<K, N> extends TypeSerializer<TimerHeapInternalTimer<K, N>> {

	private static final long serialVersionUID = 1L;

	private static final int KEY_SERIALIZER_SNAPSHOT_INDEX = 0;
	private static final int NAMESPACE_SERIALIZER_SNAPSHOT_INDEX = 1;

	/** Serializer for the key. */
	@Nonnull
	private final TypeSerializer<K> keySerializer;

	/** Serializer for the namespace. */
	@Nonnull
	private final TypeSerializer<N> namespaceSerializer;

	/** The bytes written for one timer, or -1 if variable size. */
	private final int length;

	/** True iff the serialized type (and composite objects) are immutable. */
	private final boolean immutableType;

	public TimerSerializer(
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull TypeSerializer<N> namespaceSerializer) {
		this(
			keySerializer,
			namespaceSerializer,
			computeTotalByteLength(keySerializer, namespaceSerializer),
			keySerializer.isImmutableType() && namespaceSerializer.isImmutableType());
	}

	private TimerSerializer(
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull TypeSerializer<N> namespaceSerializer,
		int length,
		boolean immutableType) {

		this.keySerializer = keySerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.length = length;
		this.immutableType = immutableType;
	}

	private static int computeTotalByteLength(
		TypeSerializer<?> keySerializer,
		TypeSerializer<?> namespaceSerializer) {
		if (keySerializer.getLength() >= 0 && namespaceSerializer.getLength() >= 0) {
			// timestamp + key + namespace
			return Long.BYTES + keySerializer.getLength() + namespaceSerializer.getLength();
		} else {
			return -1;
		}
	}

	@Override
	public boolean isImmutableType() {
		return immutableType;
	}

	@Override
	public TimerSerializer<K, N> duplicate() {

		final TypeSerializer<K> keySerializerDuplicate = keySerializer.duplicate();
		final TypeSerializer<N> namespaceSerializerDuplicate = namespaceSerializer.duplicate();

		if (keySerializerDuplicate == keySerializer &&
			namespaceSerializerDuplicate == namespaceSerializer) {
			// all delegate serializers seem stateless, so this is also stateless.
			return this;
		} else {
			// at least one delegate serializer seems to be stateful, so we return a new instance.
			return new TimerSerializer<>(
				keySerializerDuplicate,
				namespaceSerializerDuplicate,
				length,
				immutableType);
		}
	}

	@Override
	public TimerHeapInternalTimer<K, N> createInstance() {
		return new TimerHeapInternalTimer<>(
			0L,
			keySerializer.createInstance(),
			namespaceSerializer.createInstance());
	}

	@Override
	public TimerHeapInternalTimer<K, N> copy(TimerHeapInternalTimer<K, N> from) {

		K keyDuplicate;
		N namespaceDuplicate;
		if (isImmutableType()) {
			keyDuplicate = from.getKey();
			namespaceDuplicate = from.getNamespace();
		} else {
			keyDuplicate = keySerializer.copy(from.getKey());
			namespaceDuplicate = namespaceSerializer.copy(from.getNamespace());
		}

		return new TimerHeapInternalTimer<>(from.getTimestamp(), keyDuplicate, namespaceDuplicate);
	}

	@Override
	public TimerHeapInternalTimer<K, N> copy(TimerHeapInternalTimer<K, N> from, TimerHeapInternalTimer<K, N> reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public void serialize(TimerHeapInternalTimer<K, N> record, DataOutputView target) throws IOException {
		target.writeLong(MathUtils.flipSignBit(record.getTimestamp()));
		keySerializer.serialize(record.getKey(), target);
		namespaceSerializer.serialize(record.getNamespace(), target);
	}

	@Override
	public TimerHeapInternalTimer<K, N> deserialize(DataInputView source) throws IOException {
		long timestamp = MathUtils.flipSignBit(source.readLong());
		K key = keySerializer.deserialize(source);
		N namespace = namespaceSerializer.deserialize(source);
		return new TimerHeapInternalTimer<>(timestamp, key, namespace);
	}

	@Override
	public TimerHeapInternalTimer<K, N> deserialize(
		TimerHeapInternalTimer<K, N> reuse,
		DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeLong(source.readLong());
		keySerializer.copy(source, target);
		namespaceSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TimerSerializer<?, ?> that = (TimerSerializer<?, ?>) o;
		return Objects.equals(keySerializer, that.keySerializer) &&
			Objects.equals(namespaceSerializer, that.namespaceSerializer);
	}

	@Override
	public int hashCode() {
		return Objects.hash(keySerializer, namespaceSerializer);
	}

	@Override
	public TimerSerializerSnapshot<K, N> snapshotConfiguration() {
		return new TimerSerializerSnapshot<>(this);
	}

	@Nonnull
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Nonnull
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	/**
	 * Snaphot of a {@link TimerSerializer}.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 *
	 * @deprecated this snapshot class is no longer in use, and is maintained only
	 *             for backwards compatibility purposes. It is fully replaced by
	 *             {@link TimerSerializerSnapshot}.
	 */
	@Deprecated
	public static class TimerSerializerConfigSnapshot<K, N> extends CompositeTypeSerializerConfigSnapshot<TimerHeapInternalTimer<K, N>> {

		private static final int VERSION = 1;

		public TimerSerializerConfigSnapshot() {
		}

		public TimerSerializerConfigSnapshot(
			@Nonnull TypeSerializer<K> keySerializer,
			@Nonnull TypeSerializer<N> namespaceSerializer) {
			super(init(keySerializer, namespaceSerializer));
		}

		private static TypeSerializer<?>[] init(
			@Nonnull TypeSerializer<?> keySerializer,
			@Nonnull TypeSerializer<?> namespaceSerializer) {
			TypeSerializer<?>[] timerSerializers = new TypeSerializer[2];
			timerSerializers[KEY_SERIALIZER_SNAPSHOT_INDEX] = keySerializer;
			timerSerializers[NAMESPACE_SERIALIZER_SNAPSHOT_INDEX] = namespaceSerializer;
			return timerSerializers;
		}

		@Override
		public int getVersion() {
			return VERSION;
		}

		@Override
		public TypeSerializerSchemaCompatibility<TimerHeapInternalTimer<K, N>> resolveSchemaCompatibility(
				TypeSerializer<TimerHeapInternalTimer<K, N>> newSerializer) {

			final TypeSerializerSnapshot<?>[] nestedSnapshots = getNestedSerializersAndConfigs()
				.stream()
				.map(t -> t.f1)
				.toArray(TypeSerializerSnapshot[]::new);

			return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
				newSerializer,
				new TimerSerializerSnapshot<>(),
				nestedSnapshots
			);
		}
	}
}

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
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Internal class for keeping track of in-flight timers.
 *
 * @param <K> Type of the keys to which timers are scoped.
 * @param <N> Type of the namespace to which timers are scoped.
 */
@Internal
public class InternalTimer<K, N> implements Comparable<InternalTimer<K, N>> {
	private final long timestamp;
	private final K key;
	private final N namespace;

	public InternalTimer(long timestamp, K key, N namespace) {
		this.timestamp = timestamp;
		this.key = key;
		this.namespace = namespace;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public K getKey() {
		return key;
	}

	public N getNamespace() {
		return namespace;
	}

	@Override
	public int compareTo(InternalTimer<K, N> o) {
		return Long.compare(this.timestamp, o.timestamp);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()){
			return false;
		}

		InternalTimer<?, ?> timer = (InternalTimer<?, ?>) o;

		return timestamp == timer.timestamp
				&& key.equals(timer.key)
				&& namespace.equals(timer.namespace);

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
	 * A {@link TypeSerializer} used to serialize/deserialize a {@link InternalTimer}.
	 */
	public static class TimerSerializer<K, N> extends TypeSerializer<InternalTimer<K, N>> {

		private static final long serialVersionUID = 1119562170939152304L;

		private final TypeSerializer<K> keySerializer;

		private final TypeSerializer<N> namespaceSerializer;

		TimerSerializer(TypeSerializer<K> keySerializer, TypeSerializer<N> namespaceSerializer) {
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<InternalTimer<K, N>> duplicate() {
			return this;
		}

		@Override
		public InternalTimer<K, N> createInstance() {
			return null;
		}

		@Override
		public InternalTimer<K, N> copy(InternalTimer<K, N> from) {
			return new InternalTimer<>(from.timestamp, from.key, from.namespace);
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
			keySerializer.serialize(record.key, target);
			namespaceSerializer.serialize(record.namespace, target);
			LongSerializer.INSTANCE.serialize(record.timestamp, target);
		}

		@Override
		public InternalTimer<K, N> deserialize(DataInputView source) throws IOException {
			K key = keySerializer.deserialize(source);
			N namespace = namespaceSerializer.deserialize(source);
			Long timestamp = LongSerializer.INSTANCE.deserialize(source);
			return new InternalTimer<>(timestamp, key, namespace);
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
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}

		@Override
		public CompatibilityResult<InternalTimer<K, N>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			throw new UnsupportedOperationException("This serializer is not registered for managed state.");
		}
	}
}

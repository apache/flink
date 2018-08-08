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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.core.memory.ByteArrayDataInputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

/** State snapshot filter of expired values with TTL. */
abstract class TtlStateSnapshotTransformer<K, N, T> implements CollectionStateSnapshotTransformer<K, N, T> {
	private final TtlTimeProvider ttlTimeProvider;
	final long ttl;
	private final ByteArrayDataInputView div;
	private final TtlIncrementalCleanup<K, N> ttlIncrementalCleanup;

	TtlStateSnapshotTransformer(
		@Nonnull TtlTimeProvider ttlTimeProvider,
		long ttl,
		@Nullable TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
		this.ttlTimeProvider = ttlTimeProvider;
		this.ttl = ttl;
		this.ttlIncrementalCleanup = ttlIncrementalCleanup;
		this.div = new ByteArrayDataInputView();
	}

	@Override
	@Nullable
	public T filterOrTransform(@Nonnull K key, @Nonnull N namespace, @Nullable T value) {
		if (value == null) {
			return null;
		}
		T filteredValue = doFilter(value);
		if (ttlIncrementalCleanup != null) {
			ttlIncrementalCleanup.addIfExpired(key, namespace, value, filteredValue);
		}
		return filteredValue;
	}

	abstract T doFilter(T value);

	<V> TtlValue<V> filterTtlValue(TtlValue<V> value) {
		return expired(value) ? null : value;
	}

	private boolean expired(TtlValue<?> ttlValue) {
		return expired(ttlValue.getLastAccessTimestamp());
	}

	boolean expired(long ts) {
		return TtlUtils.expired(ts, ttl, ttlTimeProvider);
	}

	long deserializeTs(byte[] value) throws IOException {
		div.setData(value, 0, Long.BYTES);
		return LongSerializer.INSTANCE.deserialize(div);
	}

	@Override
	public TransformStrategy getFilterStrategy() {
		return TransformStrategy.STOP_ON_FIRST_INCLUDED;
	}

	static class TtlDeserializedValueStateSnapshotTransformer<K, N, T>
		extends TtlStateSnapshotTransformer<K, N, TtlValue<T>> {
		TtlDeserializedValueStateSnapshotTransformer(
			TtlTimeProvider ttlTimeProvider,
			long ttl,
			TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
			super(ttlTimeProvider, ttl, ttlIncrementalCleanup);
		}

		@Override
		TtlValue<T> doFilter(TtlValue<T> value) {
			return filterTtlValue(value);
		}
	}

	static class TtlSerializedValueStateSnapshotTransformer<K, N> extends TtlStateSnapshotTransformer<K, N, byte[]> {
		TtlSerializedValueStateSnapshotTransformer(
			TtlTimeProvider ttlTimeProvider,
			long ttl,
			TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
			super(ttlTimeProvider, ttl, ttlIncrementalCleanup);
		}

		@Override
		byte[] doFilter(byte[] value) {
			if (value == null) {
				return null;
			}
			long ts;
			try {
				ts = deserializeTs(value);
			} catch (IOException e) {
				throw new FlinkRuntimeException("Unexpected timestamp deserialization failure");
			}
			return expired(ts) ? null : value;
		}
	}

	static class Factory<K, N, T> implements StateSnapshotTransformFactory<K, N, TtlValue<T>> {
		private final TtlTimeProvider ttlTimeProvider;
		private final long ttl;
		final TtlIncrementalCleanup<K, N> ttlIncrementalCleanup;

		Factory(@Nonnull TtlTimeProvider ttlTimeProvider,
				long ttl,
				@Nullable TtlIncrementalCleanup<K, N> ttlIncrementalCleanup) {
			this.ttlTimeProvider = ttlTimeProvider;
			this.ttl = ttl;
			this.ttlIncrementalCleanup = ttlIncrementalCleanup;
		}

		@Override
		public Optional<StateSnapshotTransformer<K, N, TtlValue<T>>> createForDeserializedState() {
			return Optional.of(new TtlDeserializedValueStateSnapshotTransformer<>(
				ttlTimeProvider, ttl, ttlIncrementalCleanup));
		}

		@Override
		public Optional<StateSnapshotTransformer<K, N, byte[]>> createForSerializedState() {
			return Optional.of(new TtlSerializedValueStateSnapshotTransformer<>(
				ttlTimeProvider, ttl, ttlIncrementalCleanup));
		}
	}
}

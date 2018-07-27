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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.StateSnapshotFilter;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Filter expired values with TTL upon snapshoting. */
abstract class TtlStateSnapshotFilter<T> implements StateSnapshotFilter<T> {
	private final TtlTimeProvider ttlTimeProvider;
	final long ttl;

	TtlStateSnapshotFilter(@Nonnull TtlTimeProvider ttlTimeProvider, long ttl) {
		this.ttlTimeProvider = ttlTimeProvider;
		this.ttl = ttl;
	}

	static class TtlValueStateSnapshotFilter<T> extends TtlStateSnapshotFilter<TtlValue<T>> {
		TtlValueStateSnapshotFilter(TtlTimeProvider ttlTimeProvider, long ttl) {
			super(ttlTimeProvider, ttl);
		}

		@Override
		@Nonnull
		public Optional<TtlValue<T>> filter(@Nonnull TtlValue<T> value) {
			return filterTtlValue(value);
		}
	}

	static class TtlMapStateSnapshotFilter<K, V> extends TtlStateSnapshotFilter<Map<K, TtlValue<V>>> {
		TtlMapStateSnapshotFilter(TtlTimeProvider ttlTimeProvider, long ttl) {
			super(ttlTimeProvider, ttl);
		}

		@SuppressWarnings("unchecked")
		@Override
		@Nonnull
		public Optional<Map<K, TtlValue<V>>> filter(@Nonnull Map<K, TtlValue<V>> value) {
			Map<K, TtlValue<V>> cleanedMap = cleanupMap(value);
			return cleanedMap.isEmpty() ? Optional.empty() : Optional.of(cleanedMap);
		}

		private Map<K, TtlValue<V>> cleanupMap(Map<K, TtlValue<V>> map) {
			Map<K, TtlValue<V>> cleanedMap = new HashMap<>();
			for (Map.Entry<K, TtlValue<V>> entry : map.entrySet()) {
				filterTtlValue(entry.getValue()).ifPresent(v -> cleanedMap.put(entry.getKey(), v));
			}
			return map.size() == cleanedMap.size() ? map : cleanedMap;
		}
	}

	static class TtlListStateSnapshotFilter<T> extends TtlStateSnapshotFilter<List<TtlValue<T>>> {
		TtlListStateSnapshotFilter(TtlTimeProvider ttlTimeProvider, long ttl) {
			super(ttlTimeProvider, ttl);
		}

		@SuppressWarnings("unchecked")
		@Override
		@Nonnull
		public Optional<List<TtlValue<T>>> filter(@Nonnull List<TtlValue<T>> value) {
			List<TtlValue<T>> unexpired = filterList(value);
			return unexpired.isEmpty() ? Optional.empty() : Optional.of(unexpired);
		}

		private List<TtlValue<T>> filterList(List<TtlValue<T>> entries) {
			List<TtlValue<T>> unexpired = new ArrayList<>();
			for (TtlValue<T> entry : entries) {
				filterTtlValue(entry).ifPresent(unexpired::add);
			}
			return unexpired;
		}
	}

	<V> Optional<TtlValue<V>> filterTtlValue(TtlValue<V> value) {
		return expired(value) ? Optional.empty() : Optional.of(value);
	}

	private boolean expired(TtlValue<?> ttlValue) {
		return expired(ttlValue.getLastAccessTimestamp());
	}

	private boolean expired(long ts) {
		return TtlUtils.expired(ts, ttl, ttlTimeProvider);
	}

	@Override
	@Nonnull
	public Optional<byte[]> filterSerialized(@Nonnull byte[] value) {
		Preconditions.checkArgument(value.length >= 8);
		long ts;
		try {
			ts = deserializeTs(value, value.length - 8);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected timestamp deserialization failure");
		}
		return expired(ts) ? Optional.empty() : Optional.of(value);
	}

	private static long deserializeTs(
		byte[] value, int offest) throws IOException {
		return LongSerializer.INSTANCE.deserialize(
			new DataInputViewStreamWrapper(new ByteArrayInputStream(value, offest, 8)));
	}
}

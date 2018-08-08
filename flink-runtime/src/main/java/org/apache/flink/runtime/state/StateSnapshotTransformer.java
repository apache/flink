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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/**
 * Transformer of state values which are included or skipped in the snapshot.
 *
 * <p>This transformer can be applied to state values
 * to decide which entries should be included into the snapshot.
 * The included entries can be optionally modified before.
 *
 * <p>Unless specified differently, the transformer should be applied per entry
 * for collection types of state, like list or map.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <T> type of state
 */
@FunctionalInterface
public interface StateSnapshotTransformer<K, N, T> {
	/**
	 * Transform or filter out state values which are included or skipped in the snapshot.
	 *
	 * @param key state key
	 * @param namespace state namespace
	 * @param value state entry to filter or transform
	 * @return value to snapshot or null which means the entry is not included
	 */
	@Nullable
	T filterOrTransform(@Nonnull K key, @Nonnull N namespace, @Nullable T value);

	/** Collection state specific transformer which says how to transform entries of the collection. */
	interface CollectionStateSnapshotTransformer<K, N, T> extends StateSnapshotTransformer<K, N, T> {
		enum TransformStrategy {
			/** Transform all entries. */
			TRANSFORM_ALL,

			/**
			 * Skip first null entries.
			 *
			 * <p>While traversing collection entries, as optimisation, stops transforming
			 * if encounters first non-null included entry and returns it plus the rest untouched.
			 */
			STOP_ON_FIRST_INCLUDED
		}

		default TransformStrategy getFilterStrategy() {
			return TransformStrategy.TRANSFORM_ALL;
		}
	}

	/**
	 * General implementation of list state transformer.
	 *
	 * <p>This transformer wraps a transformer per-entry
	 * and transforms the whole list state.
	 * If the wrapped per entry transformer is {@link CollectionStateSnapshotTransformer},
	 * it respects its {@link TransformStrategy}.
	 */
	class ListStateSnapshotTransformer<K, N, T> implements StateSnapshotTransformer<K, N, List<T>> {
		private final StateSnapshotTransformer<K, N, T> entryValueTransformer;
		private final TransformStrategy transformStrategy;

		public ListStateSnapshotTransformer(StateSnapshotTransformer<K, N, T> entryValueTransformer) {
			this.entryValueTransformer = entryValueTransformer;
			this.transformStrategy = entryValueTransformer instanceof CollectionStateSnapshotTransformer ?
				((CollectionStateSnapshotTransformer) entryValueTransformer).getFilterStrategy() :
				TransformStrategy.TRANSFORM_ALL;
		}

		@Override
		@Nullable
		public List<T> filterOrTransform(@Nonnull K key, @Nonnull N namespace, @Nullable List<T> list) {
			if (list == null) {
				return null;
			}
			List<T> transformedList = new ArrayList<>();
			boolean anyChange = false;
			for (int i = 0; i < list.size(); i++) {
				T entry = list.get(i);
				T transformedEntry = entryValueTransformer.filterOrTransform(key, namespace, entry);
				if (transformedEntry != null) {
					if (transformStrategy == STOP_ON_FIRST_INCLUDED) {
						transformedList = list.subList(i, list.size());
						anyChange = i > 0;
						break;
					} else {
						transformedList.add(transformedEntry);
					}
				}
				anyChange |= transformedEntry == null || !Objects.equals(entry, transformedEntry);
			}
			transformedList = anyChange ? transformedList : list;
			return transformedList.isEmpty() ? null : transformedList;
		}
	}

	/**
	 * General implementation of map state transformer.
	 *
	 * <p>This transformer wraps a transformer per-entry
	 * and transforms the whole map state.
	 */
	class MapStateSnapshotTransformer<K, N, UK, UV> implements StateSnapshotTransformer<K, N, Map<UK, UV>> {
		private final StateSnapshotTransformer<K, N, UV> entryValueTransformer;

		public MapStateSnapshotTransformer(StateSnapshotTransformer<K, N, UV> entryValueTransformer) {
			this.entryValueTransformer = entryValueTransformer;
		}

		@Nullable
		@Override
		public Map<UK, UV> filterOrTransform(@Nonnull K key, @Nonnull N namespace, @Nullable Map<UK, UV> map) {
			if (map == null) {
				return null;
			}
			Map<UK, UV> transformedMap = new HashMap<>();
			boolean anyChange = false;
			for (Map.Entry<UK, UV> entry : map.entrySet()) {
				UV transformedValue = entryValueTransformer.filterOrTransform(key, namespace, entry.getValue());
				if (transformedValue != null) {
					transformedMap.put(entry.getKey(), transformedValue);
				}
				anyChange |= transformedValue == null || !Objects.equals(entry.getValue(), transformedValue);
			}
			return anyChange ? (transformedMap.isEmpty() ? null : transformedMap) : map;
		}
	}

	/**
	 * This factory creates state transformers depending on the form of values to transform.
	 *
	 * <p>If there is no transforming needed, the factory methods return {@code Optional.empty()}.
	 */
	interface StateSnapshotTransformFactory<K, N, T> {
		StateSnapshotTransformFactory<?, ?, ?> NO_TRANSFORM = createNoTransform();

		@SuppressWarnings("unchecked")
		static <K, N, T> StateSnapshotTransformFactory<K, N, T> noTransform() {
			return (StateSnapshotTransformFactory<K, N, T>) NO_TRANSFORM;
		}

		static <K, N, T> StateSnapshotTransformFactory<K, N, T> createNoTransform() {
			return new StateSnapshotTransformFactory<K, N, T>() {
				@Override
				public Optional<StateSnapshotTransformer<K, N, T>> createForDeserializedState() {
					return Optional.empty();
				}

				@Override
				public Optional<StateSnapshotTransformer<K, N, byte[]>> createForSerializedState() {
					return Optional.empty();
				}
			};
		}

		Optional<StateSnapshotTransformer<K, N, T>> createForDeserializedState();

		Optional<StateSnapshotTransformer<K, N, byte[]>> createForSerializedState();
	}
}

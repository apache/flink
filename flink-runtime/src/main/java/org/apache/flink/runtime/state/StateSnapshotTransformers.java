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

import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.runtime.state.StateSnapshotTransformer.CollectionStateSnapshotTransformer.TransformStrategy.STOP_ON_FIRST_INCLUDED;

/** Collection of common state snapshot transformers and their factories. */
public class StateSnapshotTransformers {
    /**
     * General implementation of list state transformer.
     *
     * <p>This transformer wraps a transformer per-entry and transforms the whole list state. If the
     * wrapped per entry transformer is {@link CollectionStateSnapshotTransformer}, it respects its
     * {@link CollectionStateSnapshotTransformer.TransformStrategy}.
     */
    public static class ListStateSnapshotTransformer<T>
            implements StateSnapshotTransformer<List<T>> {
        private final StateSnapshotTransformer<T> entryValueTransformer;
        private final CollectionStateSnapshotTransformer.TransformStrategy transformStrategy;

        public ListStateSnapshotTransformer(StateSnapshotTransformer<T> entryValueTransformer) {
            this.entryValueTransformer = entryValueTransformer;
            this.transformStrategy =
                    entryValueTransformer instanceof CollectionStateSnapshotTransformer
                            ? ((CollectionStateSnapshotTransformer) entryValueTransformer)
                                    .getFilterStrategy()
                            : CollectionStateSnapshotTransformer.TransformStrategy.TRANSFORM_ALL;
        }

        @Override
        @Nullable
        public List<T> filterOrTransform(@Nullable List<T> list) {
            if (list == null) {
                return null;
            }
            List<T> transformedList = new ArrayList<>();
            boolean anyChange = false;
            for (int i = 0; i < list.size(); i++) {
                T entry = list.get(i);
                T transformedEntry = entryValueTransformer.filterOrTransform(entry);
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

    public static class ListStateSnapshotTransformFactory<T>
            extends StateSnapshotTransformFactoryWrapAdaptor<T, List<T>> {
        public ListStateSnapshotTransformFactory(
                StateSnapshotTransformFactory<T> originalSnapshotTransformFactory) {
            super(originalSnapshotTransformFactory);
        }

        @Override
        public Optional<StateSnapshotTransformer<List<T>>> createForDeserializedState() {
            return originalSnapshotTransformFactory
                    .createForDeserializedState()
                    .map(ListStateSnapshotTransformer::new);
        }
    }

    /**
     * General implementation of map state transformer.
     *
     * <p>This transformer wraps a transformer per-entry and transforms the whole map state.
     */
    public static class MapStateSnapshotTransformer<K, V>
            implements StateSnapshotTransformer<Map<K, V>> {
        private final StateSnapshotTransformer<V> entryValueTransformer;

        public MapStateSnapshotTransformer(StateSnapshotTransformer<V> entryValueTransformer) {
            this.entryValueTransformer = entryValueTransformer;
        }

        @Nullable
        @Override
        public Map<K, V> filterOrTransform(@Nullable Map<K, V> map) {
            if (map == null) {
                return null;
            }
            Map<K, V> transformedMap = new HashMap<>();
            boolean anyChange = false;
            for (Map.Entry<K, V> entry : map.entrySet()) {
                V transformedValue = entryValueTransformer.filterOrTransform(entry.getValue());
                if (transformedValue != null) {
                    transformedMap.put(entry.getKey(), transformedValue);
                }
                anyChange |=
                        transformedValue == null
                                || !Objects.equals(entry.getValue(), transformedValue);
            }
            return anyChange ? (transformedMap.isEmpty() ? null : transformedMap) : map;
        }
    }

    public static class MapStateSnapshotTransformFactory<K, V>
            extends StateSnapshotTransformFactoryWrapAdaptor<V, Map<K, V>> {
        public MapStateSnapshotTransformFactory(
                StateSnapshotTransformFactory<V> originalSnapshotTransformFactory) {
            super(originalSnapshotTransformFactory);
        }

        @Override
        public Optional<StateSnapshotTransformer<Map<K, V>>> createForDeserializedState() {
            return originalSnapshotTransformFactory
                    .createForDeserializedState()
                    .map(MapStateSnapshotTransformer::new);
        }
    }

    public abstract static class StateSnapshotTransformFactoryWrapAdaptor<S, T>
            implements StateSnapshotTransformFactory<T> {
        final StateSnapshotTransformFactory<S> originalSnapshotTransformFactory;

        StateSnapshotTransformFactoryWrapAdaptor(
                StateSnapshotTransformFactory<S> originalSnapshotTransformFactory) {
            this.originalSnapshotTransformFactory = originalSnapshotTransformFactory;
        }

        @Override
        public Optional<StateSnapshotTransformer<T>> createForDeserializedState() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
            throw new UnsupportedOperationException();
        }
    }
}

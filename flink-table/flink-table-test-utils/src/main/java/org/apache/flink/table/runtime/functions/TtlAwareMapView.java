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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.dataview.MapView;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * A {@link MapView} subclass that tracks per-entry write timestamps for TTL eviction.
 *
 * <p>Uses a {@link TimestampedMap} as the backing map, which stores values alongside their
 * timestamps in a single {@code HashMap}.
 */
@Internal
class TtlAwareMapView<K, V> extends MapView<K, V> {

    private final LongSupplier clock;

    TtlAwareMapView(LongSupplier clock) {
        this.clock = clock;
        super.setMap(new TimestampedMap<>(clock));
    }

    @Override
    public void setMap(Map<K, V> map) {
        TimestampedMap<K, V> tsMap = new TimestampedMap<>(clock);
        tsMap.putAll(map);
        super.setMap(tsMap);
    }

    void evictExpired(long currentTime, long ttlMillis) {
        asTsMap().evictExpired(currentTime, ttlMillis);
    }

    Map<K, Long> getTimestamps() {
        return asTsMap().getTimestamps();
    }

    void setMapWithTimestamps(Map<K, V> map, Map<K, Long> timestamps) {
        TimestampedMap<K, V> tsMap = new TimestampedMap<>(clock);
        tsMap.setDataAndTimestamps(map, timestamps);
        super.setMap(tsMap);
    }

    @SuppressWarnings("unchecked")
    private TimestampedMap<K, V> asTsMap() {
        return (TimestampedMap<K, V>) getMap();
    }

    /** Map backed by timestamped values; mutations maintain a timestamp with each entry. */
    private static class TimestampedMap<K, V> extends AbstractMap<K, V> {

        private final HashMap<K, TimestampedValue<V>> map = new HashMap<>();
        private final LongSupplier clock;

        TimestampedMap(LongSupplier clock) {
            this.clock = clock;
        }

        @Override
        public V get(Object key) {
            TimestampedValue<V> sv = map.get(key);
            return sv != null ? sv.value : null;
        }

        @Override
        public V put(K key, V value) {
            TimestampedValue<V> old =
                    map.put(key, new TimestampedValue<>(value, clock.getAsLong()));
            return old != null ? old.value : null;
        }

        @Override
        public V remove(Object key) {
            TimestampedValue<V> old = map.remove(key);
            return old != null ? old.value : null;
        }

        @Override
        public boolean containsKey(Object key) {
            return map.containsKey(key);
        }

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public void clear() {
            map.clear();
        }

        @Override
        public Set<K> keySet() {
            return map.keySet();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return new AbstractSet<Entry<K, V>>() {
                @Override
                public Iterator<Entry<K, V>> iterator() {
                    Iterator<Entry<K, TimestampedValue<V>>> it = map.entrySet().iterator();
                    return new Iterator<Entry<K, V>>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Entry<K, V> next() {
                            Entry<K, TimestampedValue<V>> e = it.next();
                            return new SimpleImmutableEntry<>(e.getKey(), e.getValue().value);
                        }

                        @Override
                        public void remove() {
                            it.remove();
                        }
                    };
                }

                @Override
                public int size() {
                    return map.size();
                }
            };
        }

        void evictExpired(long currentTime, long ttlMillis) {
            map.values().removeIf(sv -> currentTime - sv.timestamp >= ttlMillis);
        }

        Map<K, Long> getTimestamps() {
            return map.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().timestamp));
        }

        void setDataAndTimestamps(Map<? extends K, ? extends V> data, Map<K, Long> timestamps) {
            if (data.size() != timestamps.size()) {
                throw new IllegalArgumentException(
                        "Data and timestamps must have the same size: "
                                + data.size()
                                + " vs "
                                + timestamps.size());
            }
            map.clear();
            for (Entry<? extends K, ? extends V> e : data.entrySet()) {
                Long ts = timestamps.get(e.getKey());
                map.put(e.getKey(), new TimestampedValue<>(e.getValue(), ts != null ? ts : 0L));
            }
        }
    }
}

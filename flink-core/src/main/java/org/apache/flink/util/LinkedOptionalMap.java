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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A LinkedOptionalMap is an order preserving map (like {@link LinkedHashMap}) where keys have a
 * unique string name, but are optionally present, and the values are optional.
 */
@Internal
public final class LinkedOptionalMap<K, V> {

    // --------------------------------------------------------------------------------------------------------
    // Factory
    // --------------------------------------------------------------------------------------------------------

    /**
     * Creates an {@code LinkedOptionalMap} from the provided map.
     *
     * <p>This method is the equivalent of {@link Optional#of(Object)} but for maps. To support more
     * than one {@code NULL} key, an optional map requires a unique string name to be associated
     * with each key (provided by keyNameGetter)
     *
     * @param sourceMap a source map to wrap as an optional map.
     * @param keyNameGetter function that assigns a unique name to the keys of the source map.
     * @param <K> key type
     * @param <V> value type
     * @return an {@code LinkedOptionalMap} with optional named keys, and optional values.
     */
    public static <K, V> LinkedOptionalMap<K, V> optionalMapOf(
            Map<K, V> sourceMap, Function<K, String> keyNameGetter) {

        LinkedHashMap<String, KeyValue<K, V>> underlyingMap =
                CollectionUtil.newLinkedHashMapWithExpectedSize(sourceMap.size());

        sourceMap.forEach(
                (k, v) -> {
                    String keyName = keyNameGetter.apply(k);
                    underlyingMap.put(keyName, new KeyValue<>(k, v));
                });

        return new LinkedOptionalMap<>(underlyingMap);
    }

    /** Tries to merges the keys and the values of @right into @left. */
    public static <K, V> MergeResult<K, V> mergeRightIntoLeft(
            LinkedOptionalMap<K, V> left, LinkedOptionalMap<K, V> right) {
        LinkedOptionalMap<K, V> merged = new LinkedOptionalMap<>(left);
        merged.putAll(right);

        return new MergeResult<>(merged, isLeftPrefixOfRight(left, right));
    }

    // --------------------------------------------------------------------------------------------------------
    // Constructor
    // --------------------------------------------------------------------------------------------------------

    private final LinkedHashMap<String, KeyValue<K, V>> underlyingMap;

    public LinkedOptionalMap() {
        this(new LinkedHashMap<>());
    }

    public LinkedOptionalMap(int initialSize) {
        this(CollectionUtil.newLinkedHashMapWithExpectedSize(initialSize));
    }

    @SuppressWarnings("CopyConstructorMissesField")
    LinkedOptionalMap(LinkedOptionalMap<K, V> linkedOptionalMap) {
        this(new LinkedHashMap<>(linkedOptionalMap.underlyingMap));
    }

    private LinkedOptionalMap(LinkedHashMap<String, KeyValue<K, V>> underlyingMap) {
        this.underlyingMap = checkNotNull(underlyingMap);
    }

    // --------------------------------------------------------------------------------------------------------
    // API
    // --------------------------------------------------------------------------------------------------------

    public int size() {
        return underlyingMap.size();
    }

    public void put(String keyName, @Nullable K key, @Nullable V value) {
        checkNotNull(keyName);

        underlyingMap.compute(
                keyName,
                (unused, kv) -> (kv == null) ? new KeyValue<>(key, value) : kv.merge(key, value));
    }

    void putAll(LinkedOptionalMap<K, V> right) {
        for (Entry<String, KeyValue<K, V>> entry : right.underlyingMap.entrySet()) {
            KeyValue<K, V> kv = entry.getValue();
            this.put(entry.getKey(), kv.key, kv.value);
        }
    }

    /** Returns the key names of any keys or values that are absent. */
    public Set<String> absentKeysOrValues() {
        return underlyingMap.entrySet().stream()
                .filter(LinkedOptionalMap::keyOrValueIsAbsent)
                .map(Entry::getKey)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /** Checks whether there are entries with absent keys or values. */
    public boolean hasAbsentKeysOrValues() {
        for (Entry<String, KeyValue<K, V>> entry : underlyingMap.entrySet()) {
            if (keyOrValueIsAbsent(entry)) {
                return true;
            }
        }
        return false;
    }

    /** A {@link java.util.function.Consumer} that throws exceptions. */
    @FunctionalInterface
    public interface ConsumerWithException<K, V, E extends Throwable> {
        void accept(@Nonnull String keyName, @Nullable K key, @Nullable V value) throws E;
    }

    public <E extends Throwable> void forEach(ConsumerWithException<K, V, E> consumer) throws E {
        for (Entry<String, KeyValue<K, V>> entry : underlyingMap.entrySet()) {
            KeyValue<K, V> kv = entry.getValue();
            consumer.accept(entry.getKey(), kv.key, kv.value);
        }
    }

    public Set<KeyValue<K, V>> getPresentEntries() {
        return underlyingMap.entrySet().stream()
                .filter(entry -> !LinkedOptionalMap.keyOrValueIsAbsent(entry))
                .map(Entry::getValue)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Assuming all the entries of this map are present (keys and values) this method would return a
     * map with these key and values, stripped from their Optional wrappers. NOTE: please note that
     * if any of the key or values are absent this method would throw an {@link
     * IllegalStateException}.
     */
    public LinkedHashMap<K, V> unwrapOptionals() {
        final LinkedHashMap<K, V> unwrapped =
                CollectionUtil.newLinkedHashMapWithExpectedSize(underlyingMap.size());

        for (Entry<String, KeyValue<K, V>> entry : underlyingMap.entrySet()) {
            String namedKey = entry.getKey();
            KeyValue<K, V> kv = entry.getValue();
            if (kv.key == null) {
                throw new IllegalStateException("Missing key '" + namedKey + "'");
            }
            if (kv.value == null) {
                throw new IllegalStateException("Missing value for the key '" + namedKey + "'");
            }
            unwrapped.put(kv.key, kv.value);
        }
        return unwrapped;
    }

    /** Returns the key names added to this map. */
    public Set<String> keyNames() {
        return underlyingMap.keySet();
    }

    // --------------------------------------------------------------------------------------------------------
    // Static Utility Methods
    // --------------------------------------------------------------------------------------------------------

    private static <K, V> boolean keyOrValueIsAbsent(Entry<String, KeyValue<K, V>> entry) {
        KeyValue<K, V> kv = entry.getValue();
        return kv.key == null || kv.value == null;
    }

    @VisibleForTesting
    static <K, V> boolean isLeftPrefixOfRight(
            LinkedOptionalMap<K, V> left, LinkedOptionalMap<K, V> right) {
        Iterator<String> rightKeys = right.keyNames().iterator();

        for (String leftKey : left.keyNames()) {
            if (!rightKeys.hasNext()) {
                return false;
            }
            String rightKey = rightKeys.next();
            if (!leftKey.equals(rightKey)) {
                return false;
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------------------
    // Inner Classes
    // --------------------------------------------------------------------------------------------------------

    /**
     * Key-value pairs stored by the underlying map.
     *
     * @param <K> key type.
     * @param <V> value type.
     */
    public static final class KeyValue<K, V> {
        K key;
        V value;

        KeyValue(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        KeyValue<K, V> merge(K key, V value) {
            this.key = firstNonNull(key, this.key);
            this.value = firstNonNull(value, this.value);
            return this;
        }

        private static <T> T firstNonNull(T first, T second) {
            if (first != null) {
                return first;
            }
            return second;
        }
    }

    // --------------------------------------------------------------------------------------------------------
    // Merge
    // --------------------------------------------------------------------------------------------------------

    /**
     * The result of merging two {@link LinkedOptionalMap}s using {@link
     * #mergeRightIntoLeft(LinkedOptionalMap, LinkedOptionalMap)}.
     */
    public static final class MergeResult<K, V> {
        private final LinkedOptionalMap<K, V> merged;
        private final Set<String> missingKeys;
        private final boolean isOrderedSubset;

        MergeResult(LinkedOptionalMap<K, V> merged, boolean isOrderedSubset) {
            this.merged = merged;
            this.missingKeys = merged.absentKeysOrValues();
            this.isOrderedSubset = isOrderedSubset;
        }

        public boolean hasMissingKeys() {
            return !missingKeys.isEmpty();
        }

        public Set<String> missingKeys() {
            return missingKeys;
        }

        public LinkedHashMap<K, V> getMerged() {
            return merged.unwrapOptionals();
        }

        /**
         * Returns {@code true} if keyNames present at @left, appearing in prefix order at @right.
         */
        public boolean isOrderedSubset() {
            return isOrderedSubset;
        }
    }
}

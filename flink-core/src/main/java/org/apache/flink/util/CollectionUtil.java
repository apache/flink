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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nullable;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/** Simple utility to work with Java collections. */
@Internal
public final class CollectionUtil {

    /** A safe maximum size for arrays in the JVM. */
    public static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    /** The default load factor for hash maps create with this util class. */
    static final float HASH_MAP_DEFAULT_LOAD_FACTOR = 0.75f;

    private CollectionUtil() {
        throw new AssertionError();
    }

    public static boolean isNullOrEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isNullOrEmpty(Map<?, ?> map) {
        return map == null || map.isEmpty();
    }

    public static <T> Set<T> ofNullable(@Nullable T obj) {
        return obj == null ? Collections.emptySet() : Collections.singleton(obj);
    }

    public static <T, R> Stream<R> mapWithIndex(
            Collection<T> input, final BiFunction<T, Integer, R> mapper) {
        final AtomicInteger count = new AtomicInteger(0);

        return input.stream().map(element -> mapper.apply(element, count.getAndIncrement()));
    }

    /** Partition a collection into approximately n buckets. */
    public static <T> Collection<List<T>> partition(Collection<T> elements, int numBuckets) {
        Map<Integer, List<T>> buckets = newHashMapWithExpectedSize(numBuckets);

        int initialCapacity = elements.size() / numBuckets;

        int index = 0;
        for (T element : elements) {
            int bucket = index % numBuckets;
            buckets.computeIfAbsent(bucket, key -> new ArrayList<>(initialCapacity)).add(element);
            index++;
        }

        return buckets.values();
    }

    public static <I, O> Collection<O> project(Collection<I> collection, Function<I, O> projector) {
        return collection.stream().map(projector).collect(toList());
    }

    /**
     * Collects the elements in the Iterable in a List. If the iterable argument is null, this
     * method returns an empty list.
     */
    public static <E> List<E> iterableToList(@Nullable Iterable<E> iterable) {
        if (iterable == null) {
            return Collections.emptyList();
        }

        final ArrayList<E> list = new ArrayList<>();
        iterable.iterator().forEachRemaining(list::add);
        return list;
    }

    /**
     * Collects the elements in the Iterator in a List. If the iterator argument is null, this
     * method returns an empty list.
     */
    public static <E> List<E> iteratorToList(@Nullable Iterator<E> iterator) {
        if (iterator == null) {
            return Collections.emptyList();
        }

        final ArrayList<E> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        return list;
    }

    /** Returns an immutable {@link Map.Entry}. */
    public static <K, V> Map.Entry<K, V> entry(K k, V v) {
        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }

    /** Returns an immutable {@link Map} from the provided entries. */
    @SafeVarargs
    public static <K, V> Map<K, V> map(Map.Entry<K, V>... entries) {
        if (entries == null) {
            return Collections.emptyMap();
        }
        Map<K, V> map = new HashMap<>();
        for (Map.Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * Creates a new {@link HashMap} of the expected size, i.e. a hash map that will not rehash if
     * expectedSize many keys are inserted, considering the load factor.
     *
     * @param expectedSize the expected size of the created hash map.
     * @return a new hash map instance with enough capacity for the expected size.
     * @param <K> the type of keys maintained by this map.
     * @param <V> the type of mapped values.
     */
    public static <K, V> HashMap<K, V> newHashMapWithExpectedSize(int expectedSize) {
        return new HashMap<>(
                computeRequiredCapacity(expectedSize, HASH_MAP_DEFAULT_LOAD_FACTOR),
                HASH_MAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new {@link LinkedHashMap} of the expected size, i.e. a hash map that will not
     * rehash if expectedSize many keys are inserted, considering the load factor.
     *
     * @param expectedSize the expected size of the created hash map.
     * @return a new hash map instance with enough capacity for the expected size.
     * @param <K> the type of keys maintained by this map.
     * @param <V> the type of mapped values.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMapWithExpectedSize(int expectedSize) {
        return new LinkedHashMap<>(
                computeRequiredCapacity(expectedSize, HASH_MAP_DEFAULT_LOAD_FACTOR),
                HASH_MAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new {@link HashSet} of the expected size, i.e. a hash set that will not rehash if
     * expectedSize many unique elements are inserted, considering the load factor.
     *
     * @param expectedSize the expected size of the created hash map.
     * @return a new hash map instance with enough capacity for the expected size.
     * @param <E> the type of elements stored by this set.
     */
    public static <E> HashSet<E> newHashSetWithExpectedSize(int expectedSize) {
        return new HashSet<>(
                computeRequiredCapacity(expectedSize, HASH_MAP_DEFAULT_LOAD_FACTOR),
                HASH_MAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a new {@link LinkedHashSet} of the expected size, i.e. a hash set that will not
     * rehash if expectedSize many unique elements are inserted, considering the load factor.
     *
     * @param expectedSize the expected size of the created hash map.
     * @return a new hash map instance with enough capacity for the expected size.
     * @param <E> the type of elements stored by this set.
     */
    public static <E> LinkedHashSet<E> newLinkedHashSetWithExpectedSize(int expectedSize) {
        return new LinkedHashSet<>(
                computeRequiredCapacity(expectedSize, HASH_MAP_DEFAULT_LOAD_FACTOR),
                HASH_MAP_DEFAULT_LOAD_FACTOR);
    }

    /**
     * Helper method to compute the right capacity for a hash map with load factor
     * HASH_MAP_DEFAULT_LOAD_FACTOR.
     */
    @VisibleForTesting
    static int computeRequiredCapacity(int expectedSize, float loadFactor) {
        Preconditions.checkArgument(expectedSize >= 0);
        Preconditions.checkArgument(loadFactor > 0f);
        if (expectedSize <= 2) {
            return expectedSize + 1;
        }
        return expectedSize < (Integer.MAX_VALUE / 2 + 1)
                ? (int) Math.ceil(expectedSize / loadFactor)
                : Integer.MAX_VALUE;
    }
}

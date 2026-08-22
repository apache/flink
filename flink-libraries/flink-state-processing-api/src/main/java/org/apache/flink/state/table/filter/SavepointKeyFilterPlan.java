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

package org.apache.flink.state.table.filter;

import org.apache.flink.annotation.Internal;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.state.api.filter.SerializableComparator;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Set;

/**
 * The internal, richer view of a {@link SavepointKeyFilter} used while combining and analyzing
 * filters during push-down translation.
 */
@Internal
public interface SavepointKeyFilterPlan<K> extends SavepointKeyFilter<K> {

    /** Returns {@code true} if this filter rejects every key. */
    default boolean isEmpty() {
        return false;
    }

    /** Returns the lower bound of this filter's range, or {@code null} if there is none. */
    @Nullable
    default BoundInfo<K> getLowerBound() {
        return null;
    }

    /** Returns the upper bound of this filter's range, or {@code null} if there is none. */
    @Nullable
    default BoundInfo<K> getUpperBound() {
        return null;
    }

    /**
     * Returns a filter that accepts a key if and only if both {@code this} and {@code other} accept
     * it.
     */
    SavepointKeyFilterPlan<K> intersect(SavepointKeyFilterPlan<K> other);

    /** Returns a filter matching the subset of {@code keys} that {@code predicate} accepts. */
    static <K> SavepointKeyFilterPlan<K> filterKeys(Set<K> keys, SavepointKeyFilter<K> predicate) {
        final Set<K> retained = new HashSet<>();
        for (K key : keys) {
            if (predicate.test(key)) {
                retained.add(key);
            }
        }
        return exact(retained);
    }

    /** Returns a filter accepting only the given keys, or the empty filter if there are none. */
    static <K> SavepointKeyFilterPlan<K> exact(Set<K> keys) {
        if (keys.isEmpty()) {
            return EmptyKeyFilterPlan.instance();
        }
        return new ExactKeyFilterPlan<>(keys);
    }

    /** Returns a filter accepting only the given key. */
    static <K> SavepointKeyFilterPlan<K> exact(K value) {
        return new ExactKeyFilterPlan<>(Set.of(value));
    }

    /** Returns a range filter over the natural ordering of the key type. */
    static <K extends Comparable<K>> SavepointKeyFilterPlan<K> range(
            @Nullable K lower, boolean lowerInclusive, @Nullable K upper, boolean upperInclusive) {
        return range(lower, lowerInclusive, upper, upperInclusive, new NaturalOrderComparator<>());
    }

    /** Returns a range filter ordered by {@code comparator}; a {@code null} bound is unbounded. */
    static <K> SavepointKeyFilterPlan<K> range(
            @Nullable K lower,
            boolean lowerInclusive,
            @Nullable K upper,
            boolean upperInclusive,
            SerializableComparator<K> comparator) {
        BoundInfo<K> lowerBoundInfo = lower != null ? new BoundInfo<>(lower, lowerInclusive) : null;
        BoundInfo<K> upperBoundInfo = upper != null ? new BoundInfo<>(upper, upperInclusive) : null;
        return new RangeKeyFilterPlan<>(comparator, lowerBoundInfo, upperBoundInfo);
    }

    /** Returns a filter that rejects every key. */
    static <K> SavepointKeyFilterPlan<K> empty() {
        return EmptyKeyFilterPlan.instance();
    }
}

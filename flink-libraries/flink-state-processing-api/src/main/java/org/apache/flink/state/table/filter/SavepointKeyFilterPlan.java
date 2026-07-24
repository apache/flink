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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.state.api.filter.SavepointKeyFilter;
import org.apache.flink.state.api.filter.SerializableComparator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The internal, richer view of a {@link SavepointKeyFilter} used while combining and analyzing
 * filters during push-down translation.
 */
@Experimental
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

    /** Returns {@code true} if this filter is a single range, reporting its bounds. */
    default boolean isRange() {
        return false;
    }

    /**
     * Returns the finite set of keys this filter rejects while accepting every other key, or {@code
     * null} if this filter is not a pure exclusion.
     */
    @Nullable
    default Set<K> getExcludedKeys() {
        return null;
    }

    /**
     * Decomposes this filter into its top-level conjunctive (AND) branches; a non-conjunction
     * filter is a single branch.
     */
    default List<SavepointKeyFilterPlan<K>> getConjunctionBranches() {
        return Collections.singletonList(this);
    }

    /**
     * Returns a filter that accepts a key if and only if both {@code this} and {@code other} accept
     * it.
     */
    SavepointKeyFilterPlan<K> intersect(SavepointKeyFilterPlan<K> other);

    static <K> SavepointKeyFilterPlan<K> filterKeys(Set<K> keys, SavepointKeyFilter<K> predicate) {
        final Set<K> retained = new HashSet<>();
        for (K key : keys) {
            if (predicate.test(key)) {
                retained.add(key);
            }
        }
        return exact(retained);
    }

    static <K> SavepointKeyFilterPlan<K> exact(Set<K> keys) {
        if (keys.isEmpty()) {
            return EmptyKeyFilterPlan.instance();
        }
        return new ExactKeyFilterPlan<>(keys);
    }

    static <K> SavepointKeyFilterPlan<K> exact(K value) {
        return new ExactKeyFilterPlan<>(Set.of(value));
    }

    static <K extends Comparable<K>> SavepointKeyFilterPlan<K> range(
            @Nullable K lower, boolean lowerInclusive, @Nullable K upper, boolean upperInclusive) {
        return range(lower, lowerInclusive, upper, upperInclusive, new NaturalOrderComparator<>());
    }

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

    static <K> SavepointKeyFilterPlan<K> empty() {
        return EmptyKeyFilterPlan.instance();
    }

    /**
     * Returns a filter accepting a key if and only if it is <em>not</em> one of the excluded keys
     * ({@code key <> v} or {@code key NOT IN (..)}), or {@code null} if nothing is excluded.
     */
    @Nullable
    static <K> SavepointKeyFilterPlan<K> exclude(Set<K> excluded) {
        if (excluded.isEmpty()) {
            return null;
        }
        return new ExclusionKeyFilterPlan<>(excluded);
    }

    /** Returns a filter accepting a key if and only if it is <em>not</em> the excluded key. */
    static <K> SavepointKeyFilterPlan<K> exclude(K value) {
        return new ExclusionKeyFilterPlan<>(Set.of(value));
    }

    /** Returns a filter accepting a key if and only if both {@code a} and {@code b} accept it. */
    static <K> SavepointKeyFilterPlan<K> conjunction(
            SavepointKeyFilterPlan<K> a, SavepointKeyFilterPlan<K> b) {
        if (a.isEmpty() || b.isEmpty()) {
            return empty();
        }
        final List<SavepointKeyFilterPlan<K>> branches = new ArrayList<>();
        branches.addAll(a.getConjunctionBranches());
        branches.addAll(b.getConjunctionBranches());
        return new ConjunctionKeyFilterPlan<>(branches);
    }
}

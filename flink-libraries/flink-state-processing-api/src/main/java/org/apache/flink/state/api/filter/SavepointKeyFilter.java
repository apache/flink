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

package org.apache.flink.state.api.filter;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/** Represents a key filter that can be pushed down into a savepoint scan. */
public interface SavepointKeyFilter extends Serializable {

    /** Returns {@code true} if the given key passes this filter. */
    boolean test(Object key);

    /** Returns {@code true} if this filter rejects every key. */
    default boolean isEmpty() {
        return false;
    }

    /**
     * Returns the finite set of keys this filter matches, or {@code null} if the filter does not
     * resolve to a finite key set.
     */
    @Nullable
    default Set<Object> getExactKeys() {
        return null;
    }

    /**
     * Returns the lower bound of this filter's range, or {@code null} if the filter does not define
     * a lower bound.
     */
    @Nullable
    default BoundInfo getLowerBound() {
        return null;
    }

    /**
     * Returns the upper bound of this filter's range, or {@code null} if the filter does not define
     * an upper bound.
     */
    @Nullable
    default BoundInfo getUpperBound() {
        return null;
    }

    /**
     * Returns a filter that accepts a key if and only if both {@code this} and {@code other} accept
     * it.
     */
    SavepointKeyFilter intersect(SavepointKeyFilter other);

    static SavepointKeyFilter filterKeys(Set<Object> keys, SavepointKeyFilter predicate) {
        final Set<Object> retained = new HashSet<>();
        for (Object key : keys) {
            if (predicate.test(key)) {
                retained.add(key);
            }
        }
        return exact(retained);
    }

    static SavepointKeyFilter exact(Set<Object> keys) {
        if (keys.isEmpty()) {
            return EmptyKeyFilter.INSTANCE;
        }
        return new ExactKeyFilter(keys);
    }

    static SavepointKeyFilter exact(Object value) {
        return new ExactKeyFilter(Set.of(value));
    }

    static SavepointKeyFilter range(
            @Nullable Comparable<?> lower,
            boolean lowerInclusive,
            @Nullable Comparable<?> upper,
            boolean upperInclusive) {
        BoundInfo lowerBoundInfo = lower != null ? new BoundInfo(lower, lowerInclusive) : null;
        BoundInfo upperBoundInfo = upper != null ? new BoundInfo(upper, upperInclusive) : null;
        return new RangeKeyFilter(lowerBoundInfo, upperBoundInfo);
    }

    static SavepointKeyFilter empty() {
        return EmptyKeyFilter.INSTANCE;
    }
}

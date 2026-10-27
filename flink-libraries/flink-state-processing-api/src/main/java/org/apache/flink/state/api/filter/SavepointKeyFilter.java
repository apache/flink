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

import org.apache.flink.annotation.Experimental;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Set;

/**
 * Represents a key filter that can be pushed down into a savepoint scan.
 *
 * @param <K> The type of the key.
 */
@Experimental
public interface SavepointKeyFilter<K> extends Serializable {

    /** Returns {@code true} if the given key passes this filter. */
    boolean test(K key);

    /**
     * Returns the finite set of keys this filter matches, or {@code null} if the filter does not
     * resolve to a finite key set.
     */
    @Nullable
    default Set<K> getExactKeys() {
        return null;
    }

    static <K> SavepointKeyFilter<K> exact(Set<K> keys) {
        return new ExactKeyFilter<>(keys);
    }

    static <K> SavepointKeyFilter<K> exact(K value) {
        return exact(Set.of(value));
    }

    static <K extends Comparable<K>> SavepointKeyFilter<K> range(
            @Nullable K lower, boolean lowerInclusive, @Nullable K upper, boolean upperInclusive) {
        return range(lower, lowerInclusive, upper, upperInclusive, new NaturalOrderComparator<>());
    }

    static <K> SavepointKeyFilter<K> range(
            @Nullable K lower,
            boolean lowerInclusive,
            @Nullable K upper,
            boolean upperInclusive,
            SerializableComparator<K> comparator) {
        return new RangeKeyFilter<>(comparator, lower, lowerInclusive, upper, upperInclusive);
    }

    final class NaturalOrderComparator<K extends Comparable<K>>
            implements SerializableComparator<K> {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(K a, K b) {
            return a.compareTo(b);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof NaturalOrderComparator;
        }

        @Override
        public int hashCode() {
            return NaturalOrderComparator.class.hashCode();
        }
    }
}

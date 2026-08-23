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

import java.util.HashSet;
import java.util.Set;

/**
 * A filter that rejects a finite set of keys and accepts everything else (logical NOT of a finite
 * set, e.g. {@code key <> v} or {@code key NOT IN (..)}).
 */
final class ExclusionKeyFilterPlan<K> implements SavepointKeyFilterPlan<K> {

    private static final long serialVersionUID = 6L;

    private final Set<K> excluded;

    ExclusionKeyFilterPlan(Set<K> excluded) {
        this.excluded = Set.copyOf(excluded);
    }

    @Override
    public boolean test(K key) {
        return !excluded.contains(key);
    }

    @Override
    public Set<K> getExcludedKeys() {
        return excluded;
    }

    @Override
    public SavepointKeyFilterPlan<K> intersect(SavepointKeyFilterPlan<K> other) {
        if (other.isEmpty()) {
            return other;
        }
        final Set<K> otherKeys = other.getExactKeys();
        if (otherKeys != null) {
            final Set<K> retained = new HashSet<>(otherKeys);
            retained.removeAll(excluded);
            return SavepointKeyFilterPlan.exact(retained);
        }
        final Set<K> otherExcluded = other.getExcludedKeys();
        if (otherExcluded != null) {
            // NOT a AND NOT b = NOT (a OR b).
            final Set<K> merged = new HashSet<>(excluded);
            merged.addAll(otherExcluded);
            return new ExclusionKeyFilterPlan<>(merged);
        }
        // A range (or any other predicate) minus a finite set is not a single filter, so keep both
        // constraints as a precise per-key conjunction.
        return SavepointKeyFilterPlan.conjunction(this, other);
    }

    @Override
    public String toString() {
        return "ExclusionKeyFilterPlan!" + excluded;
    }
}

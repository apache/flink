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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;

/**
 * A filter that accepts a key only if every branch accepts it (logical AND).
 *
 * <p>Used as the fallback for filter pairs that cannot be simplified into a single {@link
 * ExactKeyFilterPlan} or {@link RangeKeyFilterPlan}, for example a range intersected with an
 * exclusion.
 */
final class ConjunctionKeyFilterPlan<K> implements SavepointKeyFilterPlan<K> {

    private static final long serialVersionUID = 7L;

    private final List<SavepointKeyFilterPlan<K>> branches;

    ConjunctionKeyFilterPlan(List<SavepointKeyFilterPlan<K>> branches) {
        this.branches = List.copyOf(branches);
    }

    @Override
    public List<SavepointKeyFilterPlan<K>> getConjunctionBranches() {
        return branches;
    }

    @Override
    public boolean test(K key) {
        for (SavepointKeyFilterPlan<K> branch : branches) {
            if (!branch.test(key)) {
                return false;
            }
        }
        return true;
    }

    @Override
    @Nullable
    public Set<K> getExactKeys() {
        // If any branch is a finite key set, the conjunction is at most that set narrowed by the
        // other branches — still finite, so key groups can be pruned.
        for (SavepointKeyFilterPlan<K> branch : branches) {
            final Set<K> keys = branch.getExactKeys();
            if (keys != null) {
                return SavepointKeyFilterPlan.filterKeys(keys, this).getExactKeys();
            }
        }
        return null;
    }

    @Override
    public SavepointKeyFilterPlan<K> intersect(SavepointKeyFilterPlan<K> other) {
        if (other.isEmpty()) {
            return other;
        }
        final Set<K> otherKeys = other.getExactKeys();
        if (otherKeys != null) {
            return SavepointKeyFilterPlan.filterKeys(otherKeys, this);
        }
        return SavepointKeyFilterPlan.conjunction(this, other);
    }

    @Override
    public String toString() {
        return "ConjunctionKeyFilterPlan" + branches;
    }
}

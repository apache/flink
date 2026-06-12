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

import java.util.Set;

/** A filter based on a comparable range. */
final class RangeKeyFilter implements SavepointKeyFilter {

    private static final long serialVersionUID = 3L;

    @Nullable private final BoundInfo lower;
    @Nullable private final BoundInfo upper;

    RangeKeyFilter(@Nullable BoundInfo lower, @Nullable BoundInfo upper) {
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public boolean test(Object key) {
        if (lower != null) {
            int cmp = compare(lower.getValue(), key);
            if (cmp > 0 || (cmp == 0 && !lower.isInclusive())) {
                return false;
            }
        }
        if (upper != null) {
            int cmp = compare(upper.getValue(), key);
            if (cmp < 0 || (cmp == 0 && !upper.isInclusive())) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static int compare(Comparable<?> a, Object b) {
        return ((Comparable<Object>) a).compareTo(b);
    }

    @Override
    public BoundInfo getLowerBound() {
        return lower;
    }

    @Override
    public BoundInfo getUpperBound() {
        return upper;
    }

    @Override
    public SavepointKeyFilter intersect(SavepointKeyFilter other) {
        if (other.isEmpty()) {
            return other;
        }
        final Set<Object> otherExactKeys = other.getExactKeys();
        if (otherExactKeys != null) {
            return SavepointKeyFilter.filterKeys(otherExactKeys, this);
        }
        return intersectRange(other.getLowerBound(), other.getUpperBound());
    }

    private SavepointKeyFilter intersectRange(
            @Nullable BoundInfo otherLower, @Nullable BoundInfo otherUpper) {
        BoundInfo newLower = tighter(lower, otherLower, true);
        BoundInfo newUpper = tighter(upper, otherUpper, false);

        if (newLower != null && newUpper != null) {
            int cmp = compare(newLower.getValue(), newUpper.getValue());
            if (cmp > 0) {
                return SavepointKeyFilter.empty();
            }
            if (cmp == 0 && (!newLower.isInclusive() || !newUpper.isInclusive())) {
                return SavepointKeyFilter.empty();
            }
        }
        return new RangeKeyFilter(newLower, newUpper);
    }

    @Nullable
    private static BoundInfo tighter(
            @Nullable BoundInfo a, @Nullable BoundInfo b, boolean preferHigher) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        int c = compare(a.getValue(), b.getValue());
        if (c == 0) {
            return new BoundInfo(a.getValue(), a.isInclusive() && b.isInclusive());
        }
        boolean aWins = preferHigher ? c > 0 : c < 0;
        return aWins ? a : b;
    }

    @Override
    public String toString() {
        String lowerStr =
                lower == null ? "(-∞" : (lower.isInclusive() ? "[" : "(") + lower.getValue();
        String upperStr =
                upper == null ? "+∞)" : upper.getValue() + (upper.isInclusive() ? "]" : ")");
        return "RangeKeyFilter" + lowerStr + ", " + upperStr;
    }
}

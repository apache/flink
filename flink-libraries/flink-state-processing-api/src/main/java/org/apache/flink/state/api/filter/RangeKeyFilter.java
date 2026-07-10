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

/** A filter based on a range with an injected comparator. */
final class RangeKeyFilter<K> implements SavepointKeyFilter<K> {

    private static final long serialVersionUID = 3L;

    private final SerializableComparator<K> comparator;
    @Nullable private final K lower;
    private final boolean isLowerInclusive;
    @Nullable private final K upper;
    private final boolean isUpperInclusive;

    RangeKeyFilter(
            SerializableComparator<K> comparator,
            @Nullable K lower,
            boolean isLowerInclusive,
            @Nullable K upper,
            boolean isUpperInclusive) {
        this.comparator = comparator;
        this.lower = lower;
        this.isLowerInclusive = isLowerInclusive;
        this.upper = upper;
        this.isUpperInclusive = isUpperInclusive;
    }

    @Override
    public boolean test(K key) {
        if (lower != null) {
            int cmp = comparator.compare(lower, key);
            if (cmp > 0 || (cmp == 0 && !isLowerInclusive)) {
                return false;
            }
        }
        if (upper != null) {
            int cmp = comparator.compare(upper, key);
            if (cmp < 0 || (cmp == 0 && !isUpperInclusive)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        String lowerStr = lower == null ? "(-∞" : (isLowerInclusive ? "[" : "(") + lower;
        String upperStr = upper == null ? "+∞)" : upper + (isUpperInclusive ? "]" : ")");
        return "RangeKeyFilter" + lowerStr + ", " + upperStr;
    }
}

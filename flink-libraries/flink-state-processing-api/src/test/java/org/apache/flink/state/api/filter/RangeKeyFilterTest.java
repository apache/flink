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

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RangeKeyFilter}, built through {@link SavepointKeyFilter#range}. */
class RangeKeyFilterTest {

    // -------------------------------------------------------------------------
    //  Natural order — bound inclusiveness
    // -------------------------------------------------------------------------

    @Test
    void closedRangeIncludesBothBounds() {
        // [5, 10]
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(5L, true, 10L, true);

        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(7L)).isTrue();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void openRangeExcludesBothBounds() {
        // (5, 10)
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(5L, false, 10L, false);

        assertThat(filter.test(5L)).isFalse();
        assertThat(filter.test(6L)).isTrue();
        assertThat(filter.test(9L)).isTrue();
        assertThat(filter.test(10L)).isFalse();
    }

    @Test
    void halfOpenRangesExcludeOnlyTheExclusiveBound() {
        // [5, 10) and (5, 10]
        SavepointKeyFilter<Long> lowerInclusive = SavepointKeyFilter.range(5L, true, 10L, false);
        assertThat(lowerInclusive.test(5L)).isTrue();
        assertThat(lowerInclusive.test(10L)).isFalse();

        SavepointKeyFilter<Long> upperInclusive = SavepointKeyFilter.range(5L, false, 10L, true);
        assertThat(upperInclusive.test(5L)).isFalse();
        assertThat(upperInclusive.test(10L)).isTrue();
    }

    // -------------------------------------------------------------------------
    //  Natural order — unbounded and degenerate ranges
    // -------------------------------------------------------------------------

    @Test
    void nullLowerBoundMeansUnboundedBelow() {
        // (-∞, 10]
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(null, true, 10L, true);

        assertThat(filter.test(Long.MIN_VALUE)).isTrue();
        assertThat(filter.test(10L)).isTrue();
        assertThat(filter.test(11L)).isFalse();
    }

    @Test
    void nullUpperBoundMeansUnboundedAbove() {
        // [5, +∞)
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(5L, true, null, true);

        assertThat(filter.test(4L)).isFalse();
        assertThat(filter.test(5L)).isTrue();
        assertThat(filter.test(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void bothBoundsNullMatchesEverything() {
        // (-∞, +∞)
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(null, true, null, true);

        assertThat(filter.test(Long.MIN_VALUE)).isTrue();
        assertThat(filter.test(0L)).isTrue();
        assertThat(filter.test(Long.MAX_VALUE)).isTrue();
    }

    @Test
    void equalInclusiveBoundsMatchOnlyThatKey() {
        // [7, 7]
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(7L, true, 7L, true);

        assertThat(filter.test(6L)).isFalse();
        assertThat(filter.test(7L)).isTrue();
        assertThat(filter.test(8L)).isFalse();
    }

    @Test
    void equalBoundsWithAnExclusiveSideMatchNothing() {
        // (7, 7] and [7, 7)
        assertThat(SavepointKeyFilter.range(7L, false, 7L, true).test(7L)).isFalse();
        assertThat(SavepointKeyFilter.range(7L, true, 7L, false).test(7L)).isFalse();
    }

    @Test
    void invertedBoundsMatchNothingUnderNaturalOrder() {
        // [10, 5] — the lower bound is above the upper one
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(10L, true, 5L, true);

        assertThat(filter.test(5L)).isFalse();
        assertThat(filter.test(7L)).isFalse();
        assertThat(filter.test(10L)).isFalse();
    }

    @Test
    void doesNotResolveToAFiniteKeySet() {
        assertThat(SavepointKeyFilter.range(5L, true, 10L, true).getExactKeys()).isNull();
    }

    @Test
    void stringKeysUseNaturalOrder() {
        // ['beta', 'delta']
        SavepointKeyFilter<String> filter = SavepointKeyFilter.range("beta", true, "delta", true);

        assertThat(filter.test("alpha")).isFalse();
        assertThat(filter.test("beta")).isTrue();
        assertThat(filter.test("charlie")).isTrue();
        assertThat(filter.test("delta")).isTrue();
        assertThat(filter.test("epsilon")).isFalse();
    }

    // -------------------------------------------------------------------------
    //  Custom comparator
    // -------------------------------------------------------------------------

    @Test
    void customComparatorIsUsed() {
        // Orders strings by length — clearly not the natural String order.
        SavepointKeyFilter<String> filter =
                SavepointKeyFilter.range(
                        "aa", true, "cccc", true, (a, b) -> Integer.compare(a.length(), b.length()));

        // Length in [2, 4]: "abc" (3) passes; "a" (1) and "ccccc" (5) fail.
        assertThat(filter.test("abc")).isTrue();
        assertThat(filter.test("a")).isFalse();
        assertThat(filter.test("ccccc")).isFalse();
    }

    @Test
    void descendingComparatorMakesInvertedBoundsValid() {
        // Under a descending comparator, [6, 3] covers 3, 4, 5 and 6.
        SavepointKeyFilter<Integer> filter =
                SavepointKeyFilter.range(6, true, 3, true, (a, b) -> Integer.compare(b, a));

        assertThat(filter.test(2)).isFalse();
        assertThat(filter.test(3)).isTrue();
        assertThat(filter.test(6)).isTrue();
        assertThat(filter.test(7)).isFalse();
    }

    // -------------------------------------------------------------------------
    //  Serialization and toString
    // -------------------------------------------------------------------------

    @Test
    void survivesSerialization() throws Exception {
        // The filter is shipped with the job, so it must round-trip unchanged.
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.range(5L, true, 10L, false);
        SavepointKeyFilter<Long> copy = InstantiationUtil.clone(filter);

        assertThat(copy.test(4L)).isFalse();
        assertThat(copy.test(5L)).isTrue();
        assertThat(copy.test(10L)).isFalse();
    }

    @Test
    void survivesSerializationWithACustomComparator() throws Exception {
        // A lambda assigned to SerializableComparator is serializable too.
        SerializableComparator<Integer> descending = (a, b) -> Integer.compare(b, a);
        SavepointKeyFilter<Integer> filter = SavepointKeyFilter.range(6, true, 3, true, descending);
        SavepointKeyFilter<Integer> copy = InstantiationUtil.clone(filter);

        assertThat(copy.test(4)).isTrue();
        assertThat(copy.test(7)).isFalse();
    }

    @Test
    void toStringRendersTheBounds() {
        assertThat(SavepointKeyFilter.range(5L, true, 10L, false))
                .hasToString("RangeKeyFilter[5, 10)");
        assertThat(SavepointKeyFilter.range(null, true, 10L, true))
                .hasToString("RangeKeyFilter(-∞, 10]");
        assertThat(SavepointKeyFilter.range(5L, false, null, true))
                .hasToString("RangeKeyFilter(5, +∞)");
    }
}

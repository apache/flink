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

package org.apache.flink.table.planner.plan.utils;

import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UpsertKeyUtil}. */
class UpsertKeyUtilTest {
    private final int[] emptyKey = new int[0];

    @Test
    void testSmallestKey() {
        assertThat(UpsertKeyUtil.getSmallestKey(null)).isEqualTo(emptyKey);
        assertThat(UpsertKeyUtil.getSmallestKey(new HashSet<>())).isEqualTo(emptyKey);

        ImmutableBitSet smallestKey = ImmutableBitSet.of(0, 1);
        ImmutableBitSet middleKey = ImmutableBitSet.of(0, 2);
        ImmutableBitSet longKey = ImmutableBitSet.of(0, 1, 2);

        Set<ImmutableBitSet> upsertKeys = new HashSet<>();
        upsertKeys.add(smallestKey);
        upsertKeys.add(middleKey);
        assertThat(UpsertKeyUtil.getSmallestKey(upsertKeys)).isEqualTo(smallestKey.toArray());

        upsertKeys.clear();
        upsertKeys.add(smallestKey);
        upsertKeys.add(longKey);
        assertThat(UpsertKeyUtil.getSmallestKey(upsertKeys)).isEqualTo(smallestKey.toArray());
    }

    @Test
    void testIsContainedInAnyUpsertKey() {
        ImmutableBitSet refOn0 = ImmutableBitSet.of(0);

        assertThat(UpsertKeyUtil.isContainedInAnyUpsertKey(null, refOn0)).isFalse();
        assertThat(UpsertKeyUtil.isContainedInAnyUpsertKey(Set.of(), refOn0)).isFalse();
        assertThat(UpsertKeyUtil.isContainedInAnyUpsertKey(Set.of(ImmutableBitSet.of(0)), refOn0))
                .isTrue();
        assertThat(UpsertKeyUtil.isContainedInAnyUpsertKey(Set.of(ImmutableBitSet.of(2)), refOn0))
                .isFalse();
        assertThat(
                        UpsertKeyUtil.isContainedInAnyUpsertKey(
                                Set.of(ImmutableBitSet.of(0, 1)), refOn0))
                .isTrue();
        assertThat(
                        UpsertKeyUtil.isContainedInAnyUpsertKey(
                                Set.of(ImmutableBitSet.of(1), ImmutableBitSet.of(0)), refOn0))
                .isTrue();
    }

    @Test
    void testRemapFieldRefsThroughProjection() {
        // projectedFields[i] holds the original physical column index(es) kept at projected
        // position i. A length-1 entry is a top-level column; longer entries are nested sub-fields.

        // identity projection: original indices map to themselves
        assertThat(
                        UpsertKeyUtil.remapFieldRefsThroughProjection(
                                ImmutableBitSet.of(0, 2), new int[][] {{0}, {1}, {2}}))
                .hasValue(ImmutableBitSet.of(0, 2));

        // reordering projection SELECT b, c, a from (a=0, b=1, c=2): a -> projected 2, c -> 1
        int[][] reordered = {{1}, {2}, {0}};
        assertThat(UpsertKeyUtil.remapFieldRefsThroughProjection(ImmutableBitSet.of(0), reordered))
                .hasValue(ImmutableBitSet.of(2));
        assertThat(UpsertKeyUtil.remapFieldRefsThroughProjection(ImmutableBitSet.of(2), reordered))
                .hasValue(ImmutableBitSet.of(1));

        // dropped column: original index 2 is not kept by the projection -> no mapping
        int[][] dropsCol2 = {{0}, {1}};
        assertThat(UpsertKeyUtil.remapFieldRefsThroughProjection(ImmutableBitSet.of(2), dropsCol2))
                .isEmpty();

        // column kept only as a nested sub-field (path length != 1) -> treated as not top-level
        assertThat(
                        UpsertKeyUtil.remapFieldRefsThroughProjection(
                                ImmutableBitSet.of(1), new int[][] {{0}, {1, 0}}))
                .isEmpty();

        // all-or-nothing: one ref maps, the other is dropped -> empty
        assertThat(
                        UpsertKeyUtil.remapFieldRefsThroughProjection(
                                ImmutableBitSet.of(0, 2), dropsCol2))
                .isEmpty();

        // no refs -> empty result
        assertThat(
                        UpsertKeyUtil.remapFieldRefsThroughProjection(
                                ImmutableBitSet.of(), new int[][] {{0}, {1}}))
                .hasValue(ImmutableBitSet.of());
    }
}

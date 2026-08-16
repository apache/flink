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

import org.apache.flink.state.api.filter.SavepointKeyFilter;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SavepointKeyFilterPlanTest {

    // -------------------------------------------------------------------------
    //  Range intersection
    // -------------------------------------------------------------------------

    @Test
    void intersectNarrowsBounds() {
        // [5, ∞) ∩ (-∞, 10] = [5, 10]
        SavepointKeyFilterPlan<Long> lower = SavepointKeyFilterPlan.range(5L, true, null, true);
        SavepointKeyFilterPlan<Long> upper = SavepointKeyFilterPlan.range(null, true, 10L, true);
        SavepointKeyFilterPlan<Long> result = lower.intersect(upper);

        assertThat(result.isEmpty()).isFalse();
        assertThat(result.getExactKeys()).isNull();
        assertThat(result.test(4L)).isFalse();
        assertThat(result.test(5L)).isTrue();
        assertThat(result.test(10L)).isTrue();
        assertThat(result.test(11L)).isFalse();
    }

    @Test
    void intersectDisjointRangesReturnsEmpty() {
        // [10, ∞) ∩ (-∞, 5] — disjoint
        SavepointKeyFilterPlan<Long> a = SavepointKeyFilterPlan.range(10L, true, null, true);
        SavepointKeyFilterPlan<Long> b = SavepointKeyFilterPlan.range(null, true, 5L, true);
        assertThat(a.intersect(b).isEmpty()).isTrue();
    }

    @Test
    void intersectEqualBoundsInclusiveIsNonEmpty() {
        // [7, ∞) ∩ (-∞, 7] = [7, 7]
        SavepointKeyFilterPlan<Long> a = SavepointKeyFilterPlan.range(7L, true, null, true);
        SavepointKeyFilterPlan<Long> b = SavepointKeyFilterPlan.range(null, true, 7L, true);
        SavepointKeyFilterPlan<Long> result = a.intersect(b);
        assertThat(result.isEmpty()).isFalse();
        assertThat(result.test(7L)).isTrue();
        assertThat(result.test(6L)).isFalse();
        assertThat(result.test(8L)).isFalse();
    }

    @Test
    void intersectEqualBoundsOneExclusiveIsEmpty() {
        // (7, ∞) ∩ (-∞, 7] — empty because lower is exclusive
        SavepointKeyFilterPlan<Long> a = SavepointKeyFilterPlan.range(7L, false, null, true);
        SavepointKeyFilterPlan<Long> b = SavepointKeyFilterPlan.range(null, true, 7L, true);
        assertThat(a.intersect(b).isEmpty()).isTrue();
    }

    // -------------------------------------------------------------------------
    //  Custom comparator
    // -------------------------------------------------------------------------

    @Test
    void rangeWithCustomComparatorIsUsed() {
        // Orders strings by length — clearly not the natural String order.
        SavepointKeyFilter<String> filter =
                SavepointKeyFilterPlan.range(
                        "aa",
                        true,
                        "cccc",
                        true,
                        (a, b) -> Integer.compare(a.length(), b.length()));

        // Length in [2, 4]: "abc" (3), passes; "a" (1) and "ccccc" (5), fail.
        assertThat(filter.test("abc")).isTrue();
        assertThat(filter.test("a")).isFalse();
        assertThat(filter.test("ccccc")).isFalse();
    }

    // -------------------------------------------------------------------------
    //  Empty key filter
    // -------------------------------------------------------------------------

    @Test
    void emptyKeyFilter_rejectsEverything() {
        // empty -> matches nothing, of any key type
        SavepointKeyFilterPlan<Object> empty = SavepointKeyFilterPlan.empty();
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.getExactKeys()).isEmpty();
        assertThat(empty.test(42L)).isFalse();
        assertThat(empty.test("hello")).isFalse();
    }

    @Test
    void exactWithEmptySetReturnsEmptyKeyFilter() {
        // exact({}) -> collapses to empty
        SavepointKeyFilterPlan<Object> filter =
                SavepointKeyFilterPlan.exact(Collections.emptySet());
        assertThat(filter.isEmpty()).isTrue();
    }

    @Test
    void emptyKeyFilterSingletonPreservedAcrossSerialization() throws Exception {
        // empty is a singleton, so readResolve must return the same instance
        SavepointKeyFilterPlan<Object> original = SavepointKeyFilterPlan.empty();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        Object deserialized;
        try (ObjectInputStream ois =
                     new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            deserialized = ois.readObject();
        }
        assertThat(deserialized).isSameAs(SavepointKeyFilterPlan.empty());
    }

    // -------------------------------------------------------------------------
    //  Exact key filter — single-value factory
    // -------------------------------------------------------------------------

    @Test
    void exactSingleValueFactory() {
        // exact(42) -> {42}
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(42L);
        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(43L)).isFalse();
    }

}

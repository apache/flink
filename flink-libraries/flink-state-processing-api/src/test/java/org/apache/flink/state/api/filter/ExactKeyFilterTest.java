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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ExactKeyFilter}, built through {@link SavepointKeyFilter#exact}. */
class ExactKeyFilterTest {

    @Test
    void singleValueFactoryMatchesOnlyThatKey() {
        // exact(42) -> {42}
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(42L);

        assertThat(filter.getExactKeys()).containsExactly(42L);
        assertThat(filter.test(42L)).isTrue();
        assertThat(filter.test(43L)).isFalse();
    }

    @Test
    void setFactoryMatchesEveryKeyInTheSet() {
        // exact({1, 2, 3}) -> membership test
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(Set.of(1L, 2L, 3L));

        assertThat(filter.getExactKeys()).containsExactlyInAnyOrder(1L, 2L, 3L);
        assertThat(filter.test(1L)).isTrue();
        assertThat(filter.test(2L)).isTrue();
        assertThat(filter.test(3L)).isTrue();
        assertThat(filter.test(0L)).isFalse();
        assertThat(filter.test(4L)).isFalse();
    }

    @Test
    void emptySetMatchesNothing() {
        // exact({}) -> the scan can be pruned entirely
        SavepointKeyFilter<Object> filter = SavepointKeyFilter.exact(Collections.emptySet());

        assertThat(filter.getExactKeys()).isEmpty();
        assertThat(filter.test(42L)).isFalse();
        assertThat(filter.test("hello")).isFalse();
    }

    @Test
    void keysAreCopiedDefensively() {
        // Mutating the source set after construction must not change the filter.
        Set<Long> keys = new HashSet<>(Set.of(1L, 2L));
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(keys);

        keys.add(3L);

        assertThat(filter.getExactKeys()).containsExactlyInAnyOrder(1L, 2L);
        assertThat(filter.test(3L)).isFalse();
    }

    @Test
    void exactKeysAreUnmodifiable() {
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(Set.of(1L));

        assertThatThrownBy(() -> filter.getExactKeys().add(2L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void survivesSerialization() throws Exception {
        // The filter is shipped with the job, so it must round-trip unchanged.
        SavepointKeyFilter<Long> filter = SavepointKeyFilter.exact(Set.of(1L, 2L));
        SavepointKeyFilter<Long> copy = InstantiationUtil.clone(filter);

        assertThat(copy.getExactKeys()).containsExactlyInAnyOrder(1L, 2L);
        assertThat(copy.test(1L)).isTrue();
        assertThat(copy.test(3L)).isFalse();
    }

    @Test
    void toStringListsTheKeys() {
        assertThat(SavepointKeyFilter.exact(7L)).hasToString("ExactKeyFilter[7]");
    }
}

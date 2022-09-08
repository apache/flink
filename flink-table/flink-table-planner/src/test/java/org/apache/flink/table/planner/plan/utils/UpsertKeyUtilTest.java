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
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UpsertKeyUtil}. */
public class UpsertKeyUtilTest {
    private final int[] emptyKey = new int[0];

    @Test
    public void testSmallestKey() {
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
}

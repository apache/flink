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

package org.apache.flink.table.plan.stats;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TableStats}. */
class TableStatsTest {

    @Test
    void testMerge() {
        Map<String, ColumnStats> colStats1 = new HashMap<>();
        colStats1.put("a", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        TableStats stats1 = new TableStats(30, colStats1);

        Map<String, ColumnStats> colStats2 = new HashMap<>();
        colStats2.put("a", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        TableStats stats2 = new TableStats(32, colStats2);

        Map<String, ColumnStats> colStatsMerge = new HashMap<>();
        colStatsMerge.put("a", new ColumnStats(4L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2)).isEqualTo(new TableStats(62, colStatsMerge));
    }

    @Test
    void testMergeLackColumnStats() {
        Map<String, ColumnStats> colStats1 = new HashMap<>();
        colStats1.put("a", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        colStats1.put("b", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        TableStats stats1 = new TableStats(30, colStats1);

        Map<String, ColumnStats> colStats2 = new HashMap<>();
        colStats2.put("a", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        TableStats stats2 = new TableStats(32, colStats2);

        Map<String, ColumnStats> colStatsMerge = new HashMap<>();
        colStatsMerge.put("a", new ColumnStats(4L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2)).isEqualTo(new TableStats(62, colStatsMerge));
    }

    @Test
    void testMergeUnknownRowCount() {
        TableStats stats1 = new TableStats(-1, new HashMap<>());
        TableStats stats2 = new TableStats(32, new HashMap<>());
        assertThat(stats1.merge(stats2)).isEqualTo(new TableStats(-1, new HashMap<>()));

        stats1 = new TableStats(-1, new HashMap<>());
        stats2 = new TableStats(-1, new HashMap<>());
        assertThat(stats1.merge(stats2)).isEqualTo(new TableStats(-1, new HashMap<>()));

        stats1 = new TableStats(-3, new HashMap<>());
        stats2 = new TableStats(-2, new HashMap<>());
        assertThat(stats1.merge(stats2)).isEqualTo(new TableStats(-1, new HashMap<>()));
    }

    @Test
    void testMergeColumnStatsUnknown() {
        ColumnStats columnStats0 = new ColumnStats(4L, 5L, 2D, 3, 15, 2);
        ColumnStats columnStats1 = new ColumnStats(4L, null, 2D, 3, 15, 2);
        ColumnStats columnStats2 = new ColumnStats(4L, 5L, 2D, null, 15, 2);
        ColumnStats columnStats3 = new ColumnStats(null, 5L, 2D, 3, 15, 2);
        ColumnStats columnStats4 = new ColumnStats(4L, 5L, 2D, 3, null, 2);
        ColumnStats columnStats5 = new ColumnStats(4L, 5L, 2D, 3, 15, null);
        ColumnStats columnStats6 = new ColumnStats(4L, 5L, null, 3, 15, 2);

        assertThat(columnStats0.merge(columnStats1))
                .isEqualTo(new ColumnStats(4L, null, 2D, 3, 15, 2));
        assertThat(columnStats0.merge(columnStats2))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, null, 15, 2));
        assertThat(columnStats0.merge(columnStats3))
                .isEqualTo(new ColumnStats(null, 10L, 2D, 3, 15, 2));
        assertThat(columnStats0.merge(columnStats4))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, 3, null, 2));
        assertThat(columnStats0.merge(columnStats5))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, 3, 15, null));
        assertThat(columnStats0.merge(columnStats6))
                .isEqualTo(new ColumnStats(4L, 10L, null, 3, 15, 2));
        assertThat(columnStats6.merge(columnStats6))
                .isEqualTo(new ColumnStats(4L, 10L, null, 3, 15, 2));
    }
}

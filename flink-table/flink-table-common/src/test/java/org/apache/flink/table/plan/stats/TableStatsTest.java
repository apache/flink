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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
        assertThat(stats1.merge(stats2, null)).isEqualTo(new TableStats(62, colStatsMerge));

        Map<String, ColumnStats> colStatsMerge2 = new HashMap<>();
        colStatsMerge2.put("a", new ColumnStats(4L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2, new HashSet<>()))
                .isEqualTo(new TableStats(62, colStatsMerge2));

        // test column stats merge while column 'a' is partition key. Merged Ndv for columns which
        // are partition keys using sum instead of max.
        Map<String, ColumnStats> colStatsMerge3 = new HashMap<>();
        colStatsMerge3.put("a", new ColumnStats(7L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2, new HashSet<>(Collections.singletonList("a"))))
                .isEqualTo(new TableStats(62, colStatsMerge3));

        Map<String, ColumnStats> colStats3 = new HashMap<>();
        colStats3.put("a", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        colStats3.put("b", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        stats1 = new TableStats(30, colStats3);
        Map<String, ColumnStats> colStats4 = new HashMap<>();
        colStats4.put("a", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        colStats4.put("b", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        stats2 = new TableStats(32, colStats4);

        Map<String, ColumnStats> colStatsMerge4 = new HashMap<>();
        colStatsMerge4.put("a", new ColumnStats(7L, 20L, 7D, 23, 35, 2));
        colStatsMerge4.put("b", new ColumnStats(4L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2, new HashSet<>(Collections.singletonList("a"))))
                .isEqualTo(new TableStats(62, colStatsMerge4));

        // test merge with one side is TableStats.UNKNOWN.
        stats2 = TableStats.UNKNOWN;
        assertThat(stats1.merge(stats2, null)).isEqualTo(TableStats.UNKNOWN);

        // test merge with one side have no column stats.
        stats2 = new TableStats(32);
        assertThat(stats1.merge(stats2, null)).isEqualTo(new TableStats(62));
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
        assertThat(stats1.merge(stats2, null)).isEqualTo(new TableStats(62, colStatsMerge));

        Map<String, ColumnStats> colStatsMerge2 = new HashMap<>();
        colStatsMerge2.put("a", new ColumnStats(4L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2, new HashSet<>()))
                .isEqualTo(new TableStats(62, colStatsMerge2));

        // test column stats merge while column 'a' is partition key. Merged Ndv for columns which
        // are partition keys using sum instead of max.
        Map<String, ColumnStats> colStatsMerge3 = new HashMap<>();
        colStatsMerge3.put("a", new ColumnStats(7L, 20L, 7D, 23, 35, 2));
        assertThat(stats1.merge(stats2, new HashSet<>(Collections.singletonList("a"))))
                .isEqualTo(new TableStats(62, colStatsMerge3));
    }

    @Test
    void testMergeUnknownRowCount() {
        TableStats stats1 = new TableStats(-1, new HashMap<>());
        TableStats stats2 = new TableStats(32, new HashMap<>());
        assertThat(stats1.merge(stats2, null)).isEqualTo(TableStats.UNKNOWN);

        stats1 = new TableStats(-1, new HashMap<>());
        stats2 = new TableStats(-1, new HashMap<>());
        assertThat(stats1.merge(stats2, null)).isEqualTo(TableStats.UNKNOWN);

        stats1 = new TableStats(-3, new HashMap<>());
        stats2 = new TableStats(-2, new HashMap<>());
        assertThat(stats1.merge(stats2, null)).isEqualTo(TableStats.UNKNOWN);
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

        assertThat(columnStats0.merge(columnStats1, false))
                .isEqualTo(new ColumnStats(4L, null, 2D, 3, 15, 2));
        assertThat(columnStats0.merge(columnStats2, false))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, null, 15, 2));
        assertThat(columnStats0.merge(columnStats3, false))
                .isEqualTo(new ColumnStats(null, 10L, 2D, 3, 15, 2));
        assertThat(columnStats0.merge(columnStats4, false))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, 3, null, 2));
        assertThat(columnStats0.merge(columnStats5, false))
                .isEqualTo(new ColumnStats(4L, 10L, 2D, 3, 15, null));
        assertThat(columnStats0.merge(columnStats6, false))
                .isEqualTo(new ColumnStats(4L, 10L, null, 3, 15, 2));
        assertThat(columnStats6.merge(columnStats6, false))
                .isEqualTo(new ColumnStats(4L, 10L, null, 3, 15, 2));

        // tet column stats merge while partition key is true.
        assertThat(columnStats0.merge(columnStats1, true))
                .isEqualTo(new ColumnStats(8L, null, 2D, 3, 15, 2));
        assertThat(columnStats0.merge(columnStats3, true))
                .isEqualTo(new ColumnStats(null, 10L, 2D, 3, 15, 2));
    }
}

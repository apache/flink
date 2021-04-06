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

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/** Test for {@link TableStats}. */
public class TableStatsTest {

    @Test
    public void testMerge() {
        Map<String, ColumnStats> colStats1 = new HashMap<>();
        colStats1.put("a", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        TableStats stats1 = new TableStats(30, colStats1);

        Map<String, ColumnStats> colStats2 = new HashMap<>();
        colStats2.put("a", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        TableStats stats2 = new TableStats(32, colStats2);

        Map<String, ColumnStats> colStatsMerge = new HashMap<>();
        colStatsMerge.put("a", new ColumnStats(7L, 20L, 7D, 23, 35, 2));
        Assert.assertEquals(new TableStats(62, colStatsMerge), stats1.merge(stats2));
    }

    @Test
    public void testMergeLackColumnStats() {
        Map<String, ColumnStats> colStats1 = new HashMap<>();
        colStats1.put("a", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        colStats1.put("b", new ColumnStats(4L, 5L, 2D, 3, 15, 2));
        TableStats stats1 = new TableStats(30, colStats1);

        Map<String, ColumnStats> colStats2 = new HashMap<>();
        colStats2.put("a", new ColumnStats(3L, 15L, 12D, 23, 35, 6));
        TableStats stats2 = new TableStats(32, colStats2);

        Map<String, ColumnStats> colStatsMerge = new HashMap<>();
        colStatsMerge.put("a", new ColumnStats(7L, 20L, 7D, 23, 35, 2));
        Assert.assertEquals(new TableStats(62, colStatsMerge), stats1.merge(stats2));
    }

    @Test
    public void testMergeUnknownRowCount() {
        TableStats stats1 = new TableStats(-1, new HashMap<>());
        TableStats stats2 = new TableStats(32, new HashMap<>());
        Assert.assertEquals(new TableStats(-1, new HashMap<>()), stats1.merge(stats2));

        stats1 = new TableStats(-1, new HashMap<>());
        stats2 = new TableStats(-1, new HashMap<>());
        Assert.assertEquals(new TableStats(-1, new HashMap<>()), stats1.merge(stats2));

        stats1 = new TableStats(-3, new HashMap<>());
        stats2 = new TableStats(-2, new HashMap<>());
        Assert.assertEquals(new TableStats(-1, new HashMap<>()), stats1.merge(stats2));
    }

    @Test
    public void testMergeColumnStatsUnknown() {
        ColumnStats columnStats0 = new ColumnStats(4L, 5L, 2D, 3, 15, 2);
        ColumnStats columnStats1 = new ColumnStats(4L, null, 2D, 3, 15, 2);
        ColumnStats columnStats2 = new ColumnStats(4L, 5L, 2D, null, 15, 2);
        ColumnStats columnStats3 = new ColumnStats(null, 5L, 2D, 3, 15, 2);
        ColumnStats columnStats4 = new ColumnStats(4L, 5L, 2D, 3, null, 2);
        ColumnStats columnStats5 = new ColumnStats(4L, 5L, 2D, 3, 15, null);
        ColumnStats columnStats6 = new ColumnStats(4L, 5L, null, 3, 15, 2);

        Assert.assertEquals(
                new ColumnStats(8L, null, 2D, 3, 15, 2), columnStats0.merge(columnStats1));
        Assert.assertEquals(
                new ColumnStats(8L, 10L, 2D, null, 15, 2), columnStats0.merge(columnStats2));
        Assert.assertEquals(
                new ColumnStats(null, 10L, 2D, 3, 15, 2), columnStats0.merge(columnStats3));
        Assert.assertEquals(
                new ColumnStats(8L, 10L, 2D, 3, null, 2), columnStats0.merge(columnStats4));
        Assert.assertEquals(
                new ColumnStats(8L, 10L, 2D, 3, 15, null), columnStats0.merge(columnStats5));
        Assert.assertEquals(
                new ColumnStats(8L, 10L, null, 3, 15, 2), columnStats0.merge(columnStats6));
        Assert.assertEquals(
                new ColumnStats(8L, 10L, null, 3, 15, 2), columnStats6.merge(columnStats6));
    }
}

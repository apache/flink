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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.plan.stats.ColumnStats;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link CatalogTableStatisticsConverter}. */
public class CatalogTableStatisticsConverterTest {

    @Test
    public void testConvertToColumnStatsMapWithNullColumnStatisticsData() {
        Map<String, CatalogColumnStatisticsDataBase> columnStatisticsDataBaseMap = new HashMap<>();
        columnStatisticsDataBaseMap.put(
                "first", new CatalogColumnStatisticsDataString(10L, 5.2, 3L, 100L));
        columnStatisticsDataBaseMap.put("second", null);
        Map<String, ColumnStats> columnStatsMap =
                CatalogTableStatisticsConverter.convertToColumnStatsMap(
                        columnStatisticsDataBaseMap);
        assertNotNull(columnStatsMap);
        assertEquals(columnStatisticsDataBaseMap.size() - 1, columnStatsMap.size());
        assertTrue(columnStatsMap.containsKey("first"));
        assertFalse(columnStatsMap.containsKey("second"));
    }
}

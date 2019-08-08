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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.StatisticGeneratorTest;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToTableStats;
import static org.apache.flink.table.planner.utils.TableStatsConverter.AVG_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_LEN;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MAX_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.MIN_VALUE;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NDV;
import static org.apache.flink.table.planner.utils.TableStatsConverter.NULL_COUNT;
import static org.apache.flink.table.planner.utils.TableStatsConverter.convertToCatalogColumnStatistics;
import static org.apache.flink.table.planner.utils.TableStatsConverter.convertToCatalogTableStatistics;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link TableStatsConverter}.
 */
public class TableStatsConverterTest {

	@Test
	public void testConvertToCatalogTableStatistics_UNKNOWN() {
		assertEquals(CatalogTableStatistics.UNKNOWN, convertToCatalogTableStatistics(TableStats.UNKNOWN));
	}

	@Test
	public void testConvertToCatalogTableStatistics() {
		CatalogTableStatistics statistics = convertToCatalogTableStatistics(genTableStats(true));
		assertEquals(1000L, statistics.getRowCount());
		assertEquals(0L, statistics.getFileCount());
		assertEquals(0L, statistics.getTotalSize());
		assertEquals(0L, statistics.getRawDataSize());
	}

	@Test
	public void testConvertToCatalogColumnStatistics_UNKNOWN() {
		assertEquals(CatalogColumnStatistics.UNKNOWN, convertToCatalogColumnStatistics(TableStats.UNKNOWN));
	}

	@Test
	public void testConvertToCatalogColumnStatistics() {
		CatalogColumnStatistics statistics = convertToCatalogColumnStatistics(genTableStats(true));
		Map<String, CatalogColumnStatisticsDataBase> statisticsData = statistics.getColumnStatisticsData();
		assertEquals(9, statisticsData.size());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "2");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "1.0");
			put(MAX_LEN, "1");
		}}, statisticsData.get("boolean").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "10");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "1.0");
			put(MAX_LEN, "1");
			put(MAX_VALUE, "10");
			put(MIN_VALUE, "1");
		}}, statisticsData.get("byte").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "30");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "2.0");
			put(MAX_LEN, "2");
			put(MAX_VALUE, "50");
			put(MIN_VALUE, "-10");
		}}, statisticsData.get("short").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NULL_COUNT, "10");
			put(AVG_LEN, "4.0");
			put(MAX_LEN, "4");
			put(MAX_VALUE, "100");
			put(MIN_VALUE, "10");
		}}, statisticsData.get("int").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "50");
			put(NULL_COUNT, "0");
			put(AVG_LEN, "8.0");
			put(MAX_LEN, "8");
			put(MAX_VALUE, "100");
			put(MIN_VALUE, "10");
		}}, statisticsData.get("long").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "4.0");
			put(MAX_LEN, "4");
			put(MAX_VALUE, "5.0");
			put(MIN_VALUE, "2.0");
		}}, statisticsData.get("float").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "8.0");
			put(MAX_LEN, "8");
			put(MAX_VALUE, "25.0");
			put(MIN_VALUE, "6.0");
		}}, statisticsData.get("double").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(AVG_LEN, "12.0");
			put(MAX_LEN, "12");
			put(MAX_VALUE, "64.15");
			put(MIN_VALUE, "4.05");
		}}, statisticsData.get("decimal").getProperties());
		assertEquals(new HashMap<String, String>() {{
			put(NDV, "1000");
			put(NULL_COUNT, "10");
			put(AVG_LEN, "12.0");
			put(MAX_LEN, "12");
		}}, statisticsData.get("date").getProperties());
	}

	@Test
	public void testConvertBetweenTableStatsAndCatalogStatistics() {
		TableStats tableStats = genTableStats(true);
		CatalogTableStatistics tableStatistics = convertToCatalogTableStatistics(tableStats);
		CatalogColumnStatistics columnStatistics = convertToCatalogColumnStatistics(tableStats);
		TableSchema tableSchema = TableSchema.builder()
				.field("boolean", DataTypes.BOOLEAN())
				.field("byte", DataTypes.TINYINT())
				.field("short", DataTypes.SMALLINT())
				.field("int", DataTypes.INT())
				.field("long", DataTypes.BIGINT())
				.field("float", DataTypes.FLOAT())
				.field("double", DataTypes.DOUBLE())
				.field("decimal", DataTypes.DECIMAL(5, 2))
				.field("date", DataTypes.DATE())
				.field("unknown_col", DataTypes.STRING())
				.build();

		TableStats expected = genTableStats(false);
		TableStats actual = convertToTableStats(tableStatistics, columnStatistics, tableSchema);
		StatisticGeneratorTest.assertTableStatsEquals(expected, actual);
	}

	private TableStats genTableStats(boolean addUnknownColumn) {
		Map<String, ColumnStats> columnStatsMap = new HashMap<>();
		columnStatsMap.put("boolean", new ColumnStats(2L, 0L, 1.0, 1, null, null));
		columnStatsMap.put("byte", new ColumnStats(10L, 0L, 1.0, 1, (byte) 10, (byte) 1));
		columnStatsMap.put("short", new ColumnStats(30L, 0L, 2.0, 2, (short) 50, (short) -10));
		columnStatsMap.put("int", new ColumnStats(null, 10L, 4.0, 4, 100, 10));
		columnStatsMap.put("long", new ColumnStats(50L, 0L, 8.0, 8, 100L, 10L));
		columnStatsMap.put("float", new ColumnStats(null, null, 4.0, 4, 5.0f, 2.0f));
		columnStatsMap.put("double", new ColumnStats(null, null, 8.0, 8, 25.0d, 6.0d));
		columnStatsMap.put("decimal", new ColumnStats(null, null, 12.0, 12,
				Decimal.castFrom("64.15", 5, 2).toBigDecimal(), Decimal.castFrom("4.05", 5, 2).toBigDecimal()));
		columnStatsMap.put("date", new ColumnStats(1000L, 10L, 12.0, 12, null, null));
		if (addUnknownColumn) {
			columnStatsMap.put("unknown_col", new ColumnStats(null, null, null, null, null, null));
		}
		return new TableStats(1000L, columnStatsMap);
	}
}

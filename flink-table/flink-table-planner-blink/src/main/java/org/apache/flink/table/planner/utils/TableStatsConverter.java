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

import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.GenericCatalogColumnStatisticsData;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting {@link TableStats} to {@link CatalogTableStatistics} and {@link CatalogColumnStatistics}.
 */
public class TableStatsConverter {

	public static final String NDV = "ndv";
	public static final String NULL_COUNT = "null_count";
	public static final String MAX_LEN = "max_len";
	public static final String AVG_LEN = "avg_len";
	public static final String MAX_VALUE = "max_value";
	public static final String MIN_VALUE = "min_value";

	public static CatalogTableStatistics convertToCatalogTableStatistics(TableStats tableStats) {
		if (tableStats == null || tableStats.equals(TableStats.UNKNOWN)) {
			return CatalogTableStatistics.UNKNOWN;
		}

		long rowCount = tableStats.getRowCount();
		return new CatalogTableStatistics(rowCount, 0, 0, 0);
	}

	public static CatalogColumnStatistics convertToCatalogColumnStatistics(TableStats tableStats) {
		if (tableStats == null || tableStats.equals(TableStats.UNKNOWN) || tableStats.getColumnStats().isEmpty()) {
			return CatalogColumnStatistics.UNKNOWN;
		}

		Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData = new HashMap<>();
		for (Map.Entry<String, ColumnStats> entry : tableStats.getColumnStats().entrySet()) {
			String fieldName = entry.getKey();
			ColumnStats columnStats = entry.getValue();
			CatalogColumnStatisticsDataBase statisticsData = convertToCatalogColumnStatisticsData(columnStats);
			if (statisticsData != null) {
				columnStatisticsData.put(fieldName, statisticsData);
			}
		}

		return new CatalogColumnStatistics(columnStatisticsData);
	}

	private static GenericCatalogColumnStatisticsData convertToCatalogColumnStatisticsData(ColumnStats columnStats) {
		Map<String, String> properties = new HashMap<>();
		if (columnStats.getNdv() != null) {
			properties.put(NDV, columnStats.getNdv().toString());
		}
		if (columnStats.getNullCount() != null) {
			properties.put(NULL_COUNT, columnStats.getNullCount().toString());
		}
		if (columnStats.getMaxLen() != null) {
			properties.put(MAX_LEN, columnStats.getMaxLen().toString());
		}
		if (columnStats.getAvgLen() != null) {
			properties.put(AVG_LEN, columnStats.getAvgLen().toString());
		}
		if (columnStats.getMaxValue() != null) {
			properties.put(MAX_VALUE, columnStats.getMaxValue().toString());
		}
		if (columnStats.getMinValue() != null) {
			properties.put(MIN_VALUE, columnStats.getMinValue().toString());
		}
		if (properties.isEmpty()) {
			return null;
		} else {
			return new GenericCatalogColumnStatisticsData(properties);
		}
	}

}

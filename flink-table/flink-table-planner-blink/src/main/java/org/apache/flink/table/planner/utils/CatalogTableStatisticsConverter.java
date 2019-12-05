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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import org.apache.calcite.avatica.util.DateTimeUtils;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting {@link CatalogTableStatistics} and {@link CatalogColumnStatistics} to {@link TableStats}.
 */
public class CatalogTableStatisticsConverter {

	public static TableStats convertToTableStats(
			CatalogTableStatistics tableStatistics,
			CatalogColumnStatistics columnStatistics) {
		long rowCount;
		if (tableStatistics != null && tableStatistics.getRowCount() >= 0) {
			rowCount = tableStatistics.getRowCount();
		} else {
			rowCount = TableStats.UNKNOWN.getRowCount();
		}

		Map<String, ColumnStats> columnStatsMap;
		if (columnStatistics != null) {
			columnStatsMap = convertToColumnStatsMap(columnStatistics.getColumnStatisticsData());
		} else {
			columnStatsMap = new HashMap<>();
		}
		return new TableStats(rowCount, columnStatsMap);
	}

	private static Map<String, ColumnStats> convertToColumnStatsMap(
			Map<String, CatalogColumnStatisticsDataBase> columnStatisticsData) {
		Map<String, ColumnStats> columnStatsMap = new HashMap<>();
		for (Map.Entry<String, CatalogColumnStatisticsDataBase> entry : columnStatisticsData.entrySet()) {
			ColumnStats columnStats = convertToColumnStats(entry.getValue());
			columnStatsMap.put(entry.getKey(), columnStats);
		}
		return columnStatsMap;
	}

	private static ColumnStats convertToColumnStats(
			CatalogColumnStatisticsDataBase columnStatisticsData) {
		Long ndv = null;
		Long nullCount = columnStatisticsData.getNullCount();
		Double avgLen = null;
		Integer maxLen = null;
		Comparable<?> max = null;
		Comparable<?> min = null;
		if (columnStatisticsData instanceof CatalogColumnStatisticsDataBoolean) {
			CatalogColumnStatisticsDataBoolean booleanData = (CatalogColumnStatisticsDataBoolean) columnStatisticsData;
			avgLen = 1.0;
			maxLen = 1;
			if (null == booleanData.getFalseCount() || null == booleanData.getTrueCount()) {
				ndv = 2L;
			} else if ((booleanData.getFalseCount() == 0 && booleanData.getTrueCount() > 0) ||
					(booleanData.getFalseCount() > 0 && booleanData.getTrueCount() == 0)) {
				ndv = 1L;
			} else {
				ndv = 2L;
			}
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataLong) {
			CatalogColumnStatisticsDataLong longData = (CatalogColumnStatisticsDataLong) columnStatisticsData;
			ndv = longData.getNdv();
			avgLen = 8.0;
			maxLen = 8;
			max = longData.getMax();
			min = longData.getMin();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataDouble) {
			CatalogColumnStatisticsDataDouble doubleData = (CatalogColumnStatisticsDataDouble) columnStatisticsData;
			ndv = doubleData.getNdv();
			avgLen = 8.0;
			maxLen = 8;
			max = doubleData.getMax();
			min = doubleData.getMin();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataString) {
			CatalogColumnStatisticsDataString strData = (CatalogColumnStatisticsDataString) columnStatisticsData;
			ndv = strData.getNdv();
			avgLen = strData.getAvgLength();
			maxLen = null == strData.getMaxLength() ? null : strData.getMaxLength().intValue();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataBinary) {
			CatalogColumnStatisticsDataBinary binaryData = (CatalogColumnStatisticsDataBinary) columnStatisticsData;
			avgLen = binaryData.getAvgLength();
			maxLen = null == binaryData.getMaxLength() ? null : binaryData.getMaxLength().intValue();
		} else if (columnStatisticsData instanceof CatalogColumnStatisticsDataDate) {
			CatalogColumnStatisticsDataDate dateData = (CatalogColumnStatisticsDataDate) columnStatisticsData;
			ndv = dateData.getNdv();
			if (dateData.getMax() != null) {
				max = Date.valueOf(DateTimeUtils.unixDateToString((int) dateData.getMax().getDaysSinceEpoch()));
			}
			if (dateData.getMin() != null) {
				min = Date.valueOf(DateTimeUtils.unixDateToString((int) dateData.getMin().getDaysSinceEpoch()));
			}
		} else {
			throw new TableException("Unsupported CatalogColumnStatisticsDataBase: " +
					columnStatisticsData.getClass().getCanonicalName());
		}
		return ColumnStats.Builder
				.builder()
				.setNdv(ndv)
				.setNullCount(nullCount)
				.setAvgLen(avgLen)
				.setMaxLen(maxLen)
				.setMax(max)
				.setMin(min)
				.build();
	}
}

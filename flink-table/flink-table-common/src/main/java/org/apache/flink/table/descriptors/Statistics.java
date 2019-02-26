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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.StatisticsValidator.AVG_LENGTH;
import static org.apache.flink.table.descriptors.StatisticsValidator.DISTINCT_COUNT;
import static org.apache.flink.table.descriptors.StatisticsValidator.MAX_LENGTH;
import static org.apache.flink.table.descriptors.StatisticsValidator.MAX_VALUE;
import static org.apache.flink.table.descriptors.StatisticsValidator.MIN_VALUE;
import static org.apache.flink.table.descriptors.StatisticsValidator.NAME;
import static org.apache.flink.table.descriptors.StatisticsValidator.NULL_COUNT;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_COLUMNS;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.StatisticsValidator.STATISTICS_ROW_COUNT;
import static org.apache.flink.table.descriptors.StatisticsValidator.normalizeColumnStats;

/**
 * Statistics descriptor for describing table stats.
 */
@PublicEvolving
public class Statistics implements Descriptor {

	private final DescriptorProperties internalProperties = new DescriptorProperties(true);
	private LinkedHashMap<String, Map<String, String>> columnStats = new LinkedHashMap<>();

	/**
	 * Sets the statistics from a {@link TableStats} instance.
	 *
	 * <p>This method overwrites all existing statistics.
	 *
	 * @param tableStats the table statistics
	 */
	public Statistics tableStats(TableStats tableStats) {
		rowCount(tableStats.getRowCount());
		columnStats.clear();
		tableStats.getColumnStats().forEach(this::columnStats);
		return this;
	}

	/**
	 * Sets statistics for the overall row count. Required.
	 *
	 * @param rowCount the expected number of rows
	 */
	public Statistics rowCount(long rowCount) {
		internalProperties.putLong(STATISTICS_ROW_COUNT, rowCount);
		return this;
	}

	/**
	 * Sets statistics for a column. Overwrites all existing statistics for this column.
	 *
	 * @param columnName  the column name
	 * @param columnStats expected statistics for the column
	 */
	public Statistics columnStats(String columnName, ColumnStats columnStats) {
		Map<String, String> map = normalizeColumnStats(columnStats);
		this.columnStats.put(columnName, map);
		return this;
	}

	/**
	 * Sets the number of distinct values statistic for the given column.
	 */
	public Statistics columnDistinctCount(String columnName, Long ndv) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(DISTINCT_COUNT, String.valueOf(ndv));
		return this;
	}

	/**
	 * Sets the number of null values statistic for the given column.
	 */
	public Statistics columnNullCount(String columnName, Long nullCount) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(NULL_COUNT, String.valueOf(nullCount));
		return this;
	}

	/**
	 * Sets the average length statistic for the given column.
	 */
	public Statistics columnAvgLength(String columnName, Double avgLen) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(AVG_LENGTH, String.valueOf(avgLen));
		return this;
	}

	/**
	 * Sets the maximum length statistic for the given column.
	 */
	public Statistics columnMaxLength(String columnName, Integer maxLen) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(MAX_LENGTH, String.valueOf(maxLen));
		return this;
	}

	/**
	 * Sets the maximum value statistic for the given column.
	 */
	public Statistics columnMaxValue(String columnName, Number max) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(MAX_VALUE, String.valueOf(max));
		return this;
	}

	/**
	 * Sets the minimum value statistic for the given column.
	 */
	public Statistics columnMinValue(String columnName, Number min) {
		this.columnStats
			.computeIfAbsent(columnName, column -> new HashMap<>())
			.put(MIN_VALUE, String.valueOf(min));
		return this;
	}

	/**
	 * Converts this descriptor into a set of properties.
	 */
	@Override
	public final Map<String, String> toProperties() {
		final DescriptorProperties properties = new DescriptorProperties();
		properties.putProperties(internalProperties);

		properties.putInt(STATISTICS_PROPERTY_VERSION, 1);

		List<Map<String, String>> namedStats = new ArrayList<>();
		for (Map.Entry<String, Map<String, String>> entry : columnStats.entrySet()) {
			Map<String, String> columnStat = entry.getValue();
			columnStat.put(NAME, entry.getKey());
			namedStats.add(columnStat);
		}

		properties.putIndexedVariableProperties(STATISTICS_COLUMNS, namedStats);
		return properties.asMap();
	}
}

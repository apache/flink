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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.plan.stats.ColumnStats;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Validator for {@link Statistics}.
 */
@Internal
public class StatisticsValidator implements DescriptorValidator {

	public static final String STATISTICS_PROPERTY_VERSION = "statistics.property-version";
	public static final String STATISTICS_ROW_COUNT = "statistics.row-count";
	public static final String STATISTICS_COLUMNS = "statistics.columns";

	// per column properties
	public static final String NAME = "name";
	public static final String DISTINCT_COUNT = "distinct-count";
	public static final String NULL_COUNT = "null-count";
	public static final String AVG_LENGTH = "avg-length";
	public static final String MAX_LENGTH = "max-length";
	public static final String MAX_VALUE = "max-value";
	public static final String MIN_VALUE = "min-value";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateInt(STATISTICS_PROPERTY_VERSION, true, 0, Integer.MAX_VALUE);
		properties.validateLong(STATISTICS_ROW_COUNT, true, 0);
		validateColumnStats(properties, STATISTICS_COLUMNS);
	}

	// utilities

	public static Map<String, String> normalizeColumnStats(ColumnStats columnStats) {
		Map<String, String> stats = new HashMap<>();
		if (columnStats.getNdv() != null) {
			stats.put(DISTINCT_COUNT, String.valueOf(columnStats.getNdv()));
		}
		if (columnStats.getNullCount() != null) {
			stats.put(NULL_COUNT, String.valueOf(columnStats.getNullCount()));
		}
		if (columnStats.getAvgLen() != null) {
			stats.put(AVG_LENGTH, String.valueOf(columnStats.getAvgLen()));
		}
		if (columnStats.getMaxLen() != null) {
			stats.put(MAX_LENGTH, String.valueOf(columnStats.getMaxLen()));
		}
		if (columnStats.getMaxValue() != null) {
			stats.put(MAX_VALUE, String.valueOf(columnStats.getMaxValue()));
		}
		if (columnStats.getMinValue() != null) {
			stats.put(MIN_VALUE, String.valueOf(columnStats.getMinValue()));
		}
		return stats;
	}

	public static void validateColumnStats(DescriptorProperties properties, String key) {

		// filter for number of columns
		int columnCount = properties.getIndexedProperty(key, NAME).size();

		for (int i = 0; i < columnCount; i++) {
			final String keyPrefix = key + "." + i + ".";
			properties.validateString(keyPrefix + NAME, false, 1);
			properties.validateLong(keyPrefix + DISTINCT_COUNT, true, 0L);
			properties.validateLong(keyPrefix + NULL_COUNT, true, 0L);
			properties.validateDouble(keyPrefix + AVG_LENGTH, true, 0.0);
			properties.validateInt(keyPrefix + MAX_LENGTH, true, 0);
			properties.validateDouble(keyPrefix + MIN_VALUE, true);
			Optional<Double> min = properties.getOptionalDouble(keyPrefix + MIN_VALUE);
			if (min.isPresent()) {
				properties.validateDouble(keyPrefix + MAX_VALUE, true, min.get());
			} else {
				properties.validateDouble(keyPrefix + MAX_VALUE, true);
			}
		}
	}

	public static Map<String, ColumnStats> readColumnStats(DescriptorProperties properties, String key) {

		// filter for number of columns
		int columnCount = properties.getIndexedProperty(key, NAME).size();

		Map<String, ColumnStats> stats = new HashMap<>();
		for (int i = 0; i < columnCount; i++) {
			final String keyPrefix = key + "." + i + ".";
			final String propertyKey = keyPrefix + NAME;
			String name = properties.getString(propertyKey);

			ColumnStats columnStats = new ColumnStats(
				properties.getOptionalLong(keyPrefix + DISTINCT_COUNT).orElse(null),
				properties.getOptionalLong(keyPrefix + NULL_COUNT).orElse(null),
				properties.getOptionalDouble(keyPrefix + AVG_LENGTH).orElse(null),
				properties.getOptionalInt(keyPrefix + MAX_LENGTH).orElse(null),
				properties.getOptionalDouble(keyPrefix + MAX_VALUE).orElse(null),
				properties.getOptionalDouble(keyPrefix + MIN_VALUE).orElse(null)
			);

			stats.put(name, columnStats);
		}

		return stats;
	}
}

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

package org.apache.flink.connector.jdbc.dialect;

import org.apache.flink.connector.jdbc.internal.converter.ClickhouseJdbcRowConverter;
import org.apache.flink.connector.jdbc.internal.converter.JdbcRowConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * dialect for Clickhouse.
 */
public class ClickhouseDialect extends AbstractDialect {

	// Define MAX/MIN precision of TIMESTAMP type according to derby docs:
	private static final int MAX_TIMESTAMP_PRECISION = 9;
	private static final int MIN_TIMESTAMP_PRECISION = 1;

	// Define MAX/MIN precision of DECIMAL type according to derby docs:
	private static final int MAX_DECIMAL_PRECISION = 31;
	private static final int MIN_DECIMAL_PRECISION = 1;

	@Override
	public int maxDecimalPrecision() {
		return MIN_DECIMAL_PRECISION;
	}

	@Override
	public Optional<String> defaultDriverName() {
		return Optional.of("ru.yandex.clickhouse.ClickHouseDriver");
	}

	@Override
	public int minDecimalPrecision() {
		return MAX_DECIMAL_PRECISION;
	}

	@Override
	public int maxTimestampPrecision() {
		return MAX_TIMESTAMP_PRECISION;
	}

	@Override
	public int minTimestampPrecision() {
		return MIN_TIMESTAMP_PRECISION;
	}

	@Override
	public List<LogicalTypeRoot> unsupportedTypes() {
		return null;
	}

	@Override
	public String dialectName() {
		return "Clickhouse";
	}

	@Override
	public boolean canHandle(String url) {
		return url.startsWith("jdbc:clickhouse");
	}

	@Override
	public JdbcRowConverter getRowConverter(RowType rowType) {
		return new ClickhouseJdbcRowConverter(rowType);
	}

	@Override
	public String quoteIdentifier(String identifier) {
		return "`" + identifier + "`";
	}

	@Override
	public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
		return Optional.of(getInsertIntoStatement(tableName, fieldNames));
	}

	@Override
	public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
		return getInsertIntoStatement(tableName, fieldNames);
	}

	@Override
	public String getDeleteStatement(String tableName, String[] conditionFields) {
		String suffix = Arrays.stream(conditionFields).map(e -> e + "=" + "?")
			.collect(Collectors.joining(" AND "));
		return String.format("ALTER TABLE %s DELETE WHERE %s", tableName, suffix);
	}
}

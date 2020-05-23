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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * An extractor for a Elasticsearch key from a {@link RowData}.
 */
@Internal
class KeyExtractor implements Function<RowData, String>, Serializable {
	private final FieldFormatter[] fieldFormatters;
	private final String keyDelimiter;

	private interface FieldFormatter extends Serializable {
		String format(RowData rowData);
	}

	private KeyExtractor(
			FieldFormatter[] fieldFormatters,
			String keyDelimiter) {
		this.fieldFormatters = fieldFormatters;
		this.keyDelimiter = keyDelimiter;
	}

	@Override
	public String apply(RowData rowData) {
		final StringBuilder builder = new StringBuilder();
		for (int i = 0; i < fieldFormatters.length; i++) {
			if (i > 0) {
				builder.append(keyDelimiter);
			}
			final String value = fieldFormatters[i].format(rowData);
			builder.append(value);
		}
		return builder.toString();
	}

	private static class ColumnWithIndex {
		public TableColumn column;
		public int index;

		public ColumnWithIndex(TableColumn column, int index) {
			this.column = column;
			this.index = index;
		}

		public LogicalType getType() {
			return column.getType().getLogicalType();
		}

		public int getIndex() {
			return index;
		}
	}

	public static Function<RowData, String> createKeyExtractor(
			TableSchema schema,
			String keyDelimiter) {
		return schema.getPrimaryKey().map(key -> {
			Map<String, ColumnWithIndex> namesToColumns = new HashMap<>();
			List<TableColumn> tableColumns = schema.getTableColumns();
			for (int i = 0; i < schema.getFieldCount(); i++) {
				TableColumn column = tableColumns.get(i);
				namesToColumns.put(column.getName(), new ColumnWithIndex(column, i));
			}

			FieldFormatter[] fieldFormatters = key.getColumns()
				.stream()
				.map(namesToColumns::get)
				.map(column -> toFormatter(column.index, column.getType()))
				.toArray(FieldFormatter[]::new);

			return (Function<RowData, String>) new KeyExtractor(
				fieldFormatters,
				keyDelimiter
			);
		}).orElseGet(() -> (Function<RowData, String> & Serializable) (row) -> null);
	}

	private static FieldFormatter toFormatter(int index, LogicalType type) {
		switch (type.getTypeRoot()) {
			case DATE:
				return (row) -> LocalDate.ofEpochDay(row.getInt(index)).toString();
			case TIME_WITHOUT_TIME_ZONE:
				return (row) -> LocalTime.ofNanoOfDay((long) row.getInt(index) * 1_000_000L).toString();
			case INTERVAL_YEAR_MONTH:
				return (row) -> Period.ofDays(row.getInt(index)).toString();
			case INTERVAL_DAY_TIME:
				return (row) -> Duration.ofMillis(row.getLong(index)).toString();
			case DISTINCT_TYPE:
				return toFormatter(index, ((DistinctType) type).getSourceType());
			default:
				RowData.FieldGetter fieldGetter = RowData.createFieldGetter(
					type,
					index);
				return (row) -> fieldGetter.getFieldOrNull(row).toString();
		}
	}
}

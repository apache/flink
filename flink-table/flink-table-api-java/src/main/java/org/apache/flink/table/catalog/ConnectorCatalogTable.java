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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.DefinedProctimeAttribute;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link CatalogTable} that wraps a {@link TableSource} and/or {@link TableSink}.
 * This allows registering those in a {@link Catalog}. It can not be persisted as the
 * source and/or sink might be inline implementations and not be representable in a
 * property based form.
 *
 * @param <T1> type of the produced elements by the {@link TableSource}
 * @param <T2> type of the expected elements by the {@link TableSink}
 */
@Internal
public class ConnectorCatalogTable<T1, T2> extends AbstractCatalogTable {
	private final TableSource<T1> tableSource;
	private final TableSink<T2> tableSink;
	// Flag that tells if the tableSource/tableSink is BatchTableSource/BatchTableSink.
	// NOTES: this should be false in BLINK planner, because BLINK planner always uses StreamTableSource.
	private final boolean isBatch;

	public static <T1> ConnectorCatalogTable source(TableSource<T1> source, boolean isBatch) {
		final TableSchema tableSchema = calculateSourceSchema(source, isBatch);
		return new ConnectorCatalogTable<>(source, null, tableSchema, isBatch);
	}

	public static <T2> ConnectorCatalogTable sink(TableSink<T2> sink, boolean isBatch) {
		return new ConnectorCatalogTable<>(null, sink, sink.getTableSchema(), isBatch);
	}

	public static <T1, T2> ConnectorCatalogTable sourceAndSink(
			TableSource<T1> source,
			TableSink<T2> sink,
			boolean isBatch) {
		TableSchema tableSchema = calculateSourceSchema(source, isBatch);
		return new ConnectorCatalogTable<>(source, sink, tableSchema, isBatch);
	}

	@VisibleForTesting
	protected ConnectorCatalogTable(
			TableSource<T1> tableSource,
			TableSink<T2> tableSink,
			TableSchema tableSchema,
			boolean isBatch) {
		super(tableSchema, Collections.emptyMap(), "");
		this.tableSource = tableSource;
		this.tableSink = tableSink;
		this.isBatch = isBatch;
	}

	public Optional<TableSource<T1>> getTableSource() {
		return Optional.ofNullable(tableSource);
	}

	public Optional<TableSink<T2>> getTableSink() {
		return Optional.ofNullable(tableSink);
	}

	public boolean isBatch() {
		return isBatch;
	}

	@Override
	public Map<String, String> toProperties() {
		// This effectively makes sure the table cannot be persisted in a catalog.
		throw new UnsupportedOperationException("ConnectorCatalogTable cannot be converted to properties");
	}

	@Override
	public CatalogBaseTable copy() {
		return this;
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}

	private static <T1> TableSchema calculateSourceSchema(TableSource<T1> source, boolean isBatch) {
		TableSchema tableSchema = source.getTableSchema();
		if (isBatch) {
			return tableSchema;
		}

		DataType[] types = Arrays.copyOf(tableSchema.getFieldDataTypes(), tableSchema.getFieldCount());
		String[] fieldNames = tableSchema.getFieldNames();
		if (source instanceof DefinedRowtimeAttributes) {
			updateRowtimeIndicators((DefinedRowtimeAttributes) source, fieldNames, types);
		}
		if (source instanceof DefinedProctimeAttribute) {
			updateProctimeIndicator((DefinedProctimeAttribute) source, fieldNames, types);
		}
		return TableSchema.builder().fields(fieldNames, types).build();
	}

	private static void updateRowtimeIndicators(
			DefinedRowtimeAttributes source,
			String[] fieldNames,
			DataType[] types) {
		List<String> rowtimeAttributes = source.getRowtimeAttributeDescriptors()
			.stream()
			.map(RowtimeAttributeDescriptor::getAttributeName)
			.collect(Collectors.toList());

		for (int i = 0; i < fieldNames.length; i++) {
			if (rowtimeAttributes.contains(fieldNames[i])) {
				// bridged to timestamp for compatible flink-planner
				types[i] = new AtomicDataType(new TimestampType(true, TimestampKind.ROWTIME, 3))
						.bridgedTo(java.sql.Timestamp.class);
			}
		}
	}

	private static void updateProctimeIndicator(
			DefinedProctimeAttribute source,
			String[] fieldNames,
			DataType[] types) {
		String proctimeAttribute = source.getProctimeAttribute();

		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(proctimeAttribute)) {
				// bridged to timestamp for compatible flink-planner
				types[i] = new AtomicDataType(new TimestampType(true, TimestampKind.PROCTIME, 3))
						.bridgedTo(java.sql.Timestamp.class);
				break;
			}
		}
	}
}

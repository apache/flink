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

package org.apache.flink.table.planner.plan;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.catalog.QueryOperationCatalogViewTable;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink specific {@link CalciteCatalogReader} that changes the RelOptTable which wrapped a
 * {@link CatalogSchemaTable} to a {@link FlinkPreparingTableBase}.
 */
public class FlinkCalciteCatalogReader extends CalciteCatalogReader {

	public FlinkCalciteCatalogReader(
		CalciteSchema rootSchema,
		List<List<String>> defaultSchemas,
		RelDataTypeFactory typeFactory,
		CalciteConnectionConfig config) {

		super(
			rootSchema,
			SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
				Stream.concat(
					defaultSchemas.stream(),
					Stream.of(Collections.<String>emptyList())
				).collect(Collectors.toList()),
			typeFactory,
			config);
	}

	@Override
	public Prepare.PreparingTable getTable(List<String> names) {
		Prepare.PreparingTable originRelOptTable = super.getTable(names);
		if (originRelOptTable == null) {
			return null;
		} else {
			// Wrap as linkPreparingTableBase to use in query optimization.
			CatalogSchemaTable table = originRelOptTable.unwrap(CatalogSchemaTable.class);
			if (table != null) {
				return toPreparingTable(originRelOptTable.getRelOptSchema(),
					originRelOptTable.getQualifiedName(),
					originRelOptTable.getRowType(),
					table);
			} else {
				return originRelOptTable;
			}
		}
	}

	/**
	 * Translate this {@link CatalogSchemaTable} into Flink source table.
	 */
	public static FlinkPreparingTableBase toPreparingTable(RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			CatalogSchemaTable table) {
		if (table.isTemporary()) {
			return convertTemporaryTable(relOptSchema,
				names,
				rowType,
				table.getObjectIdentifier(),
				table.getCatalogTable(),
				table.getStatistic(),
				table.isStreamingMode());
		} else {
			return convertPermanentTable(relOptSchema,
				names,
				rowType,
				table.getObjectIdentifier(),
				table.getCatalogTable(),
				table.getStatistic(),
				table.getCatalog().getTableFactory().orElse(null),
				table.isStreamingMode());
		}
	}

	private static FlinkPreparingTableBase convertPermanentTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			ObjectIdentifier objectIdentifier,
			CatalogBaseTable table,
			FlinkStatistic statistic,
			@Nullable TableFactory tableFactory,
			boolean isStreamingMode) {
		if (table instanceof QueryOperationCatalogView) {
			return convertQueryOperationView(relOptSchema,
				names,
				rowType,
				(QueryOperationCatalogView) table);
		} else if (table instanceof ConnectorCatalogTable) {
			ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) table;
			if ((connectorTable).getTableSource().isPresent()) {
				return convertSourceTable(relOptSchema,
					names,
					rowType,
					connectorTable,
					statistic,
					isStreamingMode);
			} else {
				throw new ValidationException("Cannot convert a connector table " +
					"without source.");
			}
		} else if (table instanceof CatalogTable) {
			CatalogTable catalogTable = (CatalogTable) table;
			return convertCatalogTable(relOptSchema,
				names,
				rowType,
				objectIdentifier.toObjectPath(),
				catalogTable,
				tableFactory,
				statistic,
				isStreamingMode);
		} else {
			throw new ValidationException("Unsupported table type: " + table);
		}
	}

	private static FlinkPreparingTableBase convertTemporaryTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			ObjectIdentifier objectIdentifier,
			CatalogBaseTable table,
			FlinkStatistic statistic,
			boolean isStreamingMode) {
		if (table instanceof QueryOperationCatalogView) {
			return convertQueryOperationView(relOptSchema,
				names,
				rowType,
				(QueryOperationCatalogView) table);
		} else if (table instanceof ConnectorCatalogTable) {
			ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) table;
			if ((connectorTable).getTableSource().isPresent()) {
				return convertSourceTable(relOptSchema,
					names,
					rowType,
					connectorTable,
					statistic,
					isStreamingMode);
			} else {
				throw new ValidationException("Cannot convert a connector table " +
					"without source.");
			}
		} else if (table instanceof CatalogTable) {
			return convertCatalogTable(relOptSchema,
				names,
				rowType,
				objectIdentifier.toObjectPath(),
				(CatalogTable) table,
				null,
				statistic,
				isStreamingMode);
		} else {
			throw new ValidationException("Unsupported table type: " + table);
		}
	}

	private static FlinkPreparingTableBase convertQueryOperationView(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			QueryOperationCatalogView view) {
		return QueryOperationCatalogViewTable.create(relOptSchema, names, rowType, view);
	}

	private static FlinkPreparingTableBase convertSourceTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			ConnectorCatalogTable<?, ?> table,
			FlinkStatistic statistic,
			boolean isStreamingMode) {
		TableSource<?> tableSource = table.getTableSource().get();
		if (!(tableSource instanceof StreamTableSource ||
			tableSource instanceof LookupableTableSource)) {
			throw new ValidationException(
				"Only StreamTableSource and LookupableTableSource can be used in Blink planner.");
		}
		if (!isStreamingMode && tableSource instanceof StreamTableSource &&
			!((StreamTableSource<?>) tableSource).isBounded()) {
			throw new ValidationException("Only bounded StreamTableSource can be used in batch mode.");
		}

		return new TableSourceTable<>(
			relOptSchema,
			names,
			rowType,
			tableSource,
			isStreamingMode,
			statistic,
			table);
	}

	private static FlinkPreparingTableBase convertCatalogTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			ObjectPath tablePath,
			CatalogTable table,
			@Nullable TableFactory tableFactory,
			FlinkStatistic statistic,
			boolean isStreamingMode) {
		TableSource<?> tableSource;
		if (tableFactory != null) {
			if (tableFactory instanceof TableSourceFactory) {
				tableSource = ((TableSourceFactory) tableFactory).createTableSource(tablePath, table);
			} else {
				throw new TableException(
					"Cannot query a sink-only table. TableFactory provided by catalog must implement TableSourceFactory");
			}
		} else {
			tableSource = TableFactoryUtil.findAndCreateTableSource(table);
		}

		if (!(tableSource instanceof StreamTableSource)) {
			throw new TableException("Catalog tables support only StreamTableSource and InputFormatTableSource");
		}

		return new TableSourceTable<>(
			relOptSchema,
			names,
			rowType,
			tableSource,
			!((StreamTableSource<?>) tableSource).isBounded(),
			statistic,
			table
		);
	}
}

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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.operations.DataStreamQueryOperation;
import org.apache.flink.table.planner.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.planner.plan.schema.TableSinkTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.flink.table.util.CatalogTableStatisticsConverter.convertToTableStats;

/**
 * A mapping between Flink catalog's database and Calcite's schema.
 * Tables are registered as tables in the schema.
 */
class DatabaseCalciteSchema extends FlinkSchema {
	private final String databaseName;
	private final String catalogName;
	private final CatalogManager catalogManager;
	// Flag that tells if the current planner should work in a batch or streaming mode.
	private final boolean isStreamingMode;

	public DatabaseCalciteSchema(String databaseName, String catalogName, CatalogManager catalog, boolean isStreamingMode) {
		this.databaseName = databaseName;
		this.catalogName = catalogName;
		this.catalogManager = catalog;
		this.isStreamingMode = isStreamingMode;
	}

	@Override
	public Table getTable(String tableName) {
		ObjectIdentifier identifier = ObjectIdentifier.of(catalogName, databaseName, tableName);
		return catalogManager.getTable(identifier)
			.map(result -> {
				CatalogBaseTable table = result.getTable();
				if (result.isTemporary()) {
					return convertTemporaryTable(new ObjectPath(databaseName, tableName), table);
				} else {
					return convertPermanentTable(
						identifier.toObjectPath(),
						table,
						catalogManager.getCatalog(catalogName)
							.flatMap(Catalog::getTableFactory)
							.orElse(null)
					);
				}
			})
			.orElse(null);
	}

	private Table convertPermanentTable(
			ObjectPath tablePath,
			CatalogBaseTable table,
			@Nullable TableFactory tableFactory) {
		// TODO supports GenericCatalogView
		if (table instanceof QueryOperationCatalogView) {
			return convertQueryOperationView(tablePath, (QueryOperationCatalogView) table);
		} else if (table instanceof ConnectorCatalogTable) {
			ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) table;
			if ((connectorTable).getTableSource().isPresent()) {
				TableStats tableStats = extractTableStats(connectorTable, tablePath);
				return convertSourceTable(connectorTable, tableStats);
			} else {
				return convertSinkTable(connectorTable);
			}
		} else if (table instanceof CatalogTable) {
			return convertCatalogTable(tablePath, (CatalogTable) table, tableFactory);
		} else {
			throw new TableException("Unsupported table type: " + table);
		}
	}

	private Table convertTemporaryTable(
			ObjectPath tablePath,
			CatalogBaseTable table) {
		// TODO supports GenericCatalogView
		if (table instanceof QueryOperationCatalogView) {
			return convertQueryOperationView(tablePath, (QueryOperationCatalogView) table);
		} else if (table instanceof ConnectorCatalogTable) {
			ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) table;
			if ((connectorTable).getTableSource().isPresent()) {
				return convertSourceTable(connectorTable, TableStats.UNKNOWN);
			} else {
				return convertSinkTable(connectorTable);
			}
		} else if (table instanceof CatalogTable) {
			return convertCatalogTable(tablePath, (CatalogTable) table, null);
		} else {
			throw new TableException("Unsupported table type: " + table);
		}
	}

	private Table convertQueryOperationView(ObjectPath tablePath, QueryOperationCatalogView table) {
		QueryOperation operation = table.getQueryOperation();
		if (operation instanceof DataStreamQueryOperation) {
			List<String> qualifiedName = Arrays.asList(catalogName, databaseName, tablePath.getObjectName());
			((DataStreamQueryOperation) operation).setQualifiedName(qualifiedName);
		} else if (operation instanceof RichTableSourceQueryOperation) {
			List<String> qualifiedName = Arrays.asList(catalogName, databaseName, tablePath.getObjectName());
			((RichTableSourceQueryOperation) operation).setQualifiedName(qualifiedName);
		}
		return QueryOperationCatalogViewTable.createCalciteTable(table);
	}

	private Table convertSinkTable(ConnectorCatalogTable<?, ?> table) {
		Optional<TableSinkTable> tableSinkTable = table.getTableSink()
			.map(tableSink -> new TableSinkTable<>(
				tableSink,
				FlinkStatistic.UNKNOWN()));
		if (tableSinkTable.isPresent()) {
			return tableSinkTable.get();
		} else {
			throw new TableException("Cannot convert a connector table " +
				"without either source or sink.");
		}
	}

	private Table convertSourceTable(
			ConnectorCatalogTable<?, ?> table,
			TableStats tableStats) {
		TableSource<?> tableSource = table.getTableSource().get();
		if (!(tableSource instanceof StreamTableSource ||
				tableSource instanceof LookupableTableSource)) {
			throw new TableException(
				"Only StreamTableSource and LookupableTableSource can be used in Blink planner.");
		}
		if (!isStreamingMode && tableSource instanceof StreamTableSource &&
			!((StreamTableSource<?>) tableSource).isBounded()) {
			throw new TableException("Only bounded StreamTableSource can be used in batch mode.");
		}

		return new TableSourceTable<>(
			tableSource,
			isStreamingMode,
			FlinkStatistic.builder().tableStats(tableStats).build(),
			null);
	}

	private TableStats extractTableStats(ConnectorCatalogTable<?, ?> table, ObjectPath tablePath) {
		TableStats tableStats = TableStats.UNKNOWN;
		try {
			// TODO supports stats for partitionable table
			if (!table.isPartitioned()) {
				Catalog catalog = catalogManager.getCatalog(catalogName).get();
				CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
				CatalogColumnStatistics columnStatistics = catalog.getTableColumnStatistics(tablePath);
				tableStats = convertToTableStats(tableStatistics, columnStatistics);
			}
			return tableStats;
		} catch (TableNotExistException e) {
			throw new TableException(format(
				"Could not access table partitions for table: [%s, %s, %s]",
				catalogName,
				databaseName,
				tablePath.getObjectName()), e);
		}
	}

	private Table convertCatalogTable(ObjectPath tablePath, CatalogTable table, @Nullable TableFactory tableFactory) {
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
			tableSource,
			!((StreamTableSource<?>) tableSource).isBounded(),
			FlinkStatistic.UNKNOWN(),
			table
		);
	}

	@Override
	public Set<String> getTableNames() {
		return catalogManager.listTables(catalogName, databaseName);
	}

	@Override
	public Schema getSubSchema(String s) {
		return null;
	}

	@Override
	public Set<String> getSubSchemaNames() {
		return new HashSet<>();
	}

	@Override
	public Expression getExpression(SchemaPlus parentSchema, String name) {
		return Schemas.subSchemaExpression(parentSchema, name, getClass());
	}

	@Override
	public boolean isMutable() {
		return true;
	}

}

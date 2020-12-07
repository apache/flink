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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkTypeFactory;
import org.apache.flink.table.catalog.CatalogManager.TableLookupResult;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.plan.schema.TableSinkTable;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * A mapping between Flink catalog's database and Calcite's schema.
 * Tables are registered as tables in the schema.
 */
class DatabaseCalciteSchema implements Schema {
	private final boolean isStreamingMode;
	private final String catalogName;
	private final String databaseName;
	private final CatalogManager catalogManager;
	private final TableConfig tableConfig;

	public DatabaseCalciteSchema(
			boolean isStreamingMode,
			String databaseName,
			String catalogName,
			CatalogManager catalogManager,
			TableConfig tableConfig) {
		this.isStreamingMode = isStreamingMode;
		this.databaseName = databaseName;
		this.catalogName = catalogName;
		this.catalogManager = catalogManager;
		this.tableConfig = tableConfig;
	}

	@Override
	public Table getTable(String tableName) {
		ObjectIdentifier identifier = ObjectIdentifier.of(catalogName, databaseName, tableName);
		return catalogManager.getTable(identifier)
			.map(result -> {
				final TableFactory tableFactory;
				if (result.isTemporary()) {
					tableFactory = null;
				} else {
					tableFactory = catalogManager.getCatalog(catalogName)
						.flatMap(Catalog::getTableFactory)
						.orElse(null);
				}
				return convertTable(identifier, result, tableFactory);
			})
			.orElse(null);
	}

	private Table convertTable(ObjectIdentifier identifier, TableLookupResult lookupResult, @Nullable TableFactory tableFactory) {
		CatalogBaseTable table = lookupResult.getTable();
		TableSchema resolvedSchema = lookupResult.getResolvedSchema();
		if (table instanceof QueryOperationCatalogView) {
			return QueryOperationCatalogViewTable.createCalciteTable(
				((QueryOperationCatalogView) table),
				resolvedSchema);
		} else if (table instanceof ConnectorCatalogTable) {
			return convertConnectorTable((ConnectorCatalogTable<?, ?>) table, resolvedSchema);
		} else {
			if (table instanceof CatalogTable) {
				return convertCatalogTable(
					identifier,
					(CatalogTable) table,
					resolvedSchema,
					tableFactory,
					lookupResult.isTemporary());
			} else if (table instanceof CatalogView) {
				return convertCatalogView(
					identifier.getObjectName(),
					(CatalogView) table,
					resolvedSchema);
			} else {
				throw new TableException("Unsupported table type: " + table);
			}
		}
	}

	private Table convertConnectorTable(ConnectorCatalogTable<?, ?> table, TableSchema resolvedSchema) {
		Optional<TableSourceTable<?>> tableSourceTable = table.getTableSource()
			.map(tableSource -> new TableSourceTable<>(
				resolvedSchema,
				tableSource,
				!table.isBatch(),
				FlinkStatistic.UNKNOWN()));
		if (tableSourceTable.isPresent()) {
			return tableSourceTable.get();
		} else {
			Optional<TableSinkTable<?>> tableSinkTable = table.getTableSink()
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
	}

	private Table convertCatalogTable(
			ObjectIdentifier identifier,
			CatalogTable table,
			TableSchema resolvedSchema,
			@Nullable TableFactory tableFactory,
			boolean isTemporary) {
		final TableSource<?> tableSource;
		final TableSourceFactory.Context context = new TableSourceFactoryContextImpl(
				identifier, table, tableConfig.getConfiguration(), isTemporary);
		if (tableFactory != null) {
			if (tableFactory instanceof TableSourceFactory) {
				tableSource = ((TableSourceFactory<?>) tableFactory).createTableSource(context);
			} else {
				throw new TableException(
					"Cannot query a sink-only table. TableFactory provided by catalog must implement TableSourceFactory");
			}
		} else {
			tableSource = TableFactoryUtil.findAndCreateTableSource(context);
		}

		if (!(tableSource instanceof StreamTableSource)) {
			throw new TableException("Catalog tables support only StreamTableSource and InputFormatTableSource");
		}

		return new TableSourceTable<>(
			resolvedSchema,
			tableSource,
			// this means the TableSource extends from StreamTableSource, this is needed for the
			// legacy Planner. Blink Planner should use the information that comes from the TableSource
			// itself to determine if it is a streaming or batch source.
			isStreamingMode,
			FlinkStatistic.UNKNOWN()
		);
	}

	private Table convertCatalogView(String tableName, CatalogView table, TableSchema resolvedSchema) {
		return new ViewTable(
			null,
			typeFactory -> ((FlinkTypeFactory) typeFactory).buildLogicalRowType(resolvedSchema),
			table.getExpandedQuery(),
			Arrays.asList(catalogName, databaseName),
			Arrays.asList(catalogName, databaseName, tableName)
		);
	}

	@Override
	public Set<String> getTableNames() {
		return catalogManager.listTables(catalogName, databaseName);
	}

	@Override
	public RelProtoDataType getType(String name) {
		return null;
	}

	@Override
	public Set<String> getTypeNames() {
		return new HashSet<>();
	}

	@Override
	public Collection<Function> getFunctions(String s) {
		return new HashSet<>();
	}

	@Override
	public Set<String> getFunctionNames() {
		return new HashSet<>();
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

	@Override
	public Schema snapshot(SchemaVersion schemaVersion) {
		return this;
	}
}

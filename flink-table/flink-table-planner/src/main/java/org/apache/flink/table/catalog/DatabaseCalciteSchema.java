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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

/**
 * A mapping between Flink catalog's database and Calcite's schema.
 * Tables are registered as tables in the schema.
 */
class DatabaseCalciteSchema implements Schema {
	private final boolean isStreamingMode;
	private final String databaseName;
	private final String catalogName;
	private final Catalog catalog;

	public DatabaseCalciteSchema(
			boolean isStreamingMode,
			String databaseName,
			String catalogName,
			Catalog catalog) {
		this.isStreamingMode = isStreamingMode;
		this.databaseName = databaseName;
		this.catalogName = catalogName;
		this.catalog = catalog;
	}

	@Override
	public Table getTable(String tableName) {

		ObjectPath tablePath = new ObjectPath(databaseName, tableName);

		try {
			if (!catalog.tableExists(tablePath)) {
				return null;
			}

			CatalogBaseTable table = catalog.getTable(tablePath);

			if (table instanceof QueryOperationCatalogView) {
				return QueryOperationCatalogViewTable.createCalciteTable(((QueryOperationCatalogView) table));
			} else if (table instanceof ConnectorCatalogTable) {
				return convertConnectorTable((ConnectorCatalogTable<?, ?>) table);
			} else if (table instanceof CatalogTable) {
				return convertCatalogTable(tablePath, (CatalogTable) table);
			} else {
				throw new TableException("Unsupported table type: " + table);
			}
		} catch (TableNotExistException | CatalogException e) {
			// TableNotExistException should never happen, because we are checking it exists
			// via catalog.tableExists
			throw new TableException(format(
				"A failure occurred when accessing table. Table path [%s, %s, %s]",
				catalogName,
				databaseName,
				tableName), e);
		}
	}

	private Table convertConnectorTable(ConnectorCatalogTable<?, ?> table) {
		Optional<TableSourceTable> tableSourceTable = table.getTableSource()
			.map(tableSource -> new TableSourceTable<>(
				tableSource,
				!table.isBatch(),
				FlinkStatistic.UNKNOWN()));
		if (tableSourceTable.isPresent()) {
			return tableSourceTable.get();
		} else {
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
	}

	private Table convertCatalogTable(ObjectPath tablePath, CatalogTable table) {
		TableSource<?> tableSource;
		Optional<TableFactory> tableFactory = catalog.getTableFactory();
		if (tableFactory.isPresent()) {
			TableFactory tf = tableFactory.get();
			if (tf instanceof TableSourceFactory) {
				tableSource = ((TableSourceFactory) tf).createTableSource(tablePath, table);
			} else {
				throw new TableException(String.format("Cannot query a sink-only table. TableFactory provided by catalog %s must implement TableSourceFactory",
					catalog.getClass()));
			}
		} else {
			tableSource = TableFactoryUtil.findAndCreateTableSource(table);
		}

		if (!(tableSource instanceof StreamTableSource)) {
			throw new TableException("Catalog tables support only StreamTableSource and InputFormatTableSource");
		}

		return new TableSourceTable<>(
			tableSource,
			// this means the TableSource extends from StreamTableSource, this is needed for the
			// legacy Planner. Blink Planner should use the information that comes from the TableSource
			// itself to determine if it is a streaming or batch source.
			isStreamingMode,
			FlinkStatistic.UNKNOWN()
		);
	}

	@Override
	public Set<String> getTableNames() {
		try {
			return new HashSet<>(catalog.listTables(databaseName));
		} catch (DatabaseNotExistException e) {
			throw new CatalogException(e);
		}
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

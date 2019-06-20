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
import org.apache.flink.table.operations.DataStreamQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.RichTableSourceQueryOperation;
import org.apache.flink.table.plan.schema.TableSourceTable;
import org.apache.flink.table.plan.stats.FlinkStatistic;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;

/**
 * A mapping between Flink catalog's database and Calcite's schema.
 * Tables are registered as tables in the schema.
 */
class DatabaseCalciteSchema extends FlinkSchema {
	private final String databaseName;
	private final String catalogName;
	private final Catalog catalog;

	public DatabaseCalciteSchema(String databaseName, String catalogName, Catalog catalog) {
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

			// TODO supports GenericCatalogView
			if (table instanceof QueryOperationCatalogView) {
				QueryOperationCatalogView view = (QueryOperationCatalogView) table;
				QueryOperation operation = view.getQueryOperation();
				if (operation instanceof DataStreamQueryOperation) {
					List<String> qualifiedName = Arrays.asList(catalogName, databaseName, tableName);
					((DataStreamQueryOperation) operation).setQualifiedName(qualifiedName);
				} else if (operation instanceof RichTableSourceQueryOperation) {
					List<String> qualifiedName = Arrays.asList(catalogName, databaseName, tableName);
					((RichTableSourceQueryOperation) operation).setQualifiedName(qualifiedName);
				}
				return QueryOperationCatalogViewTable.createCalciteTable(view);
			} else if (table instanceof ConnectorCatalogTable) {
				ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable<?, ?>) table;
				return connectorTable.getTableSource()
					.map(tableSource -> new TableSourceTable<>(
						tableSource,
						!connectorTable.isBatch(),
						FlinkStatistic.UNKNOWN())
					).orElseThrow(() -> new TableException("Cannot query a sink only table."));
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

	@Override
	public Set<String> getTableNames() {
		try {
			return new HashSet<>(catalog.listTables(databaseName));
		} catch (DatabaseNotExistException e) {
			throw new CatalogException(e);
		}
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

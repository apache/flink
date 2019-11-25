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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.HashSet;
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
				Catalog catalog = catalogManager.getCatalog(catalogName).get();
				FlinkStatistic statistic = getStatistic(result.isTemporary(),
					catalog, table, identifier);
				return new CatalogSchemaTable(identifier,
					table,
					statistic,
					catalog.getTableFactory().orElse(null),
					isStreamingMode,
					result.isTemporary());
			})
			.orElse(null);
	}

	private static FlinkStatistic getStatistic(boolean isTemporary, Catalog catalog,
			CatalogBaseTable catalogBaseTable, ObjectIdentifier tableIdentifier) {
		if (isTemporary || catalogBaseTable instanceof QueryOperationCatalogView) {
			return FlinkStatistic.UNKNOWN();
		}
		if (catalogBaseTable instanceof CatalogTable) {
			return FlinkStatistic.builder()
				.tableStats(extractTableStats(catalog, tableIdentifier))
				.build();
		} else {
			return FlinkStatistic.UNKNOWN();
		}
	}

	private static TableStats extractTableStats(Catalog catalog,
			ObjectIdentifier objectIdentifier) {
		final ObjectPath tablePath = objectIdentifier.toObjectPath();
		try {
			CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
			CatalogColumnStatistics columnStatistics = catalog.getTableColumnStatistics(tablePath);
			return convertToTableStats(tableStatistics, columnStatistics);
		} catch (TableNotExistException e) {
			throw new ValidationException(format(
				"Could not get statistic for table: [%s, %s, %s]",
				objectIdentifier.getCatalogName(),
				tablePath.getDatabaseName(),
				tablePath.getObjectName()), e);
		}
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

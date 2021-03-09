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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
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
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToTableStats;

/**
 * A mapping between Flink catalog's database and Calcite's schema. Tables are registered as tables
 * in the schema.
 */
class DatabaseCalciteSchema extends FlinkSchema {
    private final String databaseName;
    private final String catalogName;
    private final CatalogManager catalogManager;
    // Flag that tells if the current planner should work in a batch or streaming mode.
    private final boolean isStreamingMode;

    public DatabaseCalciteSchema(
            String databaseName,
            String catalogName,
            CatalogManager catalog,
            boolean isStreamingMode) {
        this.databaseName = databaseName;
        this.catalogName = catalogName;
        this.catalogManager = catalog;
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public Table getTable(String tableName) {
        ObjectIdentifier identifier = ObjectIdentifier.of(catalogName, databaseName, tableName);
        return catalogManager
                .getTable(identifier)
                .map(
                        result -> {
                            CatalogBaseTable table = result.getTable();
                            FlinkStatistic statistic =
                                    getStatistic(result.isTemporary(), table, identifier);
                            return new CatalogSchemaTable(
                                    identifier,
                                    result,
                                    statistic,
                                    catalogManager
                                            .getCatalog(catalogName)
                                            .orElseThrow(IllegalStateException::new),
                                    isStreamingMode);
                        })
                .orElse(null);
    }

    private FlinkStatistic getStatistic(
            boolean isTemporary,
            CatalogBaseTable catalogBaseTable,
            ObjectIdentifier tableIdentifier) {
        if (isTemporary || catalogBaseTable instanceof QueryOperationCatalogView) {
            return FlinkStatistic.UNKNOWN();
        }
        if (catalogBaseTable instanceof CatalogTable) {
            Catalog catalog = catalogManager.getCatalog(catalogName).get();
            return FlinkStatistic.builder()
                    .tableStats(extractTableStats(catalog, tableIdentifier))
                    // this is a temporary solution, FLINK-15123 will resolve this
                    .uniqueKeys(extractUniqueKeys(catalogBaseTable.getSchema()))
                    .build();
        } else {
            return FlinkStatistic.UNKNOWN();
        }
    }

    private static TableStats extractTableStats(
            Catalog catalog, ObjectIdentifier objectIdentifier) {
        final ObjectPath tablePath = objectIdentifier.toObjectPath();
        try {
            CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
            CatalogColumnStatistics columnStatistics = catalog.getTableColumnStatistics(tablePath);
            return convertToTableStats(tableStatistics, columnStatistics);
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    format(
                            "Could not get statistic for table: [%s, %s, %s]",
                            objectIdentifier.getCatalogName(),
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName()),
                    e);
        }
    }

    private static Set<Set<String>> extractUniqueKeys(TableSchema tableSchema) {
        Optional<UniqueConstraint> primaryKeyConstraint = tableSchema.getPrimaryKey();
        if (primaryKeyConstraint.isPresent()) {
            Set<String> primaryKey = new HashSet<>(primaryKeyConstraint.get().getColumns());
            Set<Set<String>> uniqueKeys = new HashSet<>();
            uniqueKeys.add(primaryKey);
            return uniqueKeys;
        } else {
            return null;
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

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
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogManager.TableLookupResult;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
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
import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToTableStats;

/**
 * A mapping between Flink catalog's database and Calcite's schema. Tables are registered as tables
 * in the schema.
 */
class DatabaseCalciteSchema extends FlinkSchema {
    private final String catalogName;
    private final String databaseName;
    private final CatalogManager catalogManager;
    // Flag that tells if the current planner should work in a batch or streaming mode.
    private final boolean isStreamingMode;

    public DatabaseCalciteSchema(
            String catalogName,
            String databaseName,
            CatalogManager catalog,
            boolean isStreamingMode) {
        this.databaseName = databaseName;
        this.catalogName = catalogName;
        this.catalogManager = catalog;
        this.isStreamingMode = isStreamingMode;
    }

    @Override
    public Table getTable(String tableName) {
        final ObjectIdentifier identifier =
                ObjectIdentifier.of(catalogName, databaseName, tableName);
        return catalogManager
                .getTable(identifier)
                .map(
                        lookupResult ->
                                new CatalogSchemaTable(
                                        identifier,
                                        lookupResult,
                                        getStatistic(lookupResult, identifier),
                                        isStreamingMode))
                .orElse(null);
    }

    private FlinkStatistic getStatistic(
            TableLookupResult lookupResult, ObjectIdentifier identifier) {
        final ResolvedCatalogBaseTable<?> resolvedBaseTable = lookupResult.getResolvedTable();
        switch (resolvedBaseTable.getTableKind()) {
            case TABLE:
                return FlinkStatistic.builder()
                        .tableStats(extractTableStats(lookupResult, identifier))
                        // this is a temporary solution, FLINK-15123 will resolve this
                        .uniqueKeys(
                                resolvedBaseTable.getResolvedSchema().getPrimaryKey().orElse(null))
                        .build();
            case VIEW:
            default:
                return FlinkStatistic.UNKNOWN();
        }
    }

    private TableStats extractTableStats(
            TableLookupResult lookupResult, ObjectIdentifier identifier) {
        if (lookupResult.isTemporary()) {
            return TableStats.UNKNOWN;
        }
        final Catalog catalog = lookupResult.getCatalog().orElseThrow(IllegalStateException::new);
        final ObjectPath tablePath = identifier.toObjectPath();
        try {
            final CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
            final CatalogColumnStatistics columnStatistics =
                    catalog.getTableColumnStatistics(tablePath);
            return convertToTableStats(tableStatistics, columnStatistics);
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    format(
                            "Could not get statistic for table: [%s, %s, %s]",
                            identifier.getCatalogName(),
                            tablePath.getDatabaseName(),
                            tablePath.getObjectName()),
                    e);
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

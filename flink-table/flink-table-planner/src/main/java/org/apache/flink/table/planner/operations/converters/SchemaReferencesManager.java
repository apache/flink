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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.planner.expressions.ColumnReferenceFinder;
import org.apache.flink.table.utils.EncodingUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Helper class to track column references while (DROP | RENAME) COLUMN operations. */
public class SchemaReferencesManager {

    /** Available columns in the table. */
    private final Set<String> columns;

    /**
     * Mappings about the column refers which columns, e.g. column `b` refers to the column `a` in
     * the expression "b as a+1".
     */
    private final Map<String, Set<String>> columnToReferences;

    /**
     * Reverse mappings about the column refers which columns, e.g. column `a` has the dependency of
     * column `b` in the expression "b as a+1".
     */
    private final Map<String, Set<String>> columnToDependencies;

    /** Primary keys defined on the table. */
    private final Set<String> primaryKeys;

    /** The name of the column watermark expression depends on. */
    private final Set<String> watermarkReferences;

    /** The name of the column partition keys contains. */
    private final Set<String> partitionKeys;

    /** The names of the columns used as distribution keys. */
    private final Set<String> distributionKeys;

    private SchemaReferencesManager(
            Set<String> columns,
            Map<String, Set<String>> columnToReferences,
            Map<String, Set<String>> columnToDependencies,
            Set<String> primaryKeys,
            Set<String> watermarkReferences,
            Set<String> partitionKeys,
            Set<String> distributionKeys) {
        this.columns = columns;
        this.columnToReferences = columnToReferences;
        this.columnToDependencies = columnToDependencies;
        this.primaryKeys = primaryKeys;
        this.watermarkReferences = watermarkReferences;
        this.partitionKeys = partitionKeys;
        this.distributionKeys = distributionKeys;
    }

    public static SchemaReferencesManager create(ResolvedCatalogTable catalogTable) {
        Map<String, Set<String>> columnToReferences = new HashMap<>();
        Map<String, Set<String>> columnToDependencies = new HashMap<>();
        catalogTable.getResolvedSchema().getColumns().stream()
                .filter(column -> column instanceof Column.ComputedColumn)
                .forEach(
                        column -> {
                            Set<String> referencedColumns =
                                    ColumnReferenceFinder.findReferencedColumn(
                                            column.getName(), catalogTable.getResolvedSchema());
                            for (String referencedColumn : referencedColumns) {
                                columnToReferences
                                        .computeIfAbsent(referencedColumn, key -> new HashSet<>())
                                        .add(column.getName());
                                columnToDependencies
                                        .computeIfAbsent(column.getName(), key -> new HashSet<>())
                                        .add(referencedColumn);
                            }
                        });

        return new SchemaReferencesManager(
                new HashSet<>(catalogTable.getResolvedSchema().getColumnNames()),
                columnToReferences,
                columnToDependencies,
                catalogTable
                        .getResolvedSchema()
                        .getPrimaryKey()
                        .map(constraint -> new HashSet<>(constraint.getColumns()))
                        .orElse(new HashSet<>()),
                ColumnReferenceFinder.findWatermarkReferencedColumn(
                        catalogTable.getResolvedSchema()),
                new HashSet<>(catalogTable.getPartitionKeys()),
                new HashSet<>(
                        catalogTable
                                .getDistribution()
                                .map(TableDistribution::getBucketKeys)
                                .orElse(List.of())));
    }

    public void dropColumn(String columnName, Supplier<String> errorMsg) {
        checkReferences(columnName, errorMsg);
        if (primaryKeys.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column %s is used as the primary key.",
                            errorMsg.get(), EncodingUtils.escapeIdentifier(columnName)));
        }

        columnToDependencies
                .getOrDefault(columnName, Set.of())
                .forEach(
                        referredColumn ->
                                columnToReferences.get(referredColumn).remove(columnName));
        columnToDependencies.remove(columnName);
        columns.remove(columnName);
    }

    public int getColumnDependencyCount(String columnName) {
        return columnToDependencies.getOrDefault(columnName, Set.of()).size();
    }

    public void checkReferences(String columnName, Supplier<String> errorMsg) {
        if (!columns.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` does not exist in the base table.",
                            errorMsg.get(), columnName));
        }
        if (columnToReferences.containsKey(columnName)
                && !columnToReferences.get(columnName).isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe column %s is referenced by computed column %s.",
                            errorMsg.get(),
                            EncodingUtils.escapeIdentifier(columnName),
                            columnToReferences.get(columnName).stream()
                                    .map(EncodingUtils::escapeIdentifier)
                                    .sorted()
                                    .collect(Collectors.joining(", "))));
        }
        if (partitionKeys.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` is used as the partition keys.",
                            errorMsg.get(), columnName));
        }
        if (watermarkReferences.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` is referenced by watermark expression.",
                            errorMsg.get(), columnName));
        }
        if (distributionKeys.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` is used as a distribution key.",
                            errorMsg.get(), columnName));
        }
    }

    public static void buildUpdatedColumn(
            Schema.Builder builder,
            ResolvedCatalogBaseTable<?> oldTable,
            BiConsumer<Schema.Builder, Schema.UnresolvedColumn> columnConsumer) {
        // build column
        oldTable.getUnresolvedSchema()
                .getColumns()
                .forEach(column -> columnConsumer.accept(builder, column));
    }

    public static void buildUpdatedWatermark(
            Schema.Builder builder, ResolvedCatalogBaseTable<?> oldTable) {
        oldTable.getUnresolvedSchema()
                .getWatermarkSpecs()
                .forEach(
                        watermarkSpec ->
                                builder.watermark(
                                        watermarkSpec.getColumnName(),
                                        watermarkSpec.getWatermarkExpression()));
    }

    public static void buildUpdatedPrimaryKey(
            Schema.Builder builder,
            ResolvedCatalogBaseTable<?> oldTable,
            Function<String, String> columnRenamer) {
        oldTable.getUnresolvedSchema()
                .getPrimaryKey()
                .ifPresent(
                        pk -> {
                            List<String> oldPrimaryKeyNames = pk.getColumnNames();
                            String constrainName = pk.getConstraintName();
                            List<String> newPrimaryKeyNames =
                                    oldPrimaryKeyNames.stream()
                                            .map(columnRenamer)
                                            .collect(Collectors.toList());
                            builder.primaryKeyNamed(constrainName, newPrimaryKeyNames);
                        });
    }
}

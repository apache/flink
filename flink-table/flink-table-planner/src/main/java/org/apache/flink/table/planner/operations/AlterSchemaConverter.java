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

package org.apache.flink.table.planner.operations;

import org.apache.flink.sql.parser.ddl.SqlAlterTable;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropWatermark;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRenameColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.planner.expressions.ColumnReferenceFinder;
import org.apache.flink.table.utils.EncodingUtils;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Converter to convert {@link SqlAlterTableSchema} with source table to generate new {@link
 * Schema}.
 */
public class AlterSchemaConverter {

    private static final String EX_MSG_PREFIX = "Failed to execute ALTER TABLE statement.\n";

    private final SqlValidator sqlValidator;
    private final Function<SqlNode, String> escapeExpression;
    private final CatalogManager catalogManager;

    public AlterSchemaConverter(
            SqlValidator sqlValidator,
            Function<SqlNode, String> escapeExpression,
            CatalogManager catalogManager) {
        this.sqlValidator = sqlValidator;
        this.escapeExpression = escapeExpression;
        this.catalogManager = catalogManager;
    }

    /** Convert ALTER TABLE RENAME col_name to new_col_name to generate an updated Schema. */
    public Operation convertAlterSchema(
            SqlAlterTableRenameColumn renameColumn, ResolvedCatalogTable oldTable) {
        String oldColumnName = getColumnName(renameColumn.getOldColumnIdentifier());
        String newColumnName = getColumnName(renameColumn.getNewColumnIdentifier());

        ReferencesManager.create(oldTable).checkReferences(oldColumnName);
        if (oldTable.getResolvedSchema().getColumn(newColumnName).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` already existed in table schema.",
                            EX_MSG_PREFIX, newColumnName));
        }

        // generate new schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (column.getName().equals(oldColumnName)) {
                        buildNewColumnFromOldColumn(builder, column, newColumnName);
                    } else {
                        builder.fromColumns(Collections.singletonList(column));
                    }
                });
        buildUpdatedPrimaryKey(
                schemaBuilder, oldTable, (pk) -> pk.equals(oldColumnName) ? newColumnName : pk);
        buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                renameColumn,
                Collections.singletonList(
                        TableChange.modifyColumnName(
                                unwrap(oldTable.getResolvedSchema().getColumn(oldColumnName)),
                                newColumnName)),
                schemaBuilder.build(),
                oldTable);
    }

    /** Convert ALTER TABLE DROP (col1 [, col2, ...]) to generate an updated Schema. */
    public Operation convertAlterSchema(
            SqlAlterTableDropColumn dropColumn, ResolvedCatalogTable oldTable) {
        Set<String> columnsToDrop = new HashSet<>();
        dropColumn
                .getColumnList()
                .forEach(
                        identifier -> {
                            String name = getColumnName((SqlIdentifier) identifier);
                            if (!columnsToDrop.add(name)) {
                                throw new ValidationException(
                                        String.format(
                                                "%sDuplicate column `%s`.", EX_MSG_PREFIX, name));
                            }
                        });

        ReferencesManager referencesManager = ReferencesManager.create(oldTable);
        // Sort by dependencies count from smallest to largest. For example, when dropping column a,
        // b(b as a+1), the order should be: [b, a] after sort.
        List<String> sortedColumnsToDrop =
                columnsToDrop.stream()
                        .sorted(
                                Comparator.comparingInt(
                                                col ->
                                                        referencesManager.getColumnDependencyCount(
                                                                (String) col))
                                        .reversed())
                        .collect(Collectors.toList());
        List<TableChange> tableChanges = new ArrayList<>(sortedColumnsToDrop.size());
        for (String columnToDrop : sortedColumnsToDrop) {
            referencesManager.dropColumn(columnToDrop);
            tableChanges.add(TableChange.dropColumn(columnToDrop));
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (!columnsToDrop.contains(column.getName())) {
                        builder.fromColumns(Collections.singletonList(column));
                    }
                });
        buildUpdatedPrimaryKey(schemaBuilder, oldTable, Function.identity());
        buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                dropColumn, tableChanges, schemaBuilder.build(), oldTable);
    }

    /** Convert ALTER TABLE DROP PRIMARY KEY to generate an updated Schema. */
    public Operation convertAlterSchema(
            SqlAlterTableDropPrimaryKey dropPrimaryKey, ResolvedCatalogTable oldTable) {
        Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();
        if (!pkConstraint.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define any primary key.", EX_MSG_PREFIX));
        }
        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> builder.fromColumns(Collections.singletonList(column)));
        buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                dropPrimaryKey,
                Collections.singletonList(TableChange.dropConstraint(pkConstraint.get().getName())),
                schemaBuilder.build(),
                oldTable);
    }

    /**
     * Convert ALTER TABLE DROP CONSTRAINT constraint_name to generate an updated {@link Schema}.
     */
    public Operation convertAlterSchema(
            SqlAlterTableDropConstraint dropConstraint, ResolvedCatalogTable oldTable) {
        Optional<UniqueConstraint> pkConstraint = oldTable.getResolvedSchema().getPrimaryKey();
        if (!pkConstraint.isPresent()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define any primary key.", EX_MSG_PREFIX));
        }
        SqlIdentifier constraintIdentifier = dropConstraint.getConstraintName();
        String constraintName = pkConstraint.get().getName();
        if (constraintIdentifier != null
                && !constraintIdentifier.getSimple().equals(constraintName)) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define a primary key constraint named '%s'. "
                                    + "Available constraint name: ['%s'].",
                            EX_MSG_PREFIX, constraintIdentifier.getSimple(), constraintName));
        }
        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> builder.fromColumns(Collections.singletonList(column)));
        buildUpdatedWatermark(schemaBuilder, oldTable);

        return buildAlterTableChangeOperation(
                dropConstraint,
                Collections.singletonList(TableChange.dropConstraint(constraintName)),
                schemaBuilder.build(),
                oldTable);
    }

    /** Convert ALTER TABLE DROP WATERMARK to generate an updated {@link Schema}. */
    public Operation convertAlterSchema(
            SqlAlterTableDropWatermark dropWatermark, ResolvedCatalogTable oldTable) {
        if (oldTable.getResolvedSchema().getWatermarkSpecs().isEmpty()) {
            throw new ValidationException(
                    String.format(
                            "%sThe base table does not define any watermark strategy.",
                            EX_MSG_PREFIX));
        }

        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> builder.fromColumns(Collections.singletonList(column)));
        buildUpdatedPrimaryKey(schemaBuilder, oldTable, Function.identity());

        return buildAlterTableChangeOperation(
                dropWatermark,
                Collections.singletonList(TableChange.dropWatermark()),
                schemaBuilder.build(),
                oldTable);
    }

    // --------------------------------------------------------------------------------------------

    private static class ReferencesManager {

        /** Available columns in the table. */
        private final Set<String> columns;

        /**
         * Mappings about the column refers which columns, e.g. column `b` refers to the column `a`
         * in the expression "b as a+1".
         */
        private final Map<String, Set<String>> columnToReferences;

        /**
         * Reverse mappings about the column refers which columns, e.g. column `a` has the
         * dependency of column `b` in the expression "b as a+1".
         */
        private final Map<String, Set<String>> columnToDependencies;

        /** Primary keys defined on the table. */
        private final Set<String> primaryKeys;

        /** The name of the column watermark expression depends on. */
        private final Set<String> watermarkReferences;

        /** The name of the column partition keys contains. */
        private final Set<String> partitionKeys;

        private ReferencesManager(
                Set<String> columns,
                Map<String, Set<String>> columnToReferences,
                Map<String, Set<String>> columnToDependencies,
                Set<String> primaryKeys,
                Set<String> watermarkReferences,
                Set<String> partitionKeys) {
            this.columns = columns;
            this.columnToReferences = columnToReferences;
            this.columnToDependencies = columnToDependencies;
            this.primaryKeys = primaryKeys;
            this.watermarkReferences = watermarkReferences;
            this.partitionKeys = partitionKeys;
        }

        static ReferencesManager create(ResolvedCatalogTable catalogTable) {
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
                                            .computeIfAbsent(
                                                    referencedColumn, key -> new HashSet<>())
                                            .add(column.getName());
                                    columnToDependencies
                                            .computeIfAbsent(
                                                    column.getName(), key -> new HashSet<>())
                                            .add(referencedColumn);
                                }
                            });

            return new ReferencesManager(
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
                    new HashSet<>(catalogTable.getPartitionKeys()));
        }

        void dropColumn(String columnName) {
            checkReferences(columnName);
            if (primaryKeys.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sThe column %s is used as the primary key.",
                                EX_MSG_PREFIX, EncodingUtils.escapeIdentifier(columnName)));
            }

            columnToDependencies
                    .getOrDefault(columnName, Collections.emptySet())
                    .forEach(
                            referredColumn ->
                                    columnToReferences.get(referredColumn).remove(columnName));
            columnToDependencies.remove(columnName);
            columns.remove(columnName);
        }

        int getColumnDependencyCount(String columnName) {
            return columnToDependencies.getOrDefault(columnName, Collections.emptySet()).size();
        }

        void checkReferences(String columnName) {
            if (!columns.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sThe column `%s` does not exist in the base table.",
                                EX_MSG_PREFIX, columnName));
            }
            if (columnToReferences.containsKey(columnName)
                    && !columnToReferences.get(columnName).isEmpty()) {
                throw new ValidationException(
                        String.format(
                                "%sThe column %s is referenced by computed column %s.",
                                EX_MSG_PREFIX,
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
                                EX_MSG_PREFIX, columnName));
            }
            if (watermarkReferences.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sThe column `%s` is referenced by watermark expression.",
                                EX_MSG_PREFIX, columnName));
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private void buildUpdatedColumn(
            Schema.Builder builder,
            ResolvedCatalogTable oldTable,
            BiConsumer<Schema.Builder, Schema.UnresolvedColumn> columnConsumer) {
        // build column
        oldTable.getUnresolvedSchema()
                .getColumns()
                .forEach(column -> columnConsumer.accept(builder, column));
    }

    private void buildUpdatedPrimaryKey(
            Schema.Builder builder,
            ResolvedCatalogTable oldTable,
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

    private void buildUpdatedWatermark(Schema.Builder builder, ResolvedCatalogTable oldTable) {
        oldTable.getUnresolvedSchema()
                .getWatermarkSpecs()
                .forEach(
                        watermarkSpec ->
                                builder.watermark(
                                        watermarkSpec.getColumnName(),
                                        watermarkSpec.getWatermarkExpression()));
    }

    private void buildNewColumnFromOldColumn(
            Schema.Builder builder, Schema.UnresolvedColumn oldColumn, String columnName) {
        if (oldColumn instanceof Schema.UnresolvedComputedColumn) {
            builder.columnByExpression(
                    columnName, ((Schema.UnresolvedComputedColumn) oldColumn).getExpression());
        } else if (oldColumn instanceof Schema.UnresolvedPhysicalColumn) {
            builder.column(columnName, ((Schema.UnresolvedPhysicalColumn) oldColumn).getDataType());
        } else if (oldColumn instanceof Schema.UnresolvedMetadataColumn) {
            Schema.UnresolvedMetadataColumn metadataColumn =
                    (Schema.UnresolvedMetadataColumn) oldColumn;
            builder.columnByMetadata(
                    columnName,
                    metadataColumn.getDataType(),
                    metadataColumn.getMetadataKey(),
                    metadataColumn.isVirtual());
        }
        oldColumn.getComment().ifPresent(builder::withComment);
    }

    private Operation buildAlterTableChangeOperation(
            SqlAlterTable alterTable,
            List<TableChange> tableChanges,
            Schema newSchema,
            ResolvedCatalogTable oldTable) {
        return new AlterTableChangeOperation(
                catalogManager.qualifyIdentifier(
                        UnresolvedIdentifier.of(alterTable.fullTableName())),
                tableChanges,
                CatalogTable.of(
                        newSchema,
                        oldTable.getComment(),
                        oldTable.getPartitionKeys(),
                        oldTable.getOptions()),
                alterTable.ifTableExists());
    }

    private static String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAlter nested row type %s is not supported yet.",
                            EX_MSG_PREFIX, identifier));
        }
        return identifier.getSimple();
    }

    private <T> T unwrap(Optional<T> value) {
        return value.orElseThrow(() -> new TableException("The value should never be empty."));
    }
}

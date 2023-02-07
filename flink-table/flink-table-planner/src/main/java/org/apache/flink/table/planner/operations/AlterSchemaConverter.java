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
import org.apache.flink.sql.parser.ddl.SqlAlterTableAdd;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropConstraint;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropPrimaryKey;
import org.apache.flink.sql.parser.ddl.SqlAlterTableDropWatermark;
import org.apache.flink.sql.parser.ddl.SqlAlterTableModify;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRenameColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.ColumnReferenceFinder;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
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

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.utils.OperationConverterUtils.buildModifyColumnChange;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Converter to convert {@link SqlAlterTableSchema} with source table to generate new {@link
 * Schema}.
 */
public class AlterSchemaConverter {

    private static final String EX_MSG_PREFIX = "Failed to execute ALTER TABLE statement.\n";

    private final SqlValidator sqlValidator;
    private final Function<SqlNode, String> escapeExpression;
    private final CatalogManager catalogManager;

    AlterSchemaConverter(
            SqlValidator sqlValidator,
            Function<SqlNode, String> escapeExpression,
            CatalogManager catalogManager) {
        this.sqlValidator = sqlValidator;
        this.escapeExpression = escapeExpression;
        this.catalogManager = catalogManager;
    }

    /**
     * Convert ALTER TABLE ADD | MODIFY (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...])
     * to generate an updated Schema.
     */
    public Operation convertAlterSchema(
            SqlAlterTableSchema alterTableSchema, ResolvedCatalogTable oldTable) {
        SchemaConverter converter = createSchemaConverter(alterTableSchema, oldTable);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        return buildAlterTableChangeOperation(
                alterTableSchema, converter.changesCollector, converter.convert(), oldTable);
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

    private abstract static class SchemaConverter {

        List<String> sortedColumnNames = new ArrayList<>();
        Set<String> alterColNames = new HashSet<>();
        Map<String, Schema.UnresolvedColumn> columns = new HashMap<>();
        @Nullable Schema.UnresolvedWatermarkSpec watermarkSpec = null;
        @Nullable Schema.UnresolvedPrimaryKey primaryKey = null;

        Function<SqlNode, String> escapeExpressions;
        FlinkTypeFactory typeFactory;
        SqlValidator sqlValidator;
        SchemaResolver schemaResolver;

        List<TableChange> changesCollector;
        List<Function<ResolvedSchema, List<TableChange>>> changeBuilders = new ArrayList<>();

        SchemaConverter(
                Schema oldSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            this.typeFactory = typeFactory;
            this.sqlValidator = sqlValidator;
            this.escapeExpressions = escapeExpressions;
            this.schemaResolver = schemaResolver;
            this.changesCollector = new ArrayList<>();
            populateColumnsFromSourceTable(oldSchema);
            populatePrimaryKeyFromSourceTable(oldSchema);
            populateWatermarkFromSourceTable(oldSchema);
        }

        private void populateColumnsFromSourceTable(Schema oldSchema) {
            oldSchema
                    .getColumns()
                    .forEach(
                            column -> {
                                String name = column.getName();
                                sortedColumnNames.add(name);
                                columns.put(name, column);
                            });
        }

        private void populatePrimaryKeyFromSourceTable(Schema oldSchema) {
            if (oldSchema.getPrimaryKey().isPresent()) {
                primaryKey = oldSchema.getPrimaryKey().get();
            }
        }

        private void populateWatermarkFromSourceTable(Schema oldSchema) {
            for (Schema.UnresolvedWatermarkSpec sourceWatermarkSpec :
                    oldSchema.getWatermarkSpecs()) {
                watermarkSpec = sourceWatermarkSpec;
            }
        }

        private void updateColumn(List<SqlNode> alterColumnPositions) {
            applyColumnPosition(alterColumnPositions);
            for (SqlNode sqlNode : alterColumnPositions) {
                SqlTableColumnPosition alterColumnPos = (SqlTableColumnPosition) sqlNode;
                SqlTableColumn alterColumn = alterColumnPos.getColumn();
                Schema.UnresolvedColumn newColumn;
                if (alterColumn instanceof SqlTableColumn.SqlComputedColumn) {
                    newColumn =
                            convertComputedColumn((SqlTableColumn.SqlComputedColumn) alterColumn);
                } else if (alterColumn instanceof SqlTableColumn.SqlMetadataColumn) {
                    newColumn =
                            convertMetadataColumn((SqlTableColumn.SqlMetadataColumn) alterColumn);
                } else if (alterColumn instanceof SqlTableColumn.SqlRegularColumn) {
                    newColumn =
                            convertPhysicalColumn((SqlTableColumn.SqlRegularColumn) alterColumn);
                } else {
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Unsupported sql table column class: %s",
                                    alterColumn.getClass().getCanonicalName()));
                }
                columns.put(newColumn.getName(), newColumn);
            }
        }

        private void updatePrimaryKey(SqlTableConstraint alterPrimaryKey) {
            checkAndCollectPrimaryKeyChange();
            List<String> primaryKeyColumns = Arrays.asList(alterPrimaryKey.getColumnNames());
            primaryKey =
                    new Schema.UnresolvedPrimaryKey(
                            alterPrimaryKey
                                    .getConstraintName()
                                    .orElseGet(
                                            () ->
                                                    primaryKeyColumns.stream()
                                                            .collect(
                                                                    Collectors.joining(
                                                                            "_", "PK_", ""))),
                            primaryKeyColumns);
        }

        private void updatePrimaryKeyNullability(String columnName) {
            Schema.UnresolvedColumn column = columns.get(columnName);
            if (column instanceof Schema.UnresolvedPhysicalColumn) {
                AbstractDataType<?> oldType =
                        ((Schema.UnresolvedPhysicalColumn) column).getDataType();
                columns.put(
                        columnName,
                        new Schema.UnresolvedPhysicalColumn(
                                columnName, oldType.notNull(), column.getComment().orElse(null)));
            }
        }

        private void updateWatermark(SqlWatermark alterWatermarkSpec) {
            checkAndCollectWatermarkChange();
            SqlIdentifier eventTimeColumnName = alterWatermarkSpec.getEventTimeColumnName();
            if (!eventTimeColumnName.isSimple()) {
                throw new ValidationException(
                        String.format(
                                "%sWatermark strategy on nested column is not supported yet.",
                                EX_MSG_PREFIX));
            }
            watermarkSpec =
                    new Schema.UnresolvedWatermarkSpec(
                            eventTimeColumnName.getSimple(),
                            new SqlCallExpression(
                                    escapeExpressions.apply(
                                            alterWatermarkSpec.getWatermarkStrategy())));
        }

        Schema.UnresolvedPhysicalColumn convertPhysicalColumn(
                SqlTableColumn.SqlRegularColumn physicalColumn) {
            DataType dataType = getDataType(physicalColumn.getType());
            return new Schema.UnresolvedPhysicalColumn(
                    physicalColumn.getName().getSimple(), dataType, getComment(physicalColumn));
        }

        private Schema.UnresolvedMetadataColumn convertMetadataColumn(
                SqlTableColumn.SqlMetadataColumn metadataColumn) {
            DataType dataType = getDataType(metadataColumn.getType());
            return new Schema.UnresolvedMetadataColumn(
                    metadataColumn.getName().getSimple(),
                    dataType,
                    metadataColumn.getMetadataAlias().orElse(null),
                    metadataColumn.isVirtual(),
                    getComment(metadataColumn));
        }

        private Schema.UnresolvedComputedColumn convertComputedColumn(
                SqlTableColumn.SqlComputedColumn column) {
            return new Schema.UnresolvedComputedColumn(
                    column.getName().getSimple(),
                    new SqlCallExpression(escapeExpressions.apply(column.getExpr())),
                    getComment(column));
        }

        private DataType getDataType(SqlDataTypeSpec typeSpec) {
            RelDataType relType =
                    typeSpec.deriveType(
                            sqlValidator, typeSpec.getNullable() == null || typeSpec.getNullable());
            return fromLogicalToDataType(toLogicalType(relType));
        }

        @Nullable
        String getComment(SqlTableColumn column) {
            return OperationConverterUtils.getComment(column);
        }

        private void applyColumnPosition(List<SqlNode> alterColumns) {
            for (SqlNode alterColumn : alterColumns) {
                SqlTableColumnPosition columnPosition = (SqlTableColumnPosition) alterColumn;
                SqlTableColumn column = columnPosition.getColumn();
                String columnName = getColumnName(column.getName());
                if (!alterColNames.add(columnName)) {
                    throw new ValidationException(
                            String.format(
                                    "%sEncounter duplicate column `%s`.",
                                    EX_MSG_PREFIX, columnName));
                }
                updatePositionAndCollectColumnChange(columnPosition, columnName);
            }
        }

        protected String getReferencedColumn(SqlTableColumnPosition columnPosition) {
            SqlIdentifier referencedIdent = columnPosition.getAfterReferencedColumn();
            Preconditions.checkNotNull(
                    referencedIdent,
                    String.format("%sCould not refer to a null column", EX_MSG_PREFIX));
            if (!referencedIdent.isSimple()) {
                throw new UnsupportedOperationException(
                        String.format(
                                "%sAlter nested row type is not supported yet.", EX_MSG_PREFIX));
            }
            String referencedName = referencedIdent.getSimple();
            if (!sortedColumnNames.contains(referencedName)) {
                throw new ValidationException(
                        String.format(
                                "%sReferenced column `%s` by 'AFTER' does not exist in the table.",
                                EX_MSG_PREFIX, referencedName));
            }
            return referencedName;
        }

        private Schema convert() {
            Schema.Builder resultBuilder = Schema.newBuilder();
            if (primaryKey != null) {
                String constraintName = primaryKey.getConstraintName();
                List<String> pkColumns = primaryKey.getColumnNames();
                pkColumns.forEach(this::updatePrimaryKeyNullability);
                if (constraintName != null) {
                    resultBuilder.primaryKeyNamed(constraintName, pkColumns);
                } else {
                    resultBuilder.primaryKey(pkColumns);
                }
            }

            List<Schema.UnresolvedColumn> newColumns = new ArrayList<>();
            for (String column : sortedColumnNames) {
                newColumns.add(columns.get(column));
            }
            resultBuilder.fromColumns(newColumns);

            if (watermarkSpec != null) {
                resultBuilder.watermark(
                        watermarkSpec.getColumnName(), watermarkSpec.getWatermarkExpression());
            }
            Schema updatedSchema = resultBuilder.build();
            try {
                ResolvedSchema resolvedSchema = schemaResolver.resolve(updatedSchema);
                changesCollector.addAll(
                        changeBuilders.stream()
                                .flatMap(
                                        changeBuilder ->
                                                changeBuilder.apply(resolvedSchema).stream())
                                .collect(Collectors.toList()));
                return updatedSchema;
            } catch (Exception e) {
                throw new ValidationException(
                        String.format("%s%s", EX_MSG_PREFIX, e.getMessage()), e);
            }
        }

        abstract void updatePositionAndCollectColumnChange(
                SqlTableColumnPosition columnPosition, String columnName);

        abstract void checkAndCollectPrimaryKeyChange();

        abstract void checkAndCollectWatermarkChange();
    }

    private class AddSchemaConverter extends SchemaConverter {

        AddSchemaConverter(
                Schema oldSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            super(oldSchema, typeFactory, sqlValidator, escapeExpressions, schemaResolver);
        }

        @Override
        void checkAndCollectPrimaryKeyChange() {
            if (primaryKey != null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table has already defined the primary key constraint %s. You might "
                                        + "want to drop it before adding a new one.",
                                EX_MSG_PREFIX,
                                primaryKey.getColumnNames().stream()
                                        .collect(Collectors.joining("`, `", "[`", "`]"))));
            }
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.add(unwrap(schema.getPrimaryKey()))));
        }

        @Override
        void checkAndCollectWatermarkChange() {
            if (watermarkSpec != null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table has already defined the watermark strategy `%s` AS %s. You might "
                                        + "want to drop it before adding a new one.",
                                EX_MSG_PREFIX,
                                watermarkSpec.getColumnName(),
                                ((SqlCallExpression) watermarkSpec.getWatermarkExpression())
                                        .getSqlExpression()));
            }
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.add(schema.getWatermarkSpecs().get(0))));
        }

        @Override
        void updatePositionAndCollectColumnChange(
                SqlTableColumnPosition columnPosition, String columnName) {
            if (sortedColumnNames.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sTry to add a column `%s` which already exists in the table.",
                                EX_MSG_PREFIX, columnName));
            }

            if (columnPosition.isFirstColumn()) {
                changeBuilders.add(
                        schema ->
                                Collections.singletonList(
                                        TableChange.add(
                                                unwrap(schema.getColumn(columnName)),
                                                TableChange.ColumnPosition.first())));
                sortedColumnNames.add(0, columnName);
            } else if (columnPosition.isAfterReferencedColumn()) {
                String referenceName = getReferencedColumn(columnPosition);
                sortedColumnNames.add(sortedColumnNames.indexOf(referenceName) + 1, columnName);
                changeBuilders.add(
                        schema ->
                                Collections.singletonList(
                                        TableChange.add(
                                                unwrap(schema.getColumn(columnName)),
                                                TableChange.ColumnPosition.after(referenceName))));
            } else {
                changeBuilders.add(
                        schema ->
                                Collections.singletonList(
                                        TableChange.add(unwrap(schema.getColumn(columnName)))));
                sortedColumnNames.add(columnName);
            }
        }
    }

    private class ModifySchemaConverter extends SchemaConverter {

        private final ResolvedCatalogTable oldTable;

        ModifySchemaConverter(
                ResolvedCatalogTable oldTable,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            super(
                    oldTable.getUnresolvedSchema(),
                    typeFactory,
                    sqlValidator,
                    escapeExpressions,
                    schemaResolver);
            this.oldTable = oldTable;
        }

        @Override
        void updatePositionAndCollectColumnChange(
                SqlTableColumnPosition columnPosition, String columnName) {
            if (!sortedColumnNames.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sTry to modify a column `%s` which does not exist in the table.",
                                EX_MSG_PREFIX, columnName));
            }

            Column oldColumn = unwrap(oldTable.getResolvedSchema().getColumn(columnName));
            if (columnPosition.isFirstColumn()) {
                sortedColumnNames.remove(columnName);
                sortedColumnNames.add(0, columnName);

                changeBuilders.add(
                        schema ->
                                buildModifyColumnChange(
                                        oldColumn,
                                        unwrap(schema.getColumn(columnName)),
                                        TableChange.ColumnPosition.first()));
            } else if (columnPosition.isAfterReferencedColumn()) {
                String referenceName = getReferencedColumn(columnPosition);
                sortedColumnNames.remove(columnName);
                sortedColumnNames.add(sortedColumnNames.indexOf(referenceName) + 1, columnName);

                changeBuilders.add(
                        schema ->
                                buildModifyColumnChange(
                                        oldColumn,
                                        unwrap(schema.getColumn(columnName)),
                                        TableChange.ColumnPosition.after(referenceName)));
            } else {
                changeBuilders.add(
                        schema ->
                                buildModifyColumnChange(
                                        oldColumn, unwrap(schema.getColumn(columnName)), null));
            }
        }

        @Override
        void checkAndCollectPrimaryKeyChange() {
            if (primaryKey == null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any primary key constraint. You might "
                                        + "want to add a new one.",
                                EX_MSG_PREFIX));
            }
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.modify(unwrap(schema.getPrimaryKey()))));
        }

        @Override
        void checkAndCollectWatermarkChange() {
            if (watermarkSpec == null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any watermark. You might "
                                        + "want to add a new one.",
                                EX_MSG_PREFIX));
            }
            changeBuilders.add(
                    schema ->
                            Collections.singletonList(
                                    TableChange.modify(schema.getWatermarkSpecs().get(0))));
        }

        @Nullable
        @Override
        String getComment(SqlTableColumn column) {
            String comment = super.getComment(column);
            // update comment iff the alter table statement contains the field comment
            return comment == null
                    ? columns.get(column.getName().getSimple()).getComment().orElse(null)
                    : comment;
        }
    }

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

    private SchemaConverter createSchemaConverter(
            SqlAlterTableSchema alterTableSchema, ResolvedCatalogTable oldTable) {
        if (alterTableSchema instanceof SqlAlterTableAdd) {
            return new AddSchemaConverter(
                    oldTable.getUnresolvedSchema(),
                    (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                    sqlValidator,
                    escapeExpression,
                    catalogManager.getSchemaResolver());
        } else if (alterTableSchema instanceof SqlAlterTableModify) {
            return new ModifySchemaConverter(
                    oldTable,
                    (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                    sqlValidator,
                    escapeExpression,
                    catalogManager.getSchemaResolver());
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported alter table schema class: %s",
                        alterTableSchema.getClass().getCanonicalName()));
    }

    private <T> T unwrap(Optional<T> value) {
        return value.orElseThrow(() -> new TableException("The value should never be empty."));
    }
}

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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.ColumnReferenceFinder;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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
    private final Consumer<SqlTableConstraint> constraintValidator;
    private final SchemaResolver schemaResolver;

    AlterSchemaConverter(
            SqlValidator sqlValidator,
            Consumer<SqlTableConstraint> constraintValidator,
            Function<SqlNode, String> escapeExpression,
            SchemaResolver schemaResolver) {
        this.sqlValidator = sqlValidator;
        this.escapeExpression = escapeExpression;
        this.constraintValidator = constraintValidator;
        this.schemaResolver = schemaResolver;
    }

    /**
     * Convert ALTER TABLE ADD | MODIFY (&lt;schema_component&gt; [, &lt;schema_component&gt;, ...])
     * to generate an updated Schema.
     */
    public Schema applySchemaChange(
            SqlAlterTableSchema alterTableSchema,
            ResolvedCatalogTable oldTable,
            List<TableChange> tableChangeCollector) {
        AlterSchemaStrategy strategy = computeAlterSchemaStrategy(alterTableSchema);
        SchemaConverter converter =
                strategy == AlterSchemaStrategy.ADD
                        ? new AddSchemaConverter(
                                oldTable.getUnresolvedSchema(),
                                (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                                sqlValidator,
                                constraintValidator,
                                escapeExpression,
                                schemaResolver,
                                tableChangeCollector)
                        : new ModifySchemaConverter(
                                oldTable,
                                (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                                sqlValidator,
                                constraintValidator,
                                escapeExpression,
                                schemaResolver,
                                tableChangeCollector);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);
        return converter.convert();
    }

    /** Convert ALTER TABLE RENAME col_name to new_col_name to generate an updated Schema. */
    public Schema applySchemaChange(
            SqlAlterTableRenameColumn renameColumn,
            ResolvedCatalogTable oldTable,
            List<TableChange> tableChangeCollector) {
        String oldColumnName = getColumnName(renameColumn.getOldColumnIdentifier());
        String newColumnName = getColumnName(renameColumn.getNewColumnIdentifier());
        // validate old column is exists, new column name does not collide with existed column
        // names, and old column isn't referenced by computed column
        validateColumnName(
                oldColumnName,
                newColumnName,
                oldTable.getResolvedSchema(),
                oldTable.getPartitionKeys());
        validateWatermark(oldTable, oldColumnName);

        // generate new schema
        Schema.Builder schemaBuilder = Schema.newBuilder();
        buildUpdatedColumn(
                schemaBuilder,
                oldTable,
                (builder, column) -> {
                    if (column.getName().equals(oldColumnName)) {
                        buildNewColumnFromOldColumn(builder, column, newColumnName);
                        tableChangeCollector.add(
                                TableChange.modifyColumnName(
                                        unwrap(
                                                oldTable.getResolvedSchema()
                                                        .getColumn(oldColumnName)),
                                        newColumnName));
                    } else {
                        builder.fromColumns(Collections.singletonList(column));
                    }
                });
        buildUpdatedPrimaryKey(
                schemaBuilder, oldTable, (pk) -> pk.equals(oldColumnName) ? newColumnName : pk);
        buildUpdatedWatermark(schemaBuilder, oldTable);
        return schemaBuilder.build();
    }

    /** Convert ALTER TABLE DROP (col1 [, col2, ...]) to generate an updated Schema. */
    public Schema applySchemaChange(
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

        Schema.Builder schemaBuilder = Schema.newBuilder();
        for (SqlNode columnIdentifier : dropColumn.getColumnList()) {
            String columnToDrop = getColumnName((SqlIdentifier) columnIdentifier);
            // validate the column to drop exists in the table schema, is not a primary key and
            // does not derive any computed column
            validateColumnName(
                    columnToDrop,
                    oldTable.getResolvedSchema(),
                    oldTable.getPartitionKeys(),
                    columnsToDrop);
            validateWatermark(oldTable, columnToDrop);
        }
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
        return schemaBuilder.build();
    }

    /** Convert ALTER TABLE DROP PRIMARY KEY to generate an updated Schema. */
    public Schema applySchemaChange(
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
        return schemaBuilder.build();
    }

    /**
     * Convert ALTER TABLE DROP CONSTRAINT constraint_name to generate an updated {@link Schema}.
     */
    public Schema applySchemaChange(
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
        return schemaBuilder.build();
    }

    /** Convert ALTER TABLE DROP WATERMARK to generate an updated {@link Schema}. */
    public Schema applySchemaChange(
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
        return schemaBuilder.build();
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
        Consumer<SqlTableConstraint> constraintValidator;
        SchemaResolver schemaResolver;

        List<TableChange> changesCollector;
        List<Function<ResolvedSchema, List<TableChange>>> changeBuilders = new ArrayList<>();

        SchemaConverter(
                Schema oldSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver,
                List<TableChange> changesCollector) {
            this.typeFactory = typeFactory;
            this.sqlValidator = sqlValidator;
            this.constraintValidator = constraintValidator;
            this.escapeExpressions = escapeExpressions;
            this.schemaResolver = schemaResolver;
            this.changesCollector = changesCollector;
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
            constraintValidator.accept(alterPrimaryKey);
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
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver,
                List<TableChange> changeCollector) {
            super(
                    oldSchema,
                    typeFactory,
                    sqlValidator,
                    constraintValidator,
                    escapeExpressions,
                    schemaResolver,
                    changeCollector);
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
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver,
                List<TableChange> tableChangeCollector) {
            super(
                    oldTable.getUnresolvedSchema(),
                    typeFactory,
                    sqlValidator,
                    constraintValidator,
                    escapeExpressions,
                    schemaResolver,
                    tableChangeCollector);
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

    // --------------------------------------------------------------------------------------------

    private void validateColumnName(
            String oldColumnName,
            String newColumnName,
            ResolvedSchema oldSchema,
            List<String> partitionKeys) {
        validateColumnName(
                oldColumnName,
                oldSchema,
                partitionKeys,
                // fail the operation of renaming column, once the column derives a computed column
                (referencedColumn, computedColumn) -> referencedColumn.contains(oldColumnName));
        // validate new column
        if (oldSchema.getColumn(newColumnName).isPresent()) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` already existed in table schema.",
                            EX_MSG_PREFIX, newColumnName));
        }
    }

    private void validateColumnName(
            String columnToDrop,
            ResolvedSchema oldSchema,
            List<String> partitionKeys,
            Set<String> columnsToDrop) {
        validateColumnName(
                columnToDrop,
                oldSchema,
                partitionKeys,
                // fail the operation of dropping column, only if the column derives a computed
                // column, and the computed column is not being dropped along with the old column
                (referencedColumn, computedColumn) ->
                        referencedColumn.contains(columnToDrop)
                                && !columnsToDrop.contains(computedColumn.getName()));
        oldSchema
                .getPrimaryKey()
                .ifPresent(
                        pk -> {
                            if (pk.getColumns().contains(columnToDrop)) {
                                throw new ValidationException(
                                        String.format(
                                                "%sThe column `%s` is used as the primary key.",
                                                EX_MSG_PREFIX, columnToDrop));
                            }
                        });
    }

    private void validateColumnName(
            String columnToAlter,
            ResolvedSchema oldSchema,
            List<String> partitionKeys,
            BiFunction<Set<String>, Column.ComputedColumn, Boolean> computedColumnChecker) {
        // validate old column
        Set<String> tableColumns = new HashSet<>(oldSchema.getColumnNames());
        if (!tableColumns.contains(columnToAlter)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` does not exist in the base table.",
                            EX_MSG_PREFIX, columnToAlter));
        }

        // validate old column name isn't referred by computed column case
        oldSchema.getColumns().stream()
                .filter(column -> column instanceof Column.ComputedColumn)
                .forEach(
                        column -> {
                            Column.ComputedColumn computedColumn = (Column.ComputedColumn) column;
                            Set<String> referencedColumn =
                                    ColumnReferenceFinder.findReferencedColumn(
                                            computedColumn.getName(), oldSchema);
                            if (computedColumnChecker.apply(referencedColumn, computedColumn)) {
                                throw new ValidationException(
                                        String.format(
                                                "%sThe column `%s` is referenced by computed column %s.",
                                                EX_MSG_PREFIX,
                                                columnToAlter,
                                                computedColumn.asSummaryString()));
                            }
                        });
        // validate partition keys doesn't contain the old column
        if (partitionKeys.contains(columnToAlter)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` is used as the partition keys.",
                            EX_MSG_PREFIX, columnToAlter));
        }
    }

    private void validateWatermark(ResolvedCatalogTable oldTable, String columnToAlter) {
        // validate old column isn't referenced by watermark
        List<WatermarkSpec> watermarkSpecs = oldTable.getResolvedSchema().getWatermarkSpecs();
        Set<String> referencedColumns =
                ColumnReferenceFinder.findWatermarkReferencedColumn(oldTable.getResolvedSchema());
        Set<String> rowtimeAttributes =
                oldTable.getResolvedSchema().getWatermarkSpecs().stream()
                        .map(WatermarkSpec::getRowtimeAttribute)
                        .collect(Collectors.toSet());
        if (rowtimeAttributes.contains(columnToAlter)
                || referencedColumns.contains(columnToAlter)) {
            throw new ValidationException(
                    String.format(
                            "%sThe column `%s` is referenced by watermark expression %s.",
                            EX_MSG_PREFIX, columnToAlter, watermarkSpecs));
        }
    }

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

    private static String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAlter nested row type %s is not supported yet.",
                            EX_MSG_PREFIX, identifier));
        }
        return identifier.getSimple();
    }

    private AlterSchemaStrategy computeAlterSchemaStrategy(SqlAlterTableSchema alterTableSchema) {
        if (alterTableSchema instanceof SqlAlterTableAdd) {
            return AlterSchemaStrategy.ADD;
        } else if (alterTableSchema instanceof SqlAlterTableModify) {
            return AlterSchemaStrategy.MODIFY;
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported alter table schema class: %s",
                        alterTableSchema.getClass().getCanonicalName()));
    }

    private <T> T unwrap(Optional<T> value) {
        return value.orElseThrow(() -> new TableException("The value should never be empty."));
    }

    /** A strategy to describe the alter schema kind. */
    private enum AlterSchemaStrategy {
        ADD,
        MODIFY
    }
}

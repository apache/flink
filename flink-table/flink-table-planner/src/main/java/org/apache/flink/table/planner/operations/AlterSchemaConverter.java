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
import org.apache.flink.sql.parser.ddl.SqlAlterTableModify;
import org.apache.flink.sql.parser.ddl.SqlAlterTableRenameColumn;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.ColumnReferenceFinder;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCharStringLiteral;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
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

    public Schema applySchemaChange(SqlAlterTableSchema alterTableSchema, Schema originalSchema) {
        AlterSchemaStrategy strategy = computeAlterSchemaStrategy(alterTableSchema);
        SchemaConverter converter =
                strategy == AlterSchemaStrategy.ADD
                        ? new AddSchemaConverter(
                                originalSchema,
                                (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                                sqlValidator,
                                constraintValidator,
                                escapeExpression,
                                schemaResolver)
                        : new ModifySchemaConverter(
                                originalSchema,
                                (FlinkTypeFactory) sqlValidator.getTypeFactory(),
                                sqlValidator,
                                constraintValidator,
                                escapeExpression,
                                schemaResolver);
        converter.updateColumn(alterTableSchema.getColumnPositions().getList());
        alterTableSchema.getWatermark().ifPresent(converter::updateWatermark);
        alterTableSchema.getFullConstraint().ifPresent(converter::updatePrimaryKey);
        return converter.convert();
    }

    public Schema applySchemaChange(
            SqlAlterTableRenameColumn renameColumn, ContextResolvedTable originalTable) {
        String oldColumnName = getColumnName(renameColumn.getOriginColumnIdentifier());
        String newColumnName = getColumnName(renameColumn.getNewColumnIdentifier());
        List<String> tableColumns =
                originalTable.getResolvedSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList());
        // validate old column is exists or new column isn't duplicated or old column isn't
        // referenced by computed column
        validateColumnName(
                oldColumnName,
                newColumnName,
                tableColumns,
                originalTable.getResolvedSchema(),
                ((CatalogTable) originalTable.getResolvedTable()).getPartitionKeys());

        // validate old column isn't referenced by watermark
        List<WatermarkSpec> watermarkSpecs = originalTable.getResolvedSchema().getWatermarkSpecs();
        watermarkSpecs.forEach(
                watermarkSpec -> {
                    String rowtimeAttribute = watermarkSpec.getRowtimeAttribute();
                    Set<String> referencedColumns =
                            ColumnReferenceFinder.findReferencedColumn(
                                    watermarkSpec.getWatermarkExpression(), tableColumns);
                    if (oldColumnName.equals(rowtimeAttribute)
                            || referencedColumns.contains(oldColumnName)) {
                        throw new ValidationException(
                                String.format(
                                        "Old column %s is referred by watermark expression %s, "
                                                + "currently doesn't allow to rename column which is "
                                                + "referred by watermark expression.",
                                        oldColumnName, watermarkSpec.asSummaryString()));
                    }
                });

        Schema.Builder builder = Schema.newBuilder();
        // build column
        Schema originSchema = originalTable.getTable().getUnresolvedSchema();
        originSchema
                .getColumns()
                .forEach(
                        column -> {
                            if (oldColumnName.equals(column.getName())) {
                                buildNewColumnFromOriginColumn(builder, column, newColumnName);
                            } else {
                                builder.fromColumns(Collections.singletonList(column));
                            }
                        });
        // build primary key
        Optional<Schema.UnresolvedPrimaryKey> originPrimaryKey = originSchema.getPrimaryKey();
        if (originPrimaryKey.isPresent()) {
            List<String> originPrimaryKeyNames = originPrimaryKey.get().getColumnNames();
            String constrainName = originPrimaryKey.get().getConstraintName();
            List<String> newPrimaryKeyNames =
                    originPrimaryKeyNames.stream()
                            .map(pkName -> pkName.equals(oldColumnName) ? newColumnName : pkName)
                            .collect(Collectors.toList());
            builder.primaryKeyNamed(constrainName, newPrimaryKeyNames);
        }

        // build watermark
        originSchema
                .getWatermarkSpecs()
                .forEach(
                        watermarkSpec ->
                                builder.watermark(
                                        watermarkSpec.getColumnName(),
                                        watermarkSpec.getWatermarkExpression()));

        // generate new schema
        return builder.build();
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

        SchemaConverter(
                Schema originalSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            this.typeFactory = typeFactory;
            this.sqlValidator = sqlValidator;
            this.constraintValidator = constraintValidator;
            this.escapeExpressions = escapeExpressions;
            this.schemaResolver = schemaResolver;
            populateColumnsFromSourceTable(originalSchema);
            populatePrimaryKeyFromSourceTable(originalSchema);
            populateWatermarkFromSourceTable(originalSchema);
        }

        private void populateColumnsFromSourceTable(Schema originalSchema) {
            originalSchema
                    .getColumns()
                    .forEach(
                            column -> {
                                String name = column.getName();
                                sortedColumnNames.add(name);
                                columns.put(name, column);
                            });
        }

        private void populatePrimaryKeyFromSourceTable(Schema originalSchema) {
            if (originalSchema.getPrimaryKey().isPresent()) {
                primaryKey = originalSchema.getPrimaryKey().get();
            }
        }

        private void populateWatermarkFromSourceTable(Schema originalSchema) {
            for (Schema.UnresolvedWatermarkSpec sourceWatermarkSpec :
                    originalSchema.getWatermarkSpecs()) {
                watermarkSpec = sourceWatermarkSpec;
            }
        }

        private void updateColumn(List<SqlNode> alterColumnPositions) {
            applyColumnPosition(alterColumnPositions);
            for (SqlNode alterColumnPos : alterColumnPositions) {
                SqlTableColumn alterColumn = ((SqlTableColumnPosition) alterColumnPos).getColumn();
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
            checkPrimaryKeyExists();
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
                AbstractDataType<?> originalType =
                        ((Schema.UnresolvedPhysicalColumn) column).getDataType();
                columns.put(
                        columnName,
                        new Schema.UnresolvedPhysicalColumn(
                                columnName,
                                originalType.notNull(),
                                column.getComment().orElse(null)));
            }
        }

        private void updateWatermark(SqlWatermark alterWatermarkSpec) {
            checkWatermarkExists();
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
            return column.getComment()
                    .map(SqlCharStringLiteral.class::cast)
                    .map(c -> c.getValueAs(String.class))
                    .orElse(null);
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
                checkColumnExists(columnName);
                getColumnPosition(columnPosition)
                        .ifPresent(pos -> sortedColumnNames.add(pos, columnName));
            }
        }

        Optional<Integer> getColumnPosition(SqlTableColumnPosition columnPosition) {
            int pos = sortedColumnNames.size();
            if (columnPosition.isFirstColumn()) {
                pos = 0;
            } else if (columnPosition.isAfterReferencedColumn()) {
                pos = sortedColumnNames.indexOf(getReferencedColumn(columnPosition)) + 1;
            }
            return Optional.of(pos);
        }

        private String getReferencedColumn(SqlTableColumnPosition columnPosition) {
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
                schemaResolver.resolve(updatedSchema);
                return updatedSchema;
            } catch (Exception e) {
                throw new ValidationException(
                        String.format("%s%s", EX_MSG_PREFIX, e.getMessage()), e);
            }
        }

        abstract void checkColumnExists(String columnName);

        abstract void checkPrimaryKeyExists();

        abstract void checkWatermarkExists();
    }

    private static class AddSchemaConverter extends SchemaConverter {

        AddSchemaConverter(
                Schema originalSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            super(
                    originalSchema,
                    typeFactory,
                    sqlValidator,
                    constraintValidator,
                    escapeExpressions,
                    schemaResolver);
        }

        @Override
        void checkPrimaryKeyExists() {
            if (primaryKey != null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table has already defined the primary key constraint %s. You might "
                                        + "want to drop it before adding a new one.",
                                EX_MSG_PREFIX,
                                primaryKey.getColumnNames().stream()
                                        .collect(Collectors.joining("`, `", "[`", "`]"))));
            }
        }

        @Override
        void checkWatermarkExists() {
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
        }

        @Override
        void checkColumnExists(String columnName) {
            if (sortedColumnNames.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sTry to add a column `%s` which already exists in the table.",
                                EX_MSG_PREFIX, columnName));
            }
        }
    }

    private static class ModifySchemaConverter extends SchemaConverter {

        ModifySchemaConverter(
                Schema originalSchema,
                FlinkTypeFactory typeFactory,
                SqlValidator sqlValidator,
                Consumer<SqlTableConstraint> constraintValidator,
                Function<SqlNode, String> escapeExpressions,
                SchemaResolver schemaResolver) {
            super(
                    originalSchema,
                    typeFactory,
                    sqlValidator,
                    constraintValidator,
                    escapeExpressions,
                    schemaResolver);
        }

        @Override
        void checkColumnExists(String columnName) {
            if (!sortedColumnNames.contains(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sTry to modify a column `%s` which does not exist in the table.",
                                EX_MSG_PREFIX, columnName));
            }
        }

        @Override
        void checkPrimaryKeyExists() {
            if (primaryKey == null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any primary key constraint. You might "
                                        + "want to add a new one.",
                                EX_MSG_PREFIX));
            }
        }

        @Override
        void checkWatermarkExists() {
            if (watermarkSpec == null) {
                throw new ValidationException(
                        String.format(
                                "%sThe base table does not define any watermark. You might "
                                        + "want to add a new one.",
                                EX_MSG_PREFIX));
            }
        }

        @Override
        Optional<Integer> getColumnPosition(SqlTableColumnPosition columnPosition) {
            if (columnPosition.isFirstColumn() || columnPosition.isAfterReferencedColumn()) {
                sortedColumnNames.remove(columnPosition.getColumn().getName().getSimple());
                return super.getColumnPosition(columnPosition);
            }
            return Optional.empty();
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
            String originColumnName,
            String newColumnName,
            List<String> tableColumns,
            ResolvedSchema originResolvedSchema,
            List<String> partitionKeys) {
        // validate old column
        if (!tableColumns.contains(originColumnName)) {
            throw new ValidationException(
                    String.format(
                            "Old column %s not found in table schema for RENAME COLUMN",
                            originColumnName));
        }

        // validate new column
        if (tableColumns.contains(newColumnName)) {
            throw new ValidationException(
                    String.format(
                            "New column %s already existed in table schema for RENAME COLUMN",
                            newColumnName));
        }

        // validate old column name isn't referred by computed column case
        originResolvedSchema.getColumns().stream()
                .filter(column -> column instanceof Column.ComputedColumn)
                .forEach(
                        column -> {
                            Column.ComputedColumn computedColumn = (Column.ComputedColumn) column;
                            Set<String> referencedColumn =
                                    ColumnReferenceFinder.findReferencedColumn(
                                            computedColumn.getExpression(), tableColumns);
                            if (referencedColumn.contains(originColumnName)) {
                                throw new ValidationException(
                                        String.format(
                                                "Old column %s is referred by computed column %s, currently doesn't "
                                                        + "allow to rename column which is referred by computed column.",
                                                originColumnName,
                                                computedColumn.asSummaryString()));
                            }
                        });
        // validate partition keys doesn't contain the old column
        if (partitionKeys.contains(originColumnName)) {
            throw new ValidationException(
                    String.format(
                            "Can not rename column %s because it is used as the partition keys.",
                            originColumnName));
        }
    }

    private void buildNewColumnFromOriginColumn(
            Schema.Builder builder, Schema.UnresolvedColumn originColumn, String columnName) {
        if (originColumn instanceof Schema.UnresolvedComputedColumn) {
            builder.columnByExpression(
                    columnName, ((Schema.UnresolvedComputedColumn) originColumn).getExpression());
        } else if (originColumn instanceof Schema.UnresolvedPhysicalColumn) {
            builder.column(
                    columnName, ((Schema.UnresolvedPhysicalColumn) originColumn).getDataType());
        } else if (originColumn instanceof Schema.UnresolvedMetadataColumn) {
            Schema.UnresolvedMetadataColumn metadataColumn =
                    (Schema.UnresolvedMetadataColumn) originColumn;
            builder.columnByMetadata(
                    columnName,
                    metadataColumn.getDataType(),
                    metadataColumn.getMetadataKey(),
                    metadataColumn.isVirtual());
        }
        originColumn.getComment().ifPresent(builder::withComment);
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

    /** A strategy to describe the alter schema kind. */
    private enum AlterSchemaStrategy {
        ADD,
        MODIFY
    }
}

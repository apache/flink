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

import org.apache.flink.sql.parser.ddl.SqlAlterTableAdd;
import org.apache.flink.sql.parser.ddl.SqlAlterTableModify;
import org.apache.flink.sql.parser.ddl.SqlAlterTableSchema;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.SchemaResolver;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.operations.NopOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.operations.converters.AlterSchemaUtils.buildAlterTableChangeOperation;
import static org.apache.flink.table.planner.operations.converters.AlterSchemaUtils.getColumnName;
import static org.apache.flink.table.planner.operations.converters.AlterSchemaUtils.unwrap;
import static org.apache.flink.table.planner.utils.OperationConverterUtils.buildModifyColumnChange;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** An abstract generic converter for {@link SqlAlterTableSchema}. */
public abstract class SqlAlterTableSchemaConverter<T extends SqlAlterTableSchema>
        implements SqlNodeConverter<T> {

    private static final String EX_MSG_PREFIX = "Failed to execute ALTER TABLE statement.\n";

    @Override
    public Operation convertSqlNode(SqlAlterTableSchema node, ConvertContext context) {
        UnresolvedIdentifier unresolvedIdentifier = UnresolvedIdentifier.of(node.fullTableName());
        ObjectIdentifier tableIdentifier =
                context.getCatalogManager().qualifyIdentifier(unresolvedIdentifier);
        Optional<ContextResolvedTable> optionalCatalogTable =
                context.getCatalogManager().getTable(tableIdentifier);
        if (!optionalCatalogTable.isPresent() || optionalCatalogTable.get().isTemporary()) {
            if (node.ifTableExists()) {
                return new NopOperation();
            }
            throw new ValidationException(
                    String.format(
                            "Table %s doesn't exist or is a temporary table.", tableIdentifier));
        }
        CatalogBaseTable baseTable = optionalCatalogTable.get().getResolvedTable();
        if (baseTable instanceof CatalogView) {
            throw new ValidationException("ALTER TABLE for a view is not allowed");
        }

        ResolvedCatalogTable resolvedCatalogTable = (ResolvedCatalogTable) baseTable;

        SchemaConverter converter = createSchemaConverter(node, context, resolvedCatalogTable);
        converter.updateColumn(node.getColumnPositions().getList());
        node.getWatermark().ifPresent(converter::updateWatermark);
        node.getFullConstraint().ifPresent(converter::updatePrimaryKey);

        return buildAlterTableChangeOperation(
                tableIdentifier,
                node,
                converter.changesCollector,
                converter.convert(),
                resolvedCatalogTable);
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

    private static class AddSchemaConverter extends SchemaConverter {

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

    private static class ModifySchemaConverter extends SchemaConverter {

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

    // --------------------------------------------------------------------------------------------

    private SchemaConverter createSchemaConverter(
            SqlAlterTableSchema alterTableSchema,
            ConvertContext context,
            ResolvedCatalogTable oldTable) {
        if (alterTableSchema instanceof SqlAlterTableAdd) {
            return new AddSchemaConverter(
                    oldTable.getUnresolvedSchema(),
                    (FlinkTypeFactory) context.getSqlValidator().getTypeFactory(),
                    context.getSqlValidator(),
                    context::toQuotedSqlString,
                    context.getCatalogManager().getSchemaResolver());
        } else if (alterTableSchema instanceof SqlAlterTableModify) {
            return new ModifySchemaConverter(
                    oldTable,
                    (FlinkTypeFactory) context.getSqlValidator().getTypeFactory(),
                    context.getSqlValidator(),
                    context::toQuotedSqlString,
                    context.getCatalogManager().getSchemaResolver());
        }
        throw new UnsupportedOperationException(
                String.format(
                        "Unsupported alter table schema class: %s",
                        alterTableSchema.getClass().getCanonicalName()));
    }
}

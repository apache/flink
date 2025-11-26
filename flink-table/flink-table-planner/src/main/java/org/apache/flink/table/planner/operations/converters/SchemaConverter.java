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

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlWatermark;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable.TableKind;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;
import org.apache.flink.table.planner.utils.OperationConverterUtils;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** Base class for schema conversion operations. */
public abstract class SchemaConverter {
    private static final String ERROR_TEMPLATE = "Failed to execute ALTER %s statement.\n";
    protected final String exMsgPrefix;
    protected final String tableKindStr;
    protected List<String> sortedColumnNames = new ArrayList<>();
    protected Set<String> alterColNames = new HashSet<>();
    protected Map<String, Schema.UnresolvedColumn> columns = new HashMap<>();
    protected @Nullable Schema.UnresolvedWatermarkSpec watermarkSpec = null;
    protected @Nullable Schema.UnresolvedPrimaryKey primaryKey = null;

    protected Function<SqlNode, String> escapeExpressions;
    protected ConvertContext context;

    protected List<TableChange> changesCollector;
    protected List<Function<ResolvedSchema, List<TableChange>>> changeBuilders = new ArrayList<>();

    SchemaConverter(ResolvedCatalogBaseTable<?> oldTable, ConvertContext context) {
        this.changesCollector = new ArrayList<>();
        final TableKind tableKind = oldTable.getTableKind();
        this.tableKindStr = tableKind.toString().toLowerCase(Locale.ROOT).replace('_', ' ');
        this.exMsgPrefix = String.format(ERROR_TEMPLATE, tableKindStr.toUpperCase(Locale.ROOT));
        this.context = context;
        this.escapeExpressions =
                sqlNode ->
                        OperationConverterUtils.getQuotedSqlString(
                                sqlNode, context.getFlinkPlanner());
        Schema oldSchema = oldTable.getUnresolvedSchema();
        populateColumnsFromSourceTable(oldSchema);
        populatePrimaryKeyFromSourceTable(oldSchema);
        populateWatermarkFromSourceTable(oldSchema);
    }

    public List<TableChange> getChangesCollector() {
        return changesCollector;
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
        for (Schema.UnresolvedWatermarkSpec sourceWatermarkSpec : oldSchema.getWatermarkSpecs()) {
            watermarkSpec = sourceWatermarkSpec;
        }
    }

    public void updateColumn(List<SqlNode> alterColumnPositions) {
        applyColumnPosition(alterColumnPositions);
        for (SqlNode sqlNode : alterColumnPositions) {
            SqlTableColumnPosition alterColumnPos = (SqlTableColumnPosition) sqlNode;
            SqlTableColumn alterColumn = alterColumnPos.getColumn();
            Schema.UnresolvedColumn newColumn;
            if (alterColumn instanceof SqlTableColumn.SqlComputedColumn) {
                newColumn = convertComputedColumn((SqlTableColumn.SqlComputedColumn) alterColumn);
            } else if (alterColumn instanceof SqlTableColumn.SqlMetadataColumn) {
                newColumn = convertMetadataColumn((SqlTableColumn.SqlMetadataColumn) alterColumn);
            } else if (alterColumn instanceof SqlTableColumn.SqlRegularColumn) {
                newColumn = convertPhysicalColumn((SqlTableColumn.SqlRegularColumn) alterColumn);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported sql table column class: %s",
                                alterColumn.getClass().getCanonicalName()));
            }
            columns.put(newColumn.getName(), newColumn);
        }
    }

    public void updatePrimaryKey(SqlTableConstraint alterPrimaryKey) {
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
            AbstractDataType<?> oldType = ((Schema.UnresolvedPhysicalColumn) column).getDataType();
            columns.put(
                    columnName,
                    new Schema.UnresolvedPhysicalColumn(
                            columnName, oldType.notNull(), column.getComment().orElse(null)));
        }
    }

    public void updateWatermark(SqlWatermark alterWatermarkSpec) {
        checkAndCollectWatermarkChange();
        SqlIdentifier eventTimeColumnName = alterWatermarkSpec.getEventTimeColumnName();
        if (!eventTimeColumnName.isSimple()) {
            throw new ValidationException(
                    String.format(
                            "%sWatermark strategy on nested column is not supported yet.",
                            exMsgPrefix));
        }
        watermarkSpec =
                new Schema.UnresolvedWatermarkSpec(
                        eventTimeColumnName.getSimple(),
                        new SqlCallExpression(
                                escapeExpressions.apply(
                                        alterWatermarkSpec.getWatermarkStrategy())));
    }

    private Schema.UnresolvedPhysicalColumn convertPhysicalColumn(
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
                        context.getSqlValidator(),
                        typeSpec.getNullable() == null || typeSpec.getNullable());
        return fromLogicalToDataType(toLogicalType(relType));
    }

    @Nullable
    protected String getComment(SqlTableColumn column) {
        return column.getComment();
    }

    private void applyColumnPosition(List<SqlNode> alterColumns) {
        for (SqlNode alterColumn : alterColumns) {
            SqlTableColumnPosition columnPosition = (SqlTableColumnPosition) alterColumn;
            SqlTableColumn column = columnPosition.getColumn();
            String columnName = getColumnName(column.getName());
            if (!alterColNames.add(columnName)) {
                throw new ValidationException(
                        String.format(
                                "%sEncounter duplicate column `%s`.", exMsgPrefix, columnName));
            }
            updatePositionAndCollectColumnChange(columnPosition, columnName);
        }
    }

    protected String getReferencedColumn(SqlTableColumnPosition columnPosition) {
        SqlIdentifier referencedIdent = columnPosition.getAfterReferencedColumn();
        Preconditions.checkNotNull(
                referencedIdent, String.format("%sCould not refer to a null column", exMsgPrefix));
        if (!referencedIdent.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format("%sAlter nested row type is not supported yet.", exMsgPrefix));
        }
        String referencedName = referencedIdent.getSimple();
        if (!sortedColumnNames.contains(referencedName)) {
            throw new ValidationException(
                    String.format(
                            "%sReferenced column `%s` by 'AFTER' does not exist in the table.",
                            exMsgPrefix, referencedName));
        }
        return referencedName;
    }

    public Schema convert() {
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
            ResolvedSchema resolvedSchema =
                    context.getCatalogManager().getSchemaResolver().resolve(updatedSchema);
            changesCollector.addAll(
                    changeBuilders.stream()
                            .flatMap(changeBuilder -> changeBuilder.apply(resolvedSchema).stream())
                            .collect(Collectors.toList()));
            return updatedSchema;
        } catch (Exception e) {
            throw new ValidationException(String.format("%s%s", exMsgPrefix, e.getMessage()), e);
        }
    }

    protected abstract void updatePositionAndCollectColumnChange(
            SqlTableColumnPosition columnPosition, String columnName);

    protected abstract void checkAndCollectPrimaryKeyChange();

    protected abstract void checkAndCollectWatermarkChange();

    protected String getColumnName(SqlIdentifier identifier) {
        if (!identifier.isSimple()) {
            throw new UnsupportedOperationException(
                    String.format(
                            "%sAlter nested row type %s is not supported yet.",
                            exMsgPrefix, identifier));
        }
        return identifier.getSimple();
    }

    protected <T> T unwrap(Optional<T> value) {
        return value.orElseThrow(() -> new TableException("The value should never be empty."));
    }
}

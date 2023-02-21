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

package org.apache.flink.table.planner.utils;

import org.apache.flink.sql.parser.ddl.SqlAddReplaceColumns;
import org.apache.flink.sql.parser.ddl.SqlChangeColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableChangeOperation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** Utils methods for converting sql to operations. */
public class OperationConverterUtils {

    private OperationConverterUtils() {}

    public static Operation convertAddReplaceColumns(
            ObjectIdentifier tableIdentifier,
            SqlAddReplaceColumns addReplaceColumns,
            CatalogTable catalogTable,
            SqlValidator sqlValidator) {
        // This is only used by the Hive dialect at the moment. In Hive, only non-partition columns
        // can be added/replaced and users will only define non-partition columns in the new column
        // list. Therefore, we require that partitions columns must appear last in the schema (which
        // is inline with Hive). Otherwise, we won't be able to determine the column positions after
        // the non-partition columns are replaced.
        TableSchema oldSchema = catalogTable.getSchema();
        int numPartCol = catalogTable.getPartitionKeys().size();
        Set<String> lastCols =
                oldSchema.getTableColumns()
                        .subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount())
                        .stream()
                        .map(TableColumn::getName)
                        .collect(Collectors.toSet());
        if (!lastCols.equals(new HashSet<>(catalogTable.getPartitionKeys()))) {
            throw new ValidationException(
                    "ADD/REPLACE COLUMNS on partitioned tables requires partition columns to appear last");
        }

        // set non-partition columns
        TableSchema.Builder builder = TableSchema.builder();
        if (!addReplaceColumns.isReplace()) {
            List<TableColumn> nonPartCols =
                    oldSchema.getTableColumns().subList(0, oldSchema.getFieldCount() - numPartCol);
            for (TableColumn column : nonPartCols) {
                builder.add(column);
            }
            setWatermarkAndPK(builder, catalogTable.getSchema());
        }
        List<TableChange> tableChanges = new ArrayList<>();
        for (SqlNode sqlNode : addReplaceColumns.getNewColumns()) {
            TableColumn tableColumn = toTableColumn((SqlTableColumn) sqlNode, sqlValidator);
            builder.add(tableColumn);
            if (!addReplaceColumns.isReplace()) {
                tableChanges.add(
                        TableChange.add(
                                Column.physical(tableColumn.getName(), tableColumn.getType())
                                        .withComment(getComment((SqlTableColumn) sqlNode))));
            }
        }

        // set partition columns
        List<TableColumn> partCols =
                oldSchema
                        .getTableColumns()
                        .subList(oldSchema.getFieldCount() - numPartCol, oldSchema.getFieldCount());
        for (TableColumn column : partCols) {
            builder.add(column);
        }

        // set properties
        Map<String, String> newProperties = new HashMap<>(catalogTable.getOptions());
        newProperties.putAll(extractProperties(addReplaceColumns.getProperties()));

        CatalogTableImpl newTable =
                new CatalogTableImpl(
                        builder.build(),
                        catalogTable.getPartitionKeys(),
                        newProperties,
                        catalogTable.getComment());
        if (addReplaceColumns.isReplace()) {
            // It's hard to determine how to decompose the ALTER TABLE REPLACE into multiple
            // TableChanges. For example, with old schema <a INT, b INT, c INT> and the new schema
            // <a INT, d INT>, there are multiple alternatives:
            // plan 1: DROP COLUMN c, RENAME COLUMN b TO d;
            // plan 2: DROP COLUMN b, RENAME COLUMN c TO d;
            // So we don't translate with TableChanges here. One workaround is
            // to minimize the edit distance, i.e., minimize the modification times, but it
            // still cannot provide a deterministic answer.
            return new AlterTableSchemaOperation(
                    tableIdentifier, newTable, addReplaceColumns.ifTableExists());
        } else {
            return new AlterTableChangeOperation(
                    tableIdentifier, tableChanges, newTable, addReplaceColumns.ifTableExists());
        }
    }

    public static Operation convertChangeColumn(
            ObjectIdentifier tableIdentifier,
            SqlChangeColumn changeColumn,
            ResolvedCatalogTable catalogTable,
            SqlValidator sqlValidator) {
        String oldName = changeColumn.getOldName().getSimple();
        if (catalogTable.getPartitionKeys().indexOf(oldName) >= 0) {
            // disallow changing partition columns
            throw new ValidationException("CHANGE COLUMN cannot be applied to partition columns");
        }
        TableSchema oldSchema = catalogTable.getSchema();
        boolean first = changeColumn.isFirst();
        String after = changeColumn.getAfter() == null ? null : changeColumn.getAfter().getSimple();
        TableColumn.PhysicalColumn newTableColumn =
                toTableColumn(changeColumn.getNewColumn(), sqlValidator);
        TableSchema newSchema = changeColumn(oldSchema, oldName, newTableColumn, first, after);
        Map<String, String> newProperties = new HashMap<>(catalogTable.getOptions());
        newProperties.putAll(extractProperties(changeColumn.getProperties()));

        List<TableChange> tableChanges =
                buildModifyColumnChange(
                        catalogTable
                                .getResolvedSchema()
                                .getColumn(oldName)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Failed to get old column: " + oldName)),
                        Column.physical(newTableColumn.getName(), newTableColumn.getType())
                                .withComment(getComment(changeColumn.getNewColumn())),
                        first
                                ? TableChange.ColumnPosition.first()
                                : (after == null ? null : TableChange.ColumnPosition.after(after)));
        return new AlterTableChangeOperation(
                tableIdentifier,
                tableChanges,
                new CatalogTableImpl(
                        newSchema,
                        catalogTable.getPartitionKeys(),
                        newProperties,
                        catalogTable.getComment()),
                false);
        // TODO: handle watermark and constraints
    }

    public static List<TableChange> buildModifyColumnChange(
            Column oldColumn,
            Column newColumn,
            @Nullable TableChange.ColumnPosition columnPosition) {
        if (oldColumn.isPhysical() && newColumn.isPhysical()) {
            List<TableChange> changes = new ArrayList<>();
            String newComment = newColumn.getComment().orElse(oldColumn.getComment().orElse(null));
            if (!newColumn.getComment().equals(oldColumn.getComment())) {
                changes.add(TableChange.modifyColumnComment(oldColumn, newComment));
            }

            if (!oldColumn
                    .getDataType()
                    .getLogicalType()
                    .equals(newColumn.getDataType().getLogicalType())) {
                changes.add(
                        TableChange.modifyPhysicalColumnType(
                                oldColumn.withComment(newComment), newColumn.getDataType()));
            }

            if (!Objects.equals(newColumn.getName(), oldColumn.getName())) {
                changes.add(
                        TableChange.modifyColumnName(
                                oldColumn.withComment(newComment).copy(newColumn.getDataType()),
                                newColumn.getName()));
            }

            if (columnPosition != null) {
                changes.add(TableChange.modifyColumnPosition(newColumn, columnPosition));
            }

            return changes;
        } else {
            return Collections.singletonList(
                    TableChange.modify(oldColumn, newColumn, columnPosition));
        }
    }

    // change a column in the old table schema and return the updated table schema
    public static TableSchema changeColumn(
            TableSchema oldSchema,
            String oldName,
            TableColumn newTableColumn,
            boolean first,
            String after) {
        int oldIndex = Arrays.asList(oldSchema.getFieldNames()).indexOf(oldName);
        if (oldIndex < 0) {
            throw new ValidationException(
                    String.format("Old column %s not found for CHANGE COLUMN", oldName));
        }
        List<TableColumn> tableColumns = oldSchema.getTableColumns();
        if ((!first && after == null) || oldName.equals(after)) {
            tableColumns.set(oldIndex, newTableColumn);
        } else {
            // need to change column position
            tableColumns.remove(oldIndex);
            if (first) {
                tableColumns.add(0, newTableColumn);
            } else {
                int newIndex =
                        tableColumns.stream()
                                .map(TableColumn::getName)
                                .collect(Collectors.toList())
                                .indexOf(after);
                if (newIndex < 0) {
                    throw new ValidationException(
                            String.format("After column %s not found for CHANGE COLUMN", after));
                }
                tableColumns.add(newIndex + 1, newTableColumn);
            }
        }
        TableSchema.Builder builder = TableSchema.builder();
        for (TableColumn column : tableColumns) {
            builder.add(column);
        }
        setWatermarkAndPK(builder, oldSchema);
        return builder.build();
    }

    public static @Nullable String getComment(SqlTableColumn column) {
        return column.getComment()
                .map(SqlCharStringLiteral.class::cast)
                .map(c -> c.getValueAs(String.class))
                .orElse(null);
    }

    private static TableColumn.PhysicalColumn toTableColumn(
            SqlTableColumn tableColumn, SqlValidator sqlValidator) {
        if (!(tableColumn instanceof SqlRegularColumn)) {
            throw new TableException("Only regular columns are supported for this operation yet.");
        }
        SqlRegularColumn regularColumn = (SqlRegularColumn) tableColumn;
        String name = regularColumn.getName().getSimple();
        SqlDataTypeSpec typeSpec = regularColumn.getType();
        boolean nullable = typeSpec.getNullable() == null || typeSpec.getNullable();
        LogicalType logicalType =
                FlinkTypeFactory.toLogicalType(typeSpec.deriveType(sqlValidator, nullable));
        DataType dataType = TypeConversions.fromLogicalToDataType(logicalType);
        return TableColumn.physical(name, dataType);
    }

    private static void setWatermarkAndPK(TableSchema.Builder builder, TableSchema schema) {
        for (WatermarkSpec watermarkSpec : schema.getWatermarkSpecs()) {
            builder.watermark(watermarkSpec);
        }
        schema.getPrimaryKey()
                .ifPresent(
                        pk -> {
                            builder.primaryKey(
                                    pk.getName(), pk.getColumns().toArray(new String[0]));
                        });
    }

    public static Map<String, String> extractProperties(SqlNodeList propList) {
        Map<String, String> properties = new HashMap<>();
        if (propList != null) {
            propList.getList()
                    .forEach(
                            p ->
                                    properties.put(
                                            ((SqlTableOption) p).getKeyString(),
                                            ((SqlTableOption) p).getValueString()));
        }
        return properties;
    }
}

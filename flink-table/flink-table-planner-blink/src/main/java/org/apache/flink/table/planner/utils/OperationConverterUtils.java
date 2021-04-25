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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.SqlCallExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterTableSchemaOperation;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
        // can be
        // added/replaced and users will only define non-partition columns in the new column list.
        // Therefore, we require
        // that partitions columns must appear last in the schema (which is inline with Hive).
        // Otherwise, we won't be
        // able to determine the column positions after the non-partition columns are replaced.
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
        for (SqlNode sqlNode : addReplaceColumns.getNewColumns()) {
            builder.add(toTableColumn((SqlTableColumn) sqlNode, sqlValidator));
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

        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        builder.build(),
                        catalogTable.getPartitionKeys(),
                        newProperties,
                        catalogTable.getComment()));
    }

    public static Operation convertChangeColumn(
            ObjectIdentifier tableIdentifier,
            SqlChangeColumn changeColumn,
            CatalogTable catalogTable,
            SqlValidator sqlValidator) {
        String oldName = changeColumn.getOldName().getSimple();
        if (catalogTable.getPartitionKeys().indexOf(oldName) >= 0) {
            // disallow changing partition columns
            throw new ValidationException("CHANGE COLUMN cannot be applied to partition columns");
        }
        TableSchema oldSchema = catalogTable.getSchema();
        boolean first = changeColumn.isFirst();
        String after = changeColumn.getAfter() == null ? null : changeColumn.getAfter().getSimple();
        TableColumn newTableColumn = toTableColumn(changeColumn.getNewColumn(), sqlValidator);
        TableSchema newSchema = changeColumn(oldSchema, oldName, newTableColumn, first, after);
        Map<String, String> newProperties = new HashMap<>(catalogTable.getOptions());
        newProperties.putAll(extractProperties(changeColumn.getProperties()));
        return new AlterTableSchemaOperation(
                tableIdentifier,
                new CatalogTableImpl(
                        newSchema,
                        catalogTable.getPartitionKeys(),
                        newProperties,
                        catalogTable.getComment()));
        // TODO: handle watermark and constraints
    }

    public static Operation convertRenameColumn(
            ObjectIdentifier tableIdentifier,
            String originColumnName,
            String newColumnName,
            CatalogTable catalogTable) {

        Schema modifiedTableSchema = catalogTable.getUnresolvedSchema();
        validateColumnName(originColumnName, newColumnName, modifiedTableSchema);

        Schema.Builder builder = Schema.newBuilder();
        // build column
        modifiedTableSchema.getColumns().stream()
                .forEach(
                        column -> {
                            if (StringUtils.equals(column.getName(), originColumnName)) {
                                buildNewColumnFromOriginColumn(builder, column, newColumnName);
                            } else {
                                buildNewColumnFromOriginColumn(builder, column, column.getName());
                            }
                        });
        // build primary key column
        List<String> originPrimaryKeyNames =
                modifiedTableSchema
                        .getPrimaryKey()
                        .map(Schema.UnresolvedPrimaryKey::getColumnNames)
                        .orElseGet(Lists::newArrayList);

        List<String> newPrimaryKeyNames =
                originPrimaryKeyNames.stream()
                        .map(
                                pkName ->
                                        StringUtils.equals(pkName, originColumnName)
                                                ? newColumnName
                                                : pkName)
                        .collect(Collectors.toList());

        if (newPrimaryKeyNames.size() > 0) {
            builder.primaryKey(newPrimaryKeyNames);
        }
        // build watermark
        modifiedTableSchema.getWatermarkSpecs().stream()
                .forEach(
                        watermarkSpec -> {
                            String watermarkRefColumnName = watermarkSpec.getColumnName();
                            Expression watermarkExpression = watermarkSpec.getWatermarkExpression();
                            if (StringUtils.equals(watermarkRefColumnName, originColumnName)) {
                                String newWatermarkExpression =
                                        ((SqlCallExpression) watermarkExpression)
                                                .getSqlExpression()
                                                .replace(watermarkRefColumnName, newColumnName);
                                builder.watermark(newColumnName, newWatermarkExpression);
                            } else {
                                builder.watermark(watermarkRefColumnName, watermarkExpression);
                            }
                        });
        // build partition key
        List<String> newPartitionKeys =
                catalogTable.getPartitionKeys().stream()
                        .map(
                                name ->
                                        StringUtils.equals(name, originColumnName)
                                                ? newColumnName
                                                : name)
                        .collect(Collectors.toList());
        // generate new schema
        Schema newSchema = builder.build();

        return new AlterTableSchemaOperation(
                tableIdentifier,
                CatalogTable.of(
                        newSchema,
                        catalogTable.getComment(),
                        newPartitionKeys,
                        catalogTable.getOptions()));
    }

    private static void validateColumnName(
            String originColumnName, String newColumnName, Schema modifiedTableSchema) {
        List<String> columnNames =
                modifiedTableSchema.getColumns().stream()
                        .map(Schema.UnresolvedColumn::getName)
                        .collect(Collectors.toList());

        int originColumnIndex = columnNames.indexOf(originColumnName);
        if (originColumnIndex < 0) {
            throw new ValidationException(
                    String.format("Old column %s not found for RENAME COLUMN ", originColumnName));
        }

        int sameColumnNameIndex = columnNames.indexOf(newColumnName);
        if (sameColumnNameIndex >= 0) {
            throw new ValidationException(
                    String.format("New column %s existed for RENAME COLUMN ", newColumnName));
        }
    }

    private static void buildNewColumnFromOriginColumn(
            Schema.Builder builder, Schema.UnresolvedColumn originColumn, String columnName) {
        if (originColumn instanceof Schema.UnresolvedComputedColumn) {
            builder.columnByExpression(
                    columnName, ((Schema.UnresolvedComputedColumn) originColumn).getExpression());
        } else if (originColumn instanceof Schema.UnresolvedPhysicalColumn) {
            builder.column(
                    columnName, ((Schema.UnresolvedPhysicalColumn) originColumn).getDataType());
        } else {
            builder.columnByMetadata(
                    columnName, ((Schema.UnresolvedMetadataColumn) originColumn).getDataType());
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

    private static TableColumn toTableColumn(
            SqlTableColumn tableColumn, SqlValidator sqlValidator) {
        if (!(tableColumn instanceof SqlRegularColumn)) {
            throw new TableException("Only regular columns are supported for this operation yet.");
        }
        SqlRegularColumn regularColumn = (SqlRegularColumn) tableColumn;
        String name = regularColumn.getName().getSimple();
        SqlDataTypeSpec typeSpec = regularColumn.getType();
        boolean nullable = typeSpec.getNullable() == null ? true : typeSpec.getNullable();
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

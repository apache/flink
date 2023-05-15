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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Utils methods for converting table schema. */
public class TableSchemaUtils {
    private TableSchemaUtils() {}

    public static ResolvedSchema resolvedSchema(RelNode calciteTree) {
        RelDataType rowType = calciteTree.getRowType();
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);

        DataType[] fieldTypes =
                rowType.getFieldList().stream()
                        .map(
                                field ->
                                        TypeConversions.fromLogicalToDataType(
                                                FlinkTypeUtils.toLogicalType(field.getType())))
                        .toArray(DataType[]::new);

        return ResolvedSchema.physical(fieldNames, fieldTypes);
    }

    /**
     * This method is copied from OperationConverterUtils#buildModifyColumnChange located in
     * flink-table-planner, and should be synchronized if
     * OperationConverterUtils#buildModifyColumnChange changes in the the future.
     */
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

    /** Change a column in the old table schema and return the updated table schema. */
    public static ResolvedSchema changeColumn(
            ResolvedSchema oldSchema,
            String oldName,
            Column newTableColumn,
            boolean first,
            String after) {
        int oldIndex = oldSchema.getColumnNames().indexOf(oldName);
        if (oldIndex < 0) {
            throw new ValidationException(
                    String.format("Old column %s not found for CHANGE COLUMN", oldName));
        }
        List<Column> tableColumns = new ArrayList<>(oldSchema.getColumns());
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
                                .map(Column::getName)
                                .collect(Collectors.toList())
                                .indexOf(after);
                if (newIndex < 0) {
                    throw new ValidationException(
                            String.format("After column %s not found for CHANGE COLUMN", after));
                }
                tableColumns.add(newIndex + 1, newTableColumn);
            }
        }
        return new ResolvedSchema(
                tableColumns,
                oldSchema.getWatermarkSpecs(),
                oldSchema.getPrimaryKey().orElse(null));
    }
}

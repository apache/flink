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
import org.apache.flink.sql.parser.ddl.position.SqlTableColumnPosition;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedCatalogBaseTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.planner.operations.converters.SqlNodeConverter.ConvertContext;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Converter for ALTER [MATERIALIZED ]TABLE MODIFY ... schema operations. */
public class SchemaModifyConverter extends SchemaConverter {

    private final ResolvedCatalogBaseTable<?> oldBaseTable;

    public SchemaModifyConverter(ResolvedCatalogBaseTable<?> oldBaseTable, ConvertContext context) {
        super(oldBaseTable, context);
        this.oldBaseTable = oldBaseTable;
    }

    @Override
    protected void updatePositionAndCollectColumnChange(
            SqlTableColumnPosition columnPosition, String columnName) {
        if (!sortedColumnNames.contains(columnName)) {
            throw new ValidationException(
                    String.format(
                            "%sColumn `%s` does not exist in the table.", exMsgPrefix, columnName));
        }

        Column oldColumn = unwrap(oldBaseTable.getResolvedSchema().getColumn(columnName));
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
    protected void checkAndCollectPrimaryKeyChange() {
        if (primaryKey == null) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define any primary key constraint. You might "
                                    + "want to add a new one.",
                            exMsgPrefix, tableKindStr));
        }
        changeBuilders.add(
                schema ->
                        Collections.singletonList(
                                TableChange.modify(unwrap(schema.getPrimaryKey()))));
    }

    @Override
    protected void checkAndCollectWatermarkChange() {
        if (watermarkSpec == null) {
            throw new ValidationException(
                    String.format(
                            "%sThe current %s does not define any watermark. You might "
                                    + "want to add a new one.",
                            exMsgPrefix, tableKindStr));
        }
        changeBuilders.add(
                schema ->
                        Collections.singletonList(
                                TableChange.modify(schema.getWatermarkSpecs().get(0))));
    }

    @Nullable
    @Override
    protected String getComment(SqlTableColumn column) {
        String comment = super.getComment(column);
        // update comment iff the alter table statement contains the field comment
        return comment == null
                ? columns.get(column.getName().getSimple()).getComment().orElse(null)
                : comment;
    }

    private static List<TableChange> buildModifyColumnChange(
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
}

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

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.TableChange;

import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlNodeList;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Utils methods for converting sql to operations. */
public class OperationConverterUtils {

    private OperationConverterUtils() {}

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

    public static @Nullable String getComment(SqlTableColumn column) {
        return column.getComment()
                .map(SqlCharStringLiteral.class::cast)
                .map(c -> c.getValueAs(String.class))
                .orElse(null);
    }

    public static @Nullable String getTableComment(Optional<SqlCharStringLiteral> tableComment) {
        return tableComment.map(comment -> comment.getValueAs(String.class)).orElse(null);
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

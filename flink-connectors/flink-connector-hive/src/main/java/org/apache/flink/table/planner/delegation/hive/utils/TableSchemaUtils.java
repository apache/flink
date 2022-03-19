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

package org.apache.flink.table.planner.delegation.hive.utils;

import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** TableSchemaUtils. */
public class TableSchemaUtils {

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
}

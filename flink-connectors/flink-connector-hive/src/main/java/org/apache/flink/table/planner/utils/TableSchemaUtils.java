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

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

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
}

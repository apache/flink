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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.print.PrintStyle;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.stream.Collectors;

/** Utilities to build {@link TableResultInternal}. */
@Internal
public class TableResultUtils {

    /**
     * Build a {@link TableResultInternal} for {@code String[]} values with single column.
     *
     * @param columnName the column name of the result values.
     * @param values the string values of the result.
     */
    public static TableResultInternal buildStringArrayResult(String columnName, String[] values) {
        return buildTableResult(
                new String[] {columnName},
                new DataType[] {DataTypes.STRING()},
                Arrays.stream(values).map((c) -> new String[] {c}).toArray(String[][]::new));
    }

    public static TableResultInternal buildTableResult(
            String[] headers, DataType[] types, Object[][] rows) {
        ResolvedSchema schema = ResolvedSchema.physical(headers, types);
        ResultProvider provider =
                new StaticResultProvider(
                        Arrays.stream(rows).map(Row::of).collect(Collectors.toList()));
        return TableResultImpl.builder()
                .resultKind(ResultKind.SUCCESS_WITH_CONTENT)
                .schema(ResolvedSchema.physical(headers, types))
                .resultProvider(provider)
                .setPrintStyle(
                        PrintStyle.tableauWithDataInferredColumnWidths(
                                schema,
                                provider.getRowDataStringConverter(),
                                Integer.MAX_VALUE,
                                true,
                                false))
                .build();
    }
}

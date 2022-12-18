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

package org.apache.flink.table.gateway.rest.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.results.ResultSet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A {@code ResultInfo} contains information of a {@link ResultSet}. It is designed for transferring
 * the information of ResultSet via REST. For its serialization and deserialization, See:
 *
 * <p>{@link ResultInfoJsonSerializer} and {@link ResultInfoJsonDeserializer}
 */
@Internal
public class ResultInfo {

    public static final String FIELD_NAME_COLUMN_INFOS = "columns";

    public static final String FIELD_NAME_ROW_DATA_INFOS = "data";

    private final List<ColumnInfo> columnInfos;

    private final List<RowDataInfo> rowDataInfos;

    public ResultInfo(List<ColumnInfo> columnInfos, List<RowDataInfo> rowDataInfos) {
        this.columnInfos = columnInfos;
        this.rowDataInfos = rowDataInfos;
    }

    public List<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    public List<RowDataInfo> getRowDataInfos() {
        return rowDataInfos;
    }

    public static ResultInfo toResultInfo(ResultSet resultSet) {
        return new ResultInfo(
                resultSet.getResultSchema().getColumns().stream()
                        .map(ColumnInfo::toColumnInfo)
                        .collect(Collectors.toList()),
                resultSet.getData().stream()
                        .map(
                                rowData ->
                                        RowDataInfo.toRowDataInfo(
                                                rowData,
                                                buildFieldGetters(resultSet.getResultSchema())))
                        .collect(Collectors.toList()));
    }

    private static List<RowData.FieldGetter> buildFieldGetters(ResolvedSchema resultSchema) {
        return IntStream.range(0, resultSchema.getColumnCount())
                .mapToObj(
                        i ->
                                RowData.createFieldGetter(
                                        resultSchema.getColumnDataTypes().get(i).getLogicalType(),
                                        i))
                .collect(Collectors.toList());
    }

    public ResolvedSchema buildResultSchema() {
        return ResolvedSchema.of(
                columnInfos.stream().map(ColumnInfo::toColumn).collect(Collectors.toList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnInfos, rowDataInfos);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ResultInfo)) {
            return false;
        }
        ResultInfo that = (ResultInfo) o;
        return Objects.equals(columnInfos, that.columnInfos)
                && Objects.equals(rowDataInfos, that.rowDataInfos);
    }

    @Override
    public String toString() {
        return String.format(
                "ResultInfo{\n  columnInfos=[%s],\n  rowDataInfos=[%s]\n}",
                columnInfos.stream().map(Object::toString).collect(Collectors.joining(",")),
                rowDataInfos.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}

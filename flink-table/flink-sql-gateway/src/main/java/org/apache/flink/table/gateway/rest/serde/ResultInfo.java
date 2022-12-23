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
import org.apache.flink.table.types.logical.LogicalType;

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
    public static final String FIELD_NAME_DATA = "data";

    // field name of rows in serialization
    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_FIELDS = "fields";

    private final List<ColumnInfo> columnInfos;

    private final List<RowData> data;

    public ResultInfo(List<ColumnInfo> columnInfos, List<RowData> data) {
        this.columnInfos = columnInfos;
        this.data = data;
    }

    public List<ColumnInfo> getColumnInfos() {
        return columnInfos;
    }

    public List<RowData> getData() {
        return data;
    }

    public static ResultInfo toResultInfo(ResultSet resultSet) {
        return new ResultInfo(
                resultSet.getResultSchema().getColumns().stream()
                        .map(ColumnInfo::toColumnInfo)
                        .collect(Collectors.toList()),
                resultSet.getData());
    }

    public List<RowData.FieldGetter> getFieldGetters() {
        return buildFieldGetters(
                columnInfos.stream().map(ColumnInfo::getLogicalType).collect(Collectors.toList()));
    }

    public static List<RowData.FieldGetter> buildFieldGetters(
            List<LogicalType> columnLogicalTypes) {
        return IntStream.range(0, columnLogicalTypes.size())
                .mapToObj(i -> RowData.createFieldGetter(columnLogicalTypes.get(i), i))
                .collect(Collectors.toList());
    }

    public ResolvedSchema buildResultSchema() {
        return ResolvedSchema.of(
                columnInfos.stream().map(ColumnInfo::toColumn).collect(Collectors.toList()));
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnInfos, data);
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
        return Objects.equals(columnInfos, that.columnInfos) && Objects.equals(data, that.data);
    }

    @Override
    public String toString() {
        return String.format(
                "ResultInfo{\n  columnInfos=[%s],\n  rows=[%s]\n}",
                columnInfos.stream().map(Object::toString).collect(Collectors.joining(",")),
                data.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}

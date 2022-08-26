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
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.formats.json.RowDataToJsonConverters;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_DATA;

/** Json serializer for {@link ResultSet}. */
@Internal
public class JsonResultSetSerializer extends StdSerializer<ResultSet> {

    private static final long serialVersionUID = 1L;

    public JsonResultSetSerializer() {
        super(ResultSet.class);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final RowDataToJsonConverters TO_JSON_CONVERTERS =
            new RowDataToJsonConverters(
                    TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null");

    @Override
    public void serialize(
            ResultSet resultSet, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        // 1.Serialize ColumnInfo
        List<Column> columns = resultSet.getResultSchema().getColumns();
        List<ColumnInfo> columnInfos = new ArrayList<>();
        for (Column column : columns) {
            columnInfos.add(
                    new ColumnInfo(
                            column.getName(),
                            column.getDataType().getLogicalType(),
                            column.getComment().orElse(null)));
        }
        serializerProvider.defaultSerializeField(
                FIELD_NAME_COLUMN_INFOS, columnInfos, jsonGenerator);

        // 2.Serialize RowData

        // The fieldGetters for all RowData
        List<RowData.FieldGetter> fieldGetters = new ArrayList<>();
        for (int i = 0; i < resultSet.getResultSchema().getColumnCount(); i++) {
            fieldGetters.add(
                    RowData.createFieldGetter(
                            resultSet
                                    .getResultSchema()
                                    .getColumnDataTypes()
                                    .get(i)
                                    .getLogicalType(),
                            i));
        }

        // The fieldType for all RowData
        List<LogicalType> fieldTypes =
                resultSet.getResultSchema().getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());

        // Generate converters for all fieldTypes
        List<RowDataToJsonConverters.RowDataToJsonConverter> converters =
                fieldTypes.stream()
                        .map(TO_JSON_CONVERTERS::createConverter)
                        .collect(Collectors.toList());
        List<RowDataInfo> data = new ArrayList<>();
        for (RowData row : resultSet.getData()) {
            RowKind rowKind = row.getRowKind();
            List<JsonNode> fields = new ArrayList<>();
            for (int i = 0; i < row.getArity(); ++i) {
                Object field = fieldGetters.get(i).getFieldOrNull(row);
                RowDataToJsonConverters.RowDataToJsonConverter converter = converters.get(i);
                fields.add(buildJsonValueConverter(converter).apply(field));
            }
            data.add(new RowDataInfo(rowKind.name(), fields));
        }

        serializerProvider.defaultSerializeField(FIELD_NAME_DATA, data, jsonGenerator);
        jsonGenerator.writeEndObject();
    }

    private static Function<Object, JsonNode> buildJsonValueConverter(
            RowDataToJsonConverters.RowDataToJsonConverter converter) {
        return field -> converter.convert(OBJECT_MAPPER, null, field);
    }
}

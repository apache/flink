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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.rest.util.RowFormat;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Serializer for {@link ResultInfo}.
 *
 * @see ResultInfoDeserializer for the reverse operation.
 */
@Internal
public class ResultInfoSerializer extends StdSerializer<ResultInfo> {

    // Columns
    public static final String FIELD_NAME_COLUMN_INFOS = "columns";

    // RowData
    public static final String FIELD_NAME_DATA = "data";
    public static final String FIELD_NAME_KIND = "kind";
    public static final String FIELD_NAME_FIELDS = "fields";

    // RowFormat
    public static final String FIELD_NAME_ROW_FORMAT = "rowFormat";

    private static final long serialVersionUID = 1L;

    public ResultInfoSerializer() {
        super(ResultInfo.class);
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final RowDataToJsonConverters TO_JSON_CONVERTERS =
            new RowDataToJsonConverters(
                    TimestampFormat.ISO_8601, JsonFormatOptions.MapNullKeyMode.LITERAL, "null");

    @Override
    public void serialize(
            ResultInfo resultInfo,
            JsonGenerator jsonGenerator,
            SerializerProvider serializerProvider)
            throws IOException {
        jsonGenerator.writeStartObject();

        // serialize ColumnInfos
        serializerProvider.defaultSerializeField(
                FIELD_NAME_COLUMN_INFOS, resultInfo.getColumnInfos(), jsonGenerator);

        // serialize RowFormat
        serializerProvider.defaultSerializeField(
                FIELD_NAME_ROW_FORMAT, resultInfo.getRowFormat(), jsonGenerator);

        // serialize data
        serializeData(resultInfo.getData(), buildRowDataConverters(resultInfo), jsonGenerator);

        jsonGenerator.writeEndObject();
    }

    private void serializeData(
            List<RowData> data,
            List<Function<RowData, JsonNode>> converters,
            JsonGenerator jsonGenerator)
            throws IOException {
        // format:
        // data: [{"kind": "", "fields": []}, ...]
        ArrayNode serializedData = OBJECT_MAPPER.createArrayNode();
        serializedData.addAll(
                data.stream()
                        .map(rowData -> convertRowData(rowData, converters))
                        .collect(Collectors.toList()));
        jsonGenerator.writeFieldName(FIELD_NAME_DATA);
        jsonGenerator.writeTree(serializedData);
    }

    private JsonNode convertRowData(RowData rowData, List<Function<RowData, JsonNode>> converters) {
        ObjectNode serializedRowData = OBJECT_MAPPER.createObjectNode();
        // kind
        serializedRowData.put(FIELD_NAME_KIND, rowData.getRowKind().name());
        // fields
        ArrayNode fields = serializedRowData.putArray(FIELD_NAME_FIELDS);
        fields.addAll(
                converters.stream()
                        .map(converter -> converter.apply(rowData))
                        .collect(Collectors.toList()));

        return serializedRowData;
    }

    private List<Function<RowData, JsonNode>> buildRowDataConverters(ResultInfo resultInfo) {
        RowFormat rowFormat = resultInfo.getRowFormat();
        List<RowData.FieldGetter> fieldGetters = resultInfo.getFieldGetters();
        if (rowFormat == RowFormat.JSON) {
            List<RowDataToJsonConverters.RowDataToJsonConverter> converters =
                    resultInfo.getColumnInfos().stream()
                            .map(ColumnInfo::getLogicalType)
                            .map(TO_JSON_CONVERTERS::createConverter)
                            .collect(Collectors.toList());

            return IntStream.range(0, converters.size())
                    .mapToObj(
                            i ->
                                    (Function<RowData, JsonNode>)
                                            rowData ->
                                                    converters
                                                            .get(i)
                                                            .convert(
                                                                    OBJECT_MAPPER,
                                                                    null,
                                                                    fieldGetters
                                                                            .get(i)
                                                                            .getFieldOrNull(
                                                                                    rowData)))
                    .collect(Collectors.toList());
        } else if (rowFormat == RowFormat.PLAIN_TEXT) {
            return IntStream.range(0, resultInfo.getColumnInfos().size())
                    .mapToObj(
                            i ->
                                    (Function<RowData, JsonNode>)
                                            rowData -> {
                                                Object value =
                                                        fieldGetters.get(i).getFieldOrNull(rowData);
                                                return value == null
                                                        ? OBJECT_MAPPER.getNodeFactory().nullNode()
                                                        : OBJECT_MAPPER
                                                                .getNodeFactory()
                                                                .textNode(value.toString());
                                            })
                    .collect(Collectors.toList());
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown row format: %s.", rowFormat));
        }
    }
}

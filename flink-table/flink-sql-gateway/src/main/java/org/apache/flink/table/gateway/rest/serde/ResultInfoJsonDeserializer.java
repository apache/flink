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
import org.apache.flink.formats.json.JsonToRowDataConverters;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.gateway.rest.serde.ResultInfo.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.rest.serde.ResultInfo.FIELD_NAME_DATA;
import static org.apache.flink.table.gateway.rest.serde.ResultInfo.FIELD_NAME_FIELDS;
import static org.apache.flink.table.gateway.rest.serde.ResultInfo.FIELD_NAME_KIND;

/**
 * Json deserializer for {@link ResultInfo}.
 *
 * @see ResultInfoJsonSerializer for the reverse operation.
 */
@Internal
public class ResultInfoJsonDeserializer extends StdDeserializer<ResultInfo> {

    private static final long serialVersionUID = 1L;

    public ResultInfoJsonDeserializer() {
        super(ResultInfo.class);
    }

    private static final JsonToRowDataConverters TO_ROW_DATA_CONVERTERS =
            new JsonToRowDataConverters(false, false, TimestampFormat.ISO_8601);

    @Override
    public ResultInfo deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // deserialize ColumnInfos
        List<ColumnInfo> columnInfos =
                Arrays.asList(
                        jsonParser
                                .getCodec()
                                .treeToValue(
                                        node.get(FIELD_NAME_COLUMN_INFOS), ColumnInfo[].class));

        // generate converters for all fields of each row
        List<JsonToRowDataConverters.JsonToRowDataConverter> converters =
                columnInfos.stream()
                        .map(ColumnInfo::getLogicalType)
                        .map(TO_ROW_DATA_CONVERTERS::createConverter)
                        .collect(Collectors.toList());

        // deserialize rows
        List<RowData> data = deserializeData((ArrayNode) node.get(FIELD_NAME_DATA), converters);

        return new ResultInfo(columnInfos, data);
    }

    private List<RowData> deserializeData(
            ArrayNode serializedRows,
            List<JsonToRowDataConverters.JsonToRowDataConverter> converters) {
        List<RowData> data = new ArrayList<>();
        serializedRows.forEach(rowDataNode -> data.add(convertToRowData(rowDataNode, converters)));
        return data;
    }

    private GenericRowData convertToRowData(
            JsonNode serializedRow,
            List<JsonToRowDataConverters.JsonToRowDataConverter> converters) {
        ArrayNode fieldsArrayNode = (ArrayNode) serializedRow.get(FIELD_NAME_FIELDS);
        List<JsonNode> fieldNodes = CollectionUtil.iteratorToList(fieldsArrayNode.iterator());
        return GenericRowData.ofKind(
                RowKind.valueOf(serializedRow.get(FIELD_NAME_KIND).asText()),
                IntStream.range(0, fieldNodes.size())
                        .mapToObj(i -> converters.get(i).convert(fieldNodes.get(i)))
                        .toArray());
    }
}

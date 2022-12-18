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
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.DataType;
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
import static org.apache.flink.table.gateway.rest.serde.ResultInfo.FIELD_NAME_ROW_DATA_INFOS;
import static org.apache.flink.table.gateway.rest.serde.RowDataInfo.FIELD_NAME_FIELDS;
import static org.apache.flink.table.gateway.rest.serde.RowDataInfo.FIELD_NAME_KIND;

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

        // deserialize RowDataInfos

        // build schema from column info
        ResolvedSchema resultSchema =
                ResolvedSchema.of(
                        columnInfos.stream()
                                .map(ColumnInfo::toColumn)
                                .collect(Collectors.toList()));
        // generate converters for all fields of each row
        List<JsonToRowDataConverters.JsonToRowDataConverter> converters =
                resultSchema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .map(TO_ROW_DATA_CONVERTERS::createConverter)
                        .collect(Collectors.toList());

        // build RowDataInfos
        List<RowDataInfo> rowDataInfos = new ArrayList<>();
        ArrayNode rowDataInfoArrayNode = (ArrayNode) node.get(FIELD_NAME_ROW_DATA_INFOS);

        rowDataInfoArrayNode.forEach(
                dataInfoNode -> {
                    // kind
                    String kind = dataInfoNode.get(FIELD_NAME_KIND).asText();

                    // fields
                    ArrayNode fieldsArrayNode = (ArrayNode) dataInfoNode.get(FIELD_NAME_FIELDS);

                    List<JsonNode> elementNodes =
                            CollectionUtil.iteratorToList(fieldsArrayNode.iterator());
                    List<Object> fields =
                            IntStream.range(0, elementNodes.size())
                                    .mapToObj(i -> converters.get(i).convert(elementNodes.get(i)))
                                    .collect(Collectors.toList());

                    rowDataInfos.add(new RowDataInfo(kind, fields));
                });

        return new ResultInfo(columnInfos, rowDataInfos);
    }
}

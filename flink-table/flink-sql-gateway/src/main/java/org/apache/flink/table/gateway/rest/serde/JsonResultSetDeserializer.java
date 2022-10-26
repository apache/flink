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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_COLUMN_INFOS;
import static org.apache.flink.table.gateway.api.results.ResultSet.FIELD_NAME_DATA;

/** Json deserializer for {@link ResultSet}. */
@Internal
public class JsonResultSetDeserializer extends StdDeserializer<ResultSet> {

    private static final long serialVersionUID = 1L;

    public JsonResultSetDeserializer() {
        super(ResultSet.class);
    }

    private static final JsonToRowDataConverters TO_ROWDATA_CONVERTERS =
            new JsonToRowDataConverters(false, false, TimestampFormat.ISO_8601);

    @Override
    public ResultSet deserialize(JsonParser jsonParser, DeserializationContext ctx)
            throws IOException {
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);
        ResolvedSchema resolvedSchema;
        List<RowData> data = new ArrayList<>();

        // Deserialize column infos
        ColumnInfo[] columnInfos =
                jsonParser
                        .getCodec()
                        .treeToValue(node.get(FIELD_NAME_COLUMN_INFOS), ColumnInfo[].class);
        List<Column> columns = new ArrayList<>();
        for (ColumnInfo columnInfo : columnInfos) {
            LogicalType logicalType = columnInfo.getLogicalType();
            columns.add(
                    Column.physical(
                            columnInfo.getName(), DataTypeUtils.toInternalDataType(logicalType)));
        }

        // Parse the schema from Column
        resolvedSchema = ResolvedSchema.of(columns);
        // The fieldType for all RowData
        List<LogicalType> fieldTypes =
                resolvedSchema.getColumnDataTypes().stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());

        // Generate converters for all fieldTypes
        List<JsonToRowDataConverters.JsonToRowDataConverter> converters =
                fieldTypes.stream()
                        .map(TO_ROWDATA_CONVERTERS::createConverter)
                        .collect(Collectors.toList());

        // Get the RowDataInfo
        JsonParser dataParser = node.get(FIELD_NAME_DATA).traverse();
        dataParser.nextToken();
        RowDataInfo[] rowDataInfos = ctx.readValue(dataParser, RowDataInfo[].class);

        // Parse the RowData from RowDataInfo
        for (RowDataInfo rowDataInfo : rowDataInfos) {
            RowKind rowKind = RowKind.valueOf(rowDataInfo.getKind());
            GenericRowData rowData = new GenericRowData(rowKind, rowDataInfo.getFields().size());
            List<JsonNode> fields = rowDataInfo.getFields();
            // Setting fields of one RowData
            for (int i = 0; i < rowData.getArity(); ++i) {
                JsonNode jsonNode = fields.get(i);
                JsonToRowDataConverters.JsonToRowDataConverter converter = converters.get(i);
                Object object = buildObjectValueConverter(converter).apply(jsonNode);
                if (object != null && object.toString().equals("null")) {
                    rowData.setField(i, new BinaryStringData(""));
                } else {
                    rowData.setField(i, object);
                }
            }
            data.add(rowData);
        }

        // The placeholder is used to build a ResultSet
        ResultSet.ResultType resultTypePlaceHolder = ResultSet.ResultType.PAYLOAD;
        Long nextTokenPlaceHolder = 0L;
        return new ResultSet(resultTypePlaceHolder, nextTokenPlaceHolder, resolvedSchema, data);
    }

    private static Function<JsonNode, Object> buildObjectValueConverter(
            JsonToRowDataConverters.JsonToRowDataConverter converter) {
        return converter::convert;
    }
}

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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connector.testutils.formats.SchemaTestUtils.open;
import static org.apache.flink.formats.json.JsonRowDataSerDeSchemaTest.convertToExternal;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonParserRowDataDeserializationSchema}. */
public class JsonParserRowDataDeSerSchemaTest {

    /**
     * Tests parsing partial fields in actual json data, sometimes we can skip some complex field
     * parsing, e.g., field with Row/Array type.
     */
    @Test
    public void testParsePartialJson() throws Exception {
        byte tinyint = 'c';
        short smallint = 128;
        int intValue = 45536;
        float floatValue = 33.333F;
        long bigint = 1238123899121L;
        String name = "asdlkjasjkdla998y1122";
        byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        BigDecimal decimal = new BigDecimal("123.456789");

        Map<String, Long> map = new HashMap<>();
        map.put("flink", 123L);

        Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        ObjectMapper objectMapper = new ObjectMapper();
        ArrayNode doubleNode = objectMapper.createArrayNode().add(1.1D).add(2.2D).add(3.3D);

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("bool", true);
        root.put("tinyint", tinyint);
        root.put("smallint", smallint);
        root.put("int", intValue);
        root.put("bigint", bigint);
        root.put("float", floatValue);
        root.put("name", name);
        root.put("bytes", bytes);
        root.put("decimal", decimal);
        root.set("doubles", doubleNode);
        root.put("date", "1990-10-14");
        root.put("time", "12:12:43");
        root.put("timestamp3", "1990-10-14T12:12:43.123");
        root.put("timestamp9", "1990-10-14T12:12:43.123456789");
        root.putObject("map").put("flink", 123);
        root.putObject("map2map").putObject("inner_map").put("key", 234);

        byte[] serializedJson = objectMapper.writeValueAsBytes(root);

        DataType dataType =
                ROW(
                        FIELD("bool", BOOLEAN()),
                        FIELD("tinyint", TINYINT()),
                        FIELD("smallint", SMALLINT()),
                        FIELD("int", INT()),
                        FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);

        DeserializationSchema<RowData> deserializationSchema =
                new JsonParserRowDataDeserializationSchema(
                        schema, resultTypeInfo, false, false, TimestampFormat.ISO_8601);
        open(deserializationSchema);

        Row expected = new Row(5);
        expected.setField(0, true);
        expected.setField(1, tinyint);
        expected.setField(2, smallint);
        expected.setField(3, intValue);
        expected.setField(4, nestedMap);

        RowData rowData = deserializationSchema.deserialize(serializedJson);
        Row actual = convertToExternal(rowData, dataType);
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testProjected() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f0", 0);
        root.put("f1", 1);
        root.put("f2", 2);
        root.put("f3", 3);
        root.put("f4", 4);
        innerTestProjected(objectMapper.writeValueAsBytes(root), GenericRowData.of(1, 4, 0, 2));
    }

    @Test
    public void testProjectedNullable() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f0", 0);
        root.put("f1", 1);
        root.putNull("f2");
        root.put("f3", 3);
        root.putNull("f4");
        innerTestProjected(
                objectMapper.writeValueAsBytes(root), GenericRowData.of(1, null, 0, null));
    }

    private void innerTestProjected(byte[] serializedJson, GenericRowData expected)
            throws Exception {
        DataType dataType =
                ROW(FIELD("f1", INT()), FIELD("f4", INT()), FIELD("f0", INT()), FIELD("f2", INT()));
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);

        JsonParserRowDataDeserializationSchema deserializationSchema =
                new JsonParserRowDataDeserializationSchema(
                        schema,
                        resultTypeInfo,
                        false,
                        false,
                        TimestampFormat.ISO_8601,
                        new String[][] {
                            new String[] {"f1"},
                            new String[] {"f4"},
                            new String[] {"f0"},
                            new String[] {"f2"}
                        });
        open(deserializationSchema);

        RowData rowData = deserializationSchema.deserialize(serializedJson);
        assertThat(rowData).isEqualTo(expected);
    }

    private byte[] prepareNestedRow() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f0", 0);
        root.put("f1", 1);
        root.put("f2", 2);
        ObjectNode f3 = root.putObject("f3");
        f3.put("f0", 30);
        f3.put("f1", 31);
        f3.put("f2", 32);
        ObjectNode f4 = root.putObject("f4");
        f4.put("f0", 40);
        f4.put("f1", 41);

        ObjectNode f5 = root.putObject("f5");
        f5.put("f0", 50);
        ObjectNode f51 = f5.putObject("f1");
        f51.put("f0", 510);
        f51.put("f1", 511);

        return objectMapper.writeValueAsBytes(root);
    }

    private byte[] prepareNullableNestedRow() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        // Root
        ObjectNode root = objectMapper.createObjectNode();
        root.put("f0", 0);
        root.put("f1", 1);
        root.put("f2", 2);
        ObjectNode f3 = root.putObject("f3");
        f3.put("f0", 30);
        f3.put("f1", 31);
        f3.put("f2", 32);
        ObjectNode f4 = root.putObject("f4");
        f4.putNull("f0");
        f4.put("f1", 41);

        ObjectNode f5 = root.putObject("f5");
        f5.put("f0", 50);
        f5.putNull("f1");

        return objectMapper.writeValueAsBytes(root);
    }

    @Test
    public void testProjectNestedField() throws Exception {
        GenericRowData expected = GenericRowData.of(1, 31, 32, 510, 0, GenericRowData.of(40, 41));
        innerTestProjectNestedField(prepareNestedRow(), expected);
    }

    @Test
    public void testProjectNestedFieldNullable() throws Exception {
        GenericRowData expected =
                GenericRowData.of(1, 31, 32, null, 0, GenericRowData.of(null, 41));
        innerTestProjectNestedField(prepareNullableNestedRow(), expected);
    }

    private void innerTestProjectNestedField(byte[] serializedJson, GenericRowData expected)
            throws Exception {
        DataType dataType =
                ROW(
                        FIELD("f1", INT()),
                        FIELD("f3_1", INT()),
                        FIELD("f3_2", INT()),
                        FIELD("f5_1_0", INT()),
                        FIELD("f0", INT()),
                        FIELD("f4", ROW(FIELD("f0", INT()), FIELD("f1", INT()))));
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);

        JsonParserRowDataDeserializationSchema deserializationSchema =
                new JsonParserRowDataDeserializationSchema(
                        schema,
                        resultTypeInfo,
                        false,
                        false,
                        TimestampFormat.ISO_8601,
                        new String[][] {
                            new String[] {"f1"},
                            new String[] {"f3", "f1"},
                            new String[] {"f3", "f2"},
                            new String[] {"f5", "f1", "f0"},
                            new String[] {"f0"},
                            new String[] {"f4"}
                        });
        open(deserializationSchema);

        RowData rowData = deserializationSchema.deserialize(serializedJson);
        assertThat(rowData).isEqualTo(expected);
    }

    @Test
    public void testProjectBothRowAndNestedField() throws Exception {
        GenericRowData expected =
                GenericRowData.of(
                        1,
                        32,
                        GenericRowData.of(40, 41),
                        40,
                        511,
                        GenericRowData.of(510, 511),
                        GenericRowData.of(50, GenericRowData.of(510, 511)));
        innerTestProjectBothRowAndNestedField(prepareNestedRow(), expected);
    }

    @Test
    public void testProjectBothRowAndNestedFieldNullable() throws Exception {
        GenericRowData expected =
                GenericRowData.of(
                        1,
                        32,
                        GenericRowData.of(null, 41),
                        null,
                        null,
                        null,
                        GenericRowData.of(50, null));
        innerTestProjectBothRowAndNestedField(prepareNullableNestedRow(), expected);
    }

    private void innerTestProjectBothRowAndNestedField(
            byte[] serializedJson, GenericRowData expected) throws Exception {
        DataType dataType =
                ROW(
                        FIELD("f1", INT()),
                        FIELD("f3_2", INT()),
                        FIELD("f4", ROW(FIELD("f0", INT()), FIELD("f1", INT()))),
                        FIELD("f4_0", INT()),
                        FIELD("f5_1_1", INT()),
                        FIELD("f5_1", ROW(FIELD("f0", INT()), FIELD("f1", INT()))),
                        FIELD(
                                "f5",
                                ROW(
                                        FIELD("f0", INT()),
                                        FIELD("f1", ROW(FIELD("f0", INT()), FIELD("f1", INT()))))));
        RowType schema = (RowType) dataType.getLogicalType();
        TypeInformation<RowData> resultTypeInfo = InternalTypeInfo.of(schema);

        JsonParserRowDataDeserializationSchema deserializationSchema =
                new JsonParserRowDataDeserializationSchema(
                        schema,
                        resultTypeInfo,
                        false,
                        false,
                        TimestampFormat.ISO_8601,
                        new String[][] {
                            new String[] {"f1"},
                            new String[] {"f3", "f2"},
                            new String[] {"f4"},
                            new String[] {"f4", "f0"},
                            new String[] {"f5", "f1", "f1"},
                            new String[] {"f5", "f1"},
                            new String[] {"f5"}
                        });
        open(deserializationSchema);

        RowData rowData = deserializationSchema.deserialize(serializedJson);
        assertThat(rowData).isEqualTo(expected);
    }
}

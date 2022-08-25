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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.gateway.api.results.ResultSet;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link JsonResultSetSerializer} and {@link JsonResultSetDeserializer}. */
class JsonResultSetSerDeTest {

    private static final byte tinyint = 'c';
    private static final short smallint = 128;
    private static final int intValue = 45536;
    private static final float floatValue = 33.333F;
    private static final long bigint = 1238123899121L;
    private static final String name = "asdlkjasjkdla998y1122";
    private static final byte[] bytes = new byte[1024];
    private static final Double[] doubles = new Double[] {1.1, 2.2, 3.3};
    private static final BigDecimal decimal = new BigDecimal("123.456789");
    private static final LocalDate date = LocalDate.parse("1990-10-14");
    private static final LocalTime time = LocalTime.parse("12:12:43");
    private static final Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
    private static final Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");
    private static final Instant timestampWithLocalZone =
            LocalDateTime.of(1990, 10, 14, 12, 12, 43, 123456789)
                    .atOffset(ZoneOffset.of("Z"))
                    .toInstant();
    private static final int ROW_NUMBER = 10;

    private static final Map<String, Long> map = new HashMap<>();
    private static final Map<String, Integer> multiSet = new HashMap<>();
    private static final Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
    private static final Map<String, Integer> innerMap = new HashMap<>();

    static {
        map.put("element", 123L);
        multiSet.put("element", 2);
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);
        ThreadLocalRandom.current().nextBytes(bytes);
    }

    @Test
    void testSerDeResultSetWithSingleRowData() throws Exception {
        Row row = getTestRowData();
        seDeResultSet(Collections.singletonList(row), getFields());
    }

    @Test
    void testSerDeResultSetWithMultiRowData() throws Exception {
        ArrayList<Row> rowList = new ArrayList<>();
        for (int i = 0; i < ROW_NUMBER; ++i) {
            rowList.add(getTestRowData());
        }
        seDeResultSet(rowList, getFields());
    }

    @Test
    void testSerDeResultSetWithRowDataNullValues() throws Exception {
        List<Row> rowList = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < 18; ++i) {
            positions.add(new Random().nextInt(18));
        }
        for (int i = 0; i < ROW_NUMBER; ++i) {
            rowList.add(getTestRowDataWithNullValues(positions));
        }
        seDeResultSet(rowList, getFields());
    }

    void seDeResultSet(List<Row> rowList, List<DataTypes.Field> fields) throws IOException {
        List<RowData> rowDataList =
                rowList.stream()
                        .map(JsonResultSetSerDeTest::convertToInternal)
                        .collect(Collectors.toList());
        ResolvedSchema testResolvedSchema = getTestResolvedSchema(fields);
        ResultSet testResultSet =
                new ResultSet(ResultSet.ResultType.PAYLOAD, 0L, testResolvedSchema, rowDataList);
        // Test serialization & deserialization
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule resultSetModule = new SimpleModule();
        resultSetModule.addSerializer(ResultSet.class, new JsonResultSetSerializer());
        resultSetModule.addDeserializer(ResultSet.class, new JsonResultSetDeserializer());
        objectMapper.registerModule(resultSetModule);
        String result = objectMapper.writeValueAsString(testResultSet);
        ResultSet resultSet = objectMapper.readValue(result, ResultSet.class);
        List<RowData> deRowDataList = resultSet.getData();
        for (int i = 0; i < deRowDataList.size(); ++i) {
            assertThat(convertToExternal(deRowDataList.get(i), ROW(getFields())))
                    .isEqualTo(rowList.get(i));
        }
        assertThat(resultSet.getResultSchema().toString())
                .isEqualTo(testResultSet.getResultSchema().toString());
    }

    private static ResolvedSchema getTestResolvedSchema(List<DataTypes.Field> fields) {
        List<String> columnNames =
                fields.stream().map(DataTypes.AbstractField::getName).collect(Collectors.toList());
        List<DataType> columnDataTypes =
                fields.stream().map(DataTypes.Field::getDataType).collect(Collectors.toList());
        return ResolvedSchema.physical(columnNames, columnDataTypes);
    }

    private static List<DataTypes.Field> getFields() {
        return Arrays.asList(
                FIELD("bool", BOOLEAN()),
                FIELD("tinyint", TINYINT()),
                FIELD("smallint", SMALLINT()),
                FIELD("int", INT()),
                FIELD("bigint", BIGINT()),
                FIELD("float", FLOAT()),
                FIELD("name", STRING()),
                FIELD("bytes", BYTES()),
                FIELD("decimal", DECIMAL(9, 6)),
                FIELD("doubles", ARRAY(DOUBLE())),
                FIELD("date", DATE()),
                FIELD("time", TIME(0)),
                FIELD("timestamp3", TIMESTAMP(3)),
                FIELD("timestamp9", TIMESTAMP(9)),
                FIELD("timestampWithLocalZone", TIMESTAMP_WITH_LOCAL_TIME_ZONE(9)),
                FIELD("map", MAP(STRING(), BIGINT())),
                FIELD("multiSet", MULTISET(STRING())),
                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))));
    }

    private static Row getTestRowData() {
        Row testRow = new Row(18);
        setRandomKind(testRow);
        testRow.setField(0, true);
        testRow.setField(1, tinyint);
        testRow.setField(2, smallint);
        testRow.setField(3, intValue);
        testRow.setField(4, bigint);
        testRow.setField(5, floatValue);
        testRow.setField(6, name);
        testRow.setField(7, bytes);
        testRow.setField(8, decimal);
        testRow.setField(9, doubles);
        testRow.setField(10, date);
        testRow.setField(11, time);
        testRow.setField(12, timestamp3.toLocalDateTime());
        testRow.setField(13, timestamp9.toLocalDateTime());
        testRow.setField(14, timestampWithLocalZone);
        testRow.setField(15, map);
        testRow.setField(16, multiSet);
        testRow.setField(17, nestedMap);
        return testRow;
    }

    private static Row getTestRowDataWithNullValues(List<Integer> positions) {
        Row testRow = new Row(18);
        setRandomKind(testRow);
        testRow.setField(0, true);
        testRow.setField(1, tinyint);
        testRow.setField(2, smallint);
        testRow.setField(3, intValue);
        testRow.setField(4, bigint);
        testRow.setField(5, floatValue);
        testRow.setField(6, name);
        testRow.setField(7, bytes);
        testRow.setField(8, decimal);
        testRow.setField(9, doubles);
        testRow.setField(10, date);
        testRow.setField(11, time);
        testRow.setField(12, timestamp3.toLocalDateTime());
        testRow.setField(13, timestamp9.toLocalDateTime());
        testRow.setField(14, timestampWithLocalZone);
        testRow.setField(15, map);
        testRow.setField(16, multiSet);
        testRow.setField(17, nestedMap);
        for (int position : positions) {
            testRow.setField(position, null);
        }
        return testRow;
    }

    private static void setRandomKind(Row testRow) {
        int i = new Random().nextInt() % 4;
        switch (i) {
            case 0:
                testRow.setKind(RowKind.INSERT);
                break;
            case 1:
                testRow.setKind(RowKind.DELETE);
                break;
            case 2:
                testRow.setKind(RowKind.UPDATE_AFTER);
                break;
            case 3:
                testRow.setKind(RowKind.UPDATE_BEFORE);
                break;
        }
    }

    @SuppressWarnings("unchecked")
    private static Row convertToExternal(RowData rowData, DataType dataType) {
        return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
    }

    @SuppressWarnings("unchecked")
    private static GenericRowData convertToInternal(Row row) {
        DataFormatConverters.DataFormatConverter<GenericRowData, Row> converter =
                DataFormatConverters.getConverterForDataType(ROW(getFields()));
        return converter.toInternal(row);
    }
}

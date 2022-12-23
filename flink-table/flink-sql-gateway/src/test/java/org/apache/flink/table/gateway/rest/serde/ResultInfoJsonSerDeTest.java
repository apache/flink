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

import org.junit.jupiter.api.BeforeAll;
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

/** Tests for {@link ResultInfoJsonSerializer} and {@link ResultInfoJsonDeserializer}. */
public class ResultInfoJsonSerDeTest {

    private final byte tinyint = 'c';
    private final short smallint = 128;
    private final int intValue = 45536;
    private final float floatValue = 33.333F;
    private final long bigint = 1238123899121L;
    private final String name = "asdlkjasjkdla998y1122";
    private static final byte[] BYTES = new byte[1024];
    private final Double[] doubles = new Double[] {1.1, 2.2, 3.3};
    private final BigDecimal decimal = new BigDecimal("123.456789");
    private final LocalDate date = LocalDate.parse("1990-10-14");
    private final LocalTime time = LocalTime.parse("12:12:43");
    private final Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
    private final Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");
    private final Instant timestampWithLocalZone =
            LocalDateTime.of(1990, 10, 14, 12, 12, 43, 123456789)
                    .atOffset(ZoneOffset.of("Z"))
                    .toInstant();

    private static final Map<String, Long> MAP = new HashMap<>();
    private static final Map<String, Integer> MULTI_SET = new HashMap<>();
    private static final Map<String, Map<String, Integer>> NESTED_MAP = new HashMap<>();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final int rowNumber = 10;

    @BeforeAll
    public static void setUp() {
        MAP.put("element", 123L);
        MULTI_SET.put("element", 2);
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        NESTED_MAP.put("inner_map", innerMap);
        ThreadLocalRandom.current().nextBytes(BYTES);

        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(ResultInfo.class, new ResultInfoJsonSerializer());
        simpleModule.addDeserializer(ResultInfo.class, new ResultInfoJsonDeserializer());
        OBJECT_MAPPER.registerModule(simpleModule);
    }

    @Test
    void testResultInfoSerDeWithSingleRowData() throws Exception {
        Row row = getTestRowData();
        serDeTest(Collections.singletonList(row), getFields());
    }

    @Test
    void testResultInfoSerDeWithMultiRowData() throws Exception {
        List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < rowNumber; i++) {
            rowList.add(getTestRowData());
        }
        serDeTest(rowList, getFields());
    }

    @Test
    void testResultInfoSerDeWithNullValues() throws Exception {
        List<Row> rowList = new ArrayList<>();
        List<Integer> positions = new ArrayList<>();
        for (int i = 0; i < 18; i++) {
            positions.add(new Random().nextInt(18));
        }
        for (int i = 0; i < rowNumber; i++) {
            rowList.add(getTestRowDataWithNullValues(positions));
        }
        serDeTest(rowList, getFields());
    }

    private void serDeTest(List<Row> rowList, List<DataTypes.Field> fields) throws IOException {
        List<RowData> rowDataList =
                rowList.stream().map(this::convertToInternal).collect(Collectors.toList());
        ResolvedSchema testResolvedSchema = getTestResolvedSchema(fields);
        ResultInfo testResultInfo =
                ResultInfo.toResultInfo(
                        new ResultSet(
                                ResultSet.ResultType.PAYLOAD, 0L, testResolvedSchema, rowDataList));

        // test serialization & deserialization
        String result = OBJECT_MAPPER.writeValueAsString(testResultInfo);
        ResultInfo resultInfo = OBJECT_MAPPER.readValue(result, ResultInfo.class);

        assertThat(resultInfo.buildResultSchema().toString())
                .isEqualTo(testResultInfo.buildResultSchema().toString());

        List<RowData> data = resultInfo.getData();
        for (int i = 0; i < data.size(); i++) {
            assertThat(convertToExternal(data.get(i), ROW(getFields()))).isEqualTo(rowList.get(i));
        }
    }

    private ResolvedSchema getTestResolvedSchema(List<DataTypes.Field> fields) {
        List<String> columnNames =
                fields.stream().map(DataTypes.AbstractField::getName).collect(Collectors.toList());
        List<DataType> columnDataTypes =
                fields.stream().map(DataTypes.Field::getDataType).collect(Collectors.toList());
        return ResolvedSchema.physical(columnNames, columnDataTypes);
    }

    private List<DataTypes.Field> getFields() {
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

    private Row getTestRowData() {
        Row testRow = new Row(18);
        setRandomKind(testRow);
        testRow.setField(0, true);
        testRow.setField(1, tinyint);
        testRow.setField(2, smallint);
        testRow.setField(3, intValue);
        testRow.setField(4, bigint);
        testRow.setField(5, floatValue);
        testRow.setField(6, name);
        testRow.setField(7, BYTES);
        testRow.setField(8, decimal);
        testRow.setField(9, doubles);
        testRow.setField(10, date);
        testRow.setField(11, time);
        testRow.setField(12, timestamp3.toLocalDateTime());
        testRow.setField(13, timestamp9.toLocalDateTime());
        testRow.setField(14, timestampWithLocalZone);
        testRow.setField(15, MAP);
        testRow.setField(16, MULTI_SET);
        testRow.setField(17, NESTED_MAP);
        return testRow;
    }

    private Row getTestRowDataWithNullValues(List<Integer> positions) {
        Row testRow = new Row(18);
        setRandomKind(testRow);
        testRow.setField(0, true);
        testRow.setField(1, tinyint);
        testRow.setField(2, smallint);
        testRow.setField(3, intValue);
        testRow.setField(4, bigint);
        testRow.setField(5, floatValue);
        testRow.setField(6, name);
        testRow.setField(7, BYTES);
        testRow.setField(8, decimal);
        testRow.setField(9, doubles);
        testRow.setField(10, date);
        testRow.setField(11, time);
        testRow.setField(12, timestamp3.toLocalDateTime());
        testRow.setField(13, timestamp9.toLocalDateTime());
        testRow.setField(14, timestampWithLocalZone);
        testRow.setField(15, MAP);
        testRow.setField(16, MULTI_SET);
        testRow.setField(17, NESTED_MAP);
        for (int position : positions) {
            testRow.setField(position, null);
        }
        return testRow;
    }

    private void setRandomKind(Row testRow) {
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
    private Row convertToExternal(RowData rowData, DataType dataType) {
        return (Row) DataFormatConverters.getConverterForDataType(dataType).toExternal(rowData);
    }

    @SuppressWarnings("unchecked")
    private GenericRowData convertToInternal(Row row) {
        DataFormatConverters.DataFormatConverter<GenericRowData, Row> converter =
                DataFormatConverters.getConverterForDataType(ROW(getFields()));
        return converter.toInternal(row);
    }
}

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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.planner.functions.casting.RowDataToStringConverterImpl;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.table.utils.print.RowDataToStringConverter;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResultInfoSerializer} and {@link ResultInfoDeserializer}. */
class ResultInfoJsonSerDeTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Row testRow = initRow();

    @BeforeAll
    static void setUp() {
        SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(ResultInfo.class, new ResultInfoSerializer());
        simpleModule.addDeserializer(ResultInfo.class, new ResultInfoDeserializer());
        OBJECT_MAPPER.registerModule(simpleModule);
    }

    @ParameterizedTest
    @EnumSource(RowFormat.class)
    void testResultInfoSerDeWithSingleRow(RowFormat rowFormat) throws Exception {
        serDeTest(Collections.singletonList(testRow), rowFormat);
    }

    @ParameterizedTest
    @EnumSource(RowFormat.class)
    void testResultInfoSerDeWithMultiRowData(RowFormat rowFormat) throws Exception {
        serDeTest(Collections.nCopies(10, testRow), rowFormat);
    }

    @ParameterizedTest
    @EnumSource(RowFormat.class)
    void testResultInfoSerDeWithNullValues(RowFormat rowFormat) throws Exception {
        List<Integer> positions =
                IntStream.range(0, 20)
                        .mapToObj(i -> new Random().nextInt(20))
                        .collect(Collectors.toList());

        serDeTest(
                Collections.nCopies(10, getTestRowDataWithNullValues(initRow(), positions)),
                rowFormat);
    }

    @ParameterizedTest
    @ValueSource(strings = {"result_info_json_format.txt", "result_info_plain_text_format.txt"})
    void testDeserializationFromJson(String fileName) throws Exception {
        URL url = ResultInfoJsonSerDeTest.class.getClassLoader().getResource(fileName);
        String input =
                IOUtils.toString(Preconditions.checkNotNull(url), StandardCharsets.UTF_8).trim();
        ResultInfo deserializedResult = OBJECT_MAPPER.readValue(input, ResultInfo.class);
        assertThat(OBJECT_MAPPER.writeValueAsString(deserializedResult)).isEqualTo(input);
    }

    private void serDeTest(List<Row> rows, RowFormat rowFormat) throws IOException {
        List<RowData> rowDatas =
                rows.stream().map(this::convertToInternal).collect(Collectors.toList());
        if (rowFormat == RowFormat.PLAIN_TEXT) {
            rowDatas =
                    rowDatas.stream()
                            .map(this::toPlainTextFormatRowData)
                            .collect(Collectors.toList());
        }

        ResolvedSchema testResolvedSchema = getTestResolvedSchema(getFields());
        ResultInfo testResultInfo =
                new ResultInfo(
                        testResolvedSchema.getColumns().stream()
                                .map(ColumnInfo::toColumnInfo)
                                .collect(Collectors.toList()),
                        rowDatas,
                        rowFormat);

        // test serialization & deserialization
        String result = OBJECT_MAPPER.writeValueAsString(testResultInfo);
        ResultInfo resultInfo = OBJECT_MAPPER.readValue(result, ResultInfo.class);

        // validate schema
        assertThat(resultInfo.getResultSchema())
                .hasToString(testResultInfo.getResultSchema().toString());

        // validate data
        assertDataWithFormat(resultInfo.getData(), rows, rowFormat);
    }

    private static Row initRow() {
        final byte tinyint = 'c';
        final short smallint = 128;
        final int intValue = 45536;
        final float floatValue = 33.333F;
        final long bigint = 1238123899121L;
        final String name = "asdlkjasjkdla998y1122";
        final byte[] bytes = new byte[1024];
        ThreadLocalRandom.current().nextBytes(bytes);
        final Double[] doubles = new Double[] {1.1, 2.2, 3.3};
        final BigDecimal decimal = new BigDecimal("123.456789");
        final LocalDate date = LocalDate.parse("1990-10-14");
        final LocalTime time = LocalTime.parse("12:12:43");
        final Timestamp timestamp3 = Timestamp.valueOf("1990-10-14 12:12:43.123");
        final Timestamp timestamp9 = Timestamp.valueOf("1990-10-14 12:12:43.123456789");
        final long duration = Duration.of(123, ChronoUnit.HOURS).getSeconds();
        final int period = Period.of(1, 2, 3).getDays();
        final Instant timestampWithLocalZone =
                LocalDateTime.of(1990, 10, 14, 12, 12, 43, 123456789)
                        .atOffset(ZoneOffset.of("Z"))
                        .toInstant();

        final Map<String, Long> map = new HashMap<>();
        map.put("element", 123L);

        final Map<String, Integer> multiSet = new HashMap<>();
        multiSet.put("element", 2);

        final Map<String, Map<String, Integer>> nestedMap = new HashMap<>();
        Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("key", 234);
        nestedMap.put("inner_map", innerMap);

        Row testRow = new Row(20);
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
        testRow.setField(18, duration);
        testRow.setField(19, period);
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
                FIELD("map2map", MAP(STRING(), MAP(STRING(), INT()))),
                FIELD("dayTimeInterval", INTERVAL(DAY(), SECOND(3))),
                FIELD("yearMonthInterval", INTERVAL(YEAR(), MONTH())));
    }

    private ResolvedSchema getTestResolvedSchema(List<DataTypes.Field> fields) {
        List<String> columnNames =
                fields.stream().map(DataTypes.AbstractField::getName).collect(Collectors.toList());
        List<DataType> columnDataTypes =
                fields.stream().map(DataTypes.Field::getDataType).collect(Collectors.toList());
        return ResolvedSchema.physical(columnNames, columnDataTypes);
    }

    private Row getTestRowDataWithNullValues(Row testRow, List<Integer> positions) {
        for (int position : positions) {
            testRow.setField(position, null);
        }
        return testRow;
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

    private RowData toPlainTextFormatRowData(RowData rowData) {
        RowDataToStringConverter converter =
                new RowDataToStringConverterImpl(
                        getTestResolvedSchema(getFields()).toPhysicalRowDataType(),
                        DateTimeUtils.UTC_ZONE.toZoneId(),
                        ResultInfoJsonSerDeTest.class.getClassLoader(),
                        false);

        StringData[] plainText =
                Arrays.stream(converter.convert(rowData))
                        .map(StringData::fromString)
                        .toArray(StringData[]::new);

        return GenericRowData.ofKind(rowData.getRowKind(), (Object[]) plainText);
    }

    private void assertDataWithFormat(
            List<RowData> expected, List<Row> actual, RowFormat rowFormat) {
        if (rowFormat == RowFormat.JSON) {
            for (int i = 0; i < expected.size(); i++) {
                assertThat(convertToExternal(expected.get(i), ROW(getFields())))
                        .isEqualTo(actual.get(i));
            }
        } else {
            for (int i = 0; i < expected.size(); i++) {
                assertPlainTextFormatData(
                        expected.get(i),
                        toPlainTextFormatRowData(convertToInternal(actual.get(i))));
            }
        }
    }

    private void assertPlainTextFormatData(RowData expected, RowData actual) {
        for (int i = 0; i < expected.getArity(); i++) {
            assertThat(expected.getString(i)).isEqualTo(actual.getString(i));
        }
    }
}

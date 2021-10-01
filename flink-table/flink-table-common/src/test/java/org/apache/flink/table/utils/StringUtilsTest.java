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

package org.apache.flink.table.utils;

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
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
import static org.apache.flink.table.api.DataTypes.NULL;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.junit.jupiter.api.Assertions.assertEquals;

class StringUtilsTest {

    private static Stream<Arguments> toSQLStringCases() {
        return Stream.of(
                // Any null input, regardless of type, returns NULL
                Arguments.of(null, NULL(), "NULL"),
                Arguments.of(null, INT().nullable(), "NULL"),

                // Atomic types
                Arguments.of(true, BOOLEAN(), "TRUE"),
                Arguments.of(false, BOOLEAN(), "FALSE"),
                Arguments.of(new byte[] {0, 1}, BINARY(2), "\u0000\u0001"),
                Arguments.of(new byte[] {0, 1}, BINARY(3), "\u0000\u0001\u0000"),
                Arguments.of(new byte[] {0, 1, 2}, VARBINARY(3), "\u0000\u0001\u0002"),
                Arguments.of(new byte[] {0, 1, 2}, VARBINARY(5), "\u0000\u0001\u0002"),
                Arguments.of(new byte[] {0, 1, 2, 3, 4}, BYTES(), "\u0000\u0001\u0002\u0003\u0004"),
                Arguments.of(-125, TINYINT(), "-125"),
                Arguments.of(32767, SMALLINT(), "32767"),
                Arguments.of(-12345678, INT(), "-12345678"),
                Arguments.of(1234567891234L, BIGINT(), "1234567891234"),
                Arguments.of(-123.456, FLOAT(), "-123.456"),
                Arguments.of(12345.67890, DOUBLE(), "12345.6789"),
                Arguments.of(9.87D, DECIMAL(4, 3), "9.870"),
                Arguments.of(new BigDecimal("9.87"), DECIMAL(4, 3), "9.870"),

                // Date and time
                Arguments.of(LocalDate.parse("2021-09-24"), DATE(), "2021-09-24"),
                Arguments.of(Date.valueOf("2021-09-24"), DATE(), "2021-09-24"),
                Arguments.of(LocalTime.parse("12:34:56.12"), TIME(2), "12:34:56.12"),
                Arguments.of(Time.valueOf("12:34:56"), TIME(), "12:34:56"),
                Arguments.of(
                        LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                        TIMESTAMP(),
                        "2021-09-24 12:34:56.123456"),
                Arguments.of(
                        LocalDateTime.parse("2021-09-24T12:34:56.123"),
                        TIMESTAMP(4),
                        "2021-09-24 12:34:56.1230"),
                Arguments.of(
                        TimestampData.fromLocalDateTime(
                                LocalDateTime.parse("2021-09-24T12:34:56.123456")),
                        TIMESTAMP(),
                        "2021-09-24 12:34:56.123456"),
                Arguments.of(
                        Timestamp.valueOf("2021-09-24 12:34:56.123456"),
                        TIMESTAMP(),
                        "2021-09-24 12:34:56.123456"),

                // Intervals
                Arguments.of(84, INTERVAL(YEAR()), "+7-00"),
                Arguments.of(Period.ofMonths(84), INTERVAL(YEAR()), "+7-00"),
                Arguments.of(5, INTERVAL(MONTH()), "+0-05"),
                Arguments.of(Period.ofMonths(123).negated(), INTERVAL(MONTH()), "-10-03"),
                Arguments.of(12334, INTERVAL(MONTH()), "+1027-10"),
                Arguments.of(10, INTERVAL(DAY()), "+0 00:00:00.010"),
                Arguments.of(-123456789L, INTERVAL(DAY()), "-1 10:17:36.789"),
                Arguments.of(Duration.ofHours(36), INTERVAL(DAY()), "+1 12:00:00.000"),

                // Composite
                Arguments.of(
                        Arrays.asList(Period.ofMonths(123).negated(), Period.ofMonths(123)),
                        ARRAY(INTERVAL(MONTH())),
                        "[-10-03, +10-03]"),
                Arguments.of(Arrays.asList(123, -456), ARRAY(INT()), "[123, -456]"),
                Arguments.of(
                        GenericRowData.of(123, StringData.fromString("abc")),
                        ROW(FIELD("f0", INT()), FIELD("f1", STRING())),
                        "(123, abc)"),
                Arguments.of(
                        Row.of(123, "abc"),
                        ROW(FIELD("f0", INT()), FIELD("f1", STRING())),
                        "(123, abc)"),
                Arguments.of(testMap(), MAP(STRING(), INTERVAL(MONTH())), "{a=+10-03, b=-10-03}"),
                Arguments.of(
                        testMapData(), MAP(STRING(), INTERVAL(MONTH())), "{a=+10-03, b=-10-03}"));
    }

    @ParameterizedTest
    @MethodSource("toSQLStringCases")
    void toSQLString(Object input, DataType inputType, String expected) {
        assertEquals(expected, StringUtils.toSQLString(input, inputType.getLogicalType()));
    }

    /**
     * TIMESTAMP_LTZ requires a specific test to avoid different results based on different system
     * timezones.
     */
    @Test
    void toSQLStringWithTimestampLTZ() {
        LogicalType timestampType = TIMESTAMP_LTZ().getLogicalType();

        assertEquals(
                "2021-09-24 12:34:56.123456",
                StringUtils.toSQLString(
                        TimestampData.fromInstant(Instant.parse("2021-09-24T12:34:56.123456Z")),
                        timestampType,
                        DateTimeUtils.UTC_ZONE.toZoneId()));

        assertEquals(
                "2021-09-24 12:34:56.000000",
                StringUtils.toSQLString(
                        Instant.parse("2021-09-24T12:34:56Z"),
                        timestampType,
                        DateTimeUtils.UTC_ZONE.toZoneId()));

        assertEquals(
                "2021-09-24 12:34:56.123456",
                StringUtils.toSQLString(
                        Timestamp.valueOf("2021-09-24 12:34:56.123456"),
                        timestampType,
                        ZoneId.systemDefault()));
    }

    private static Map<String, Period> testMap() {
        Map<String, Period> map = new HashMap<>();
        map.put("a", Period.ofMonths(123));
        map.put("b", Period.ofMonths(123).negated());
        return map;
    }

    private static MapData testMapData() {
        Map<StringData, Integer> map = new HashMap<>();
        map.put(StringData.fromString("a"), 123);
        map.put(StringData.fromString("b"), -123);
        return new GenericMapData(map);
    }
}

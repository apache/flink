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

package org.apache.flink.table.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.TimePointUnit;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.utils.ValueDataTypeConverter;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for {@link ValueDataTypeConverter}. */
class ValueDataTypeConverterTest {

    private static Stream<Arguments> testData() {
        return Stream.of(
                of("Hello World", DataTypes.CHAR(11)),
                of("", new AtomicDataType(CharType.ofEmptyLiteral())),
                of(new byte[] {1, 2, 3}, DataTypes.BINARY(3)),
                of(new byte[0], new AtomicDataType(BinaryType.ofEmptyLiteral())),
                of(BigDecimal.ZERO, DataTypes.DECIMAL(1, 0)),
                of(new BigDecimal("12.123"), DataTypes.DECIMAL(5, 3)),
                of(new BigDecimal("1E+36"), DataTypes.DECIMAL(37, 0)),
                of(12, DataTypes.INT()),
                of(LocalTime.of(13, 24, 25, 1000), DataTypes.TIME(6)),
                of(LocalTime.of(13, 24, 25, 0), DataTypes.TIME(0)),
                of(LocalTime.of(13, 24, 25, 1), DataTypes.TIME(9)),
                of(LocalTime.of(13, 24, 25, 999_999_999), DataTypes.TIME(9)),
                of(LocalDateTime.of(2019, 11, 11, 13, 24, 25, 1001), DataTypes.TIMESTAMP(9)),
                of(
                        ZonedDateTime.of(2019, 11, 11, 13, 24, 25, 1001, ZoneId.systemDefault()),
                        DataTypes.TIMESTAMP_WITH_TIME_ZONE(9).bridgedTo(ZonedDateTime.class)),
                of(
                        OffsetDateTime.of(2019, 11, 11, 13, 24, 25, 1001, ZoneOffset.UTC),
                        DataTypes.TIMESTAMP_WITH_TIME_ZONE(9).bridgedTo(OffsetDateTime.class)),
                of(
                        Instant.ofEpochMilli(12345602021L),
                        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(Instant.class)),
                of(
                        Period.ofYears(1000),
                        DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH())
                                .bridgedTo(Period.class)),
                of(
                        Duration.ofMillis(1100),
                        DataTypes.INTERVAL(DataTypes.DAY(1), DataTypes.SECOND(1))
                                .bridgedTo(Duration.class)),
                of(
                        Duration.ofDays(42),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.SECOND(0))
                                .bridgedTo(Duration.class)),
                of(
                        Timestamp.valueOf("2018-01-01 12:13:14.123"),
                        DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class)),
                of(new Integer[] {1, 2, 3}, DataTypes.ARRAY(DataTypes.INT())),
                of(new Integer[] {1, null, 3}, DataTypes.ARRAY(DataTypes.INT())),
                of(
                        new BigDecimal[] {
                            new BigDecimal("12.1234"),
                            new BigDecimal("42.4321"),
                            new BigDecimal("20.0000")
                        },
                        DataTypes.ARRAY(DataTypes.DECIMAL(6, 4))),
                of(
                        new BigDecimal[] {null, new BigDecimal("42.4321")},
                        DataTypes.ARRAY(DataTypes.DECIMAL(6, 4))),
                of(new Integer[0], DataTypes.ARRAY(DataTypes.INT())),
                of(
                        new Integer[][] {
                            new Integer[] {1, null, 3}, new Integer[0], new Integer[] {1}
                        },
                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
                of(TimePointUnit.HOUR, new AtomicDataType(new SymbolType<>(), TimePointUnit.class)),
                of(new BigDecimal[0], null));
    }

    @ParameterizedTest(name = "[{index}] value: {0} type: {1}")
    @MethodSource("testData")
    void testClassToDataTypeConversion(Object value, DataType dataType) {
        if (dataType == null) {
            assertThat(ValueDataTypeConverter.extractDataType(value)).isEmpty();
        } else {
            assertThat(ValueDataTypeConverter.extractDataType(value)).contains(dataType.notNull());
        }
    }
}

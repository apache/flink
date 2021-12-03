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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.CastExecutor;
import org.apache.flink.table.planner.functions.CastFunctionITCase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.DateTimeUtils;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
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
import static org.apache.flink.table.api.DataTypes.RAW;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.api.DataTypes.YEAR;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class runs unit tests of {@link CastRule} implementations. For IT test cases, check out the
 * {@link CastFunctionITCase}
 */
class CastRulesTest {

    private static final ZoneId CET = ZoneId.of("CET");

    private static final CastRule.Context CET_CONTEXT =
            CastRule.Context.create(false, CET, Thread.currentThread().getContextClassLoader());

    private static final byte DEFAULT_POSITIVE_TINY_INT = (byte) 5;
    private static final byte DEFAULT_NEGATIVE_TINY_INT = (byte) -5;
    private static final short DEFAULT_POSITIVE_SMALL_INT = (short) 12345;
    private static final short DEFAULT_NEGATIVE_SMALL_INT = (short) -12345;
    private static final int DEFAULT_POSITIVE_INT = 1234567;
    private static final int DEFAULT_NEGATIVE_INT = -1234567;
    private static final long DEFAULT_POSITIVE_BIGINT = 12345678901L;
    private static final long DEFAULT_NEGATIVE_BIGINT = -12345678901L;
    private static final float DEFAULT_POSITIVE_FLOAT = 123.456f;
    private static final float DEFAULT_NEGATIVE_FLOAT = -123.456f;
    private static final double DEFAULT_POSITIVE_DOUBLE = 123.456789d;
    private static final double DEFAULT_NEGATIVE_DOUBLE = -123.456789d;

    private static final int DATE = DateTimeUtils.toInternal(LocalDate.parse("2021-09-24"));
    private static final int TIME = DateTimeUtils.toInternal(LocalTime.parse("12:34:56.123"));
    private static final StringData DATE_STRING = StringData.fromString("2021-09-24");
    private static final StringData TIME_STRING = StringData.fromString("12:34:56.123");

    private static final TimestampData TIMESTAMP =
            TimestampData.fromLocalDateTime(LocalDateTime.parse("2021-09-24T12:34:56.123456"));
    private static final StringData TIMESTAMP_STRING =
            StringData.fromString("2021-09-24 12:34:56.123456");
    private static final StringData TIMESTAMP_STRING_CET =
            StringData.fromString("2021-09-24 14:34:56.123456");

    Stream<CastTestSpecBuilder> testCases() {
        return Stream.of(
                CastTestSpecBuilder.testCastTo(TINYINT())
                        .fromCase(TINYINT(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), (byte) 1)
                        .fromCase(STRING(), StringData.fromString("123"), (byte) 123)
                        .fail(STRING(), StringData.fromString("-130"), TableException.class)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                (byte) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(
                                DECIMAL(10, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9123.87"), 10, 3),
                                (byte) -93)
                        .fromCase(TINYINT(), DEFAULT_POSITIVE_TINY_INT, DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(TINYINT(), DEFAULT_NEGATIVE_TINY_INT, DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(SMALLINT(), (short) 32, (byte) 32)
                        .fromCase(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT, (byte) 57)
                        .fromCase(SMALLINT(), DEFAULT_NEGATIVE_SMALL_INT, (byte) -57)
                        .fromCase(INT(), -12, (byte) -12)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (byte) -121)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (byte) 121)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, (byte) 53)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, (byte) -53)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, (byte) 123)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, (byte) -123)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, (byte) 123)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, (byte) -123)
                        .fromCase(BOOLEAN(), true, (byte) 1)
                        .fromCase(BOOLEAN(), false, (byte) 0),
                CastTestSpecBuilder.testCastTo(SMALLINT())
                        .fromCase(SMALLINT(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), (short) 1)
                        .fromCase(STRING(), StringData.fromString("123"), (short) 123)
                        .fail(STRING(), StringData.fromString("-32769"), TableException.class)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                (short) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(
                                DECIMAL(10, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("91235.87"), 10, 3),
                                (short) 25699)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                (short) DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                (short) DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(
                                SMALLINT(), DEFAULT_POSITIVE_SMALL_INT, DEFAULT_POSITIVE_SMALL_INT)
                        .fromCase(
                                SMALLINT(), DEFAULT_NEGATIVE_SMALL_INT, DEFAULT_NEGATIVE_SMALL_INT)
                        .fromCase(SMALLINT(), (short) 32780, (short) -32756)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (short) -10617)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (short) 10617)
                        .fromCase(INT(), -12, (short) -12)
                        .fromCase(BIGINT(), 123L, (short) 123)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, (short) 7221)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, (short) -7221)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, (short) 123)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, (short) -123)
                        .fromCase(FLOAT(), 123456.78f, (short) -7616)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, (short) 123)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, (short) -123)
                        .fromCase(DOUBLE(), 123456.7890d, (short) -7616)
                        .fromCase(BOOLEAN(), true, (short) 1)
                        .fromCase(BOOLEAN(), false, (short) 0),
                CastTestSpecBuilder.testCastTo(INT())
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), 1)
                        .fromCase(STRING(), StringData.fromString("123"), 123)
                        .fail(
                                STRING(),
                                StringData.fromString("-3276913443134"),
                                TableException.class)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(
                                DECIMAL(20, 3),
                                DecimalData.fromBigDecimal(
                                        new BigDecimal("3276913443134.87"), 20, 3),
                                -146603714)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                (int) DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                (int) DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_POSITIVE_SMALL_INT,
                                (int) DEFAULT_POSITIVE_SMALL_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_NEGATIVE_SMALL_INT,
                                (int) DEFAULT_NEGATIVE_SMALL_INT)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, DEFAULT_POSITIVE_INT)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, DEFAULT_NEGATIVE_INT)
                        .fromCase(BIGINT(), 123L, 123)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, -539222987)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, 539222987)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, 123)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, -123)
                        .fromCase(FLOAT(), 9234567891.12f, 2147483647)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123)
                        .fromCase(DOUBLE(), 9234567891.12345d, 2147483647)
                        .fromCase(INTERVAL(YEAR(), MONTH()), 123, 123)
                        .fromCase(INTERVAL(DAY(), SECOND()), 123L, 123)
                        .fromCase(BOOLEAN(), true, 1)
                        .fromCase(BOOLEAN(), false, 0),
                CastTestSpecBuilder.testCastTo(BIGINT())
                        .fromCase(BIGINT(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), 1L)
                        .fromCase(STRING(), StringData.fromString("123"), 123L)
                        .fromCase(
                                STRING(), StringData.fromString("-3276913443134"), -3276913443134L)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                9L)
                        .fromCase(
                                DECIMAL(20, 3),
                                DecimalData.fromBigDecimal(
                                        new BigDecimal("3276913443134.87"), 20, 3),
                                3276913443134L)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                (long) DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                (long) DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_POSITIVE_SMALL_INT,
                                (long) DEFAULT_POSITIVE_SMALL_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_NEGATIVE_SMALL_INT,
                                (long) DEFAULT_NEGATIVE_SMALL_INT)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (long) DEFAULT_POSITIVE_INT)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (long) DEFAULT_NEGATIVE_INT)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, DEFAULT_POSITIVE_BIGINT)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, DEFAULT_NEGATIVE_BIGINT)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, 123L)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, -123L)
                        .fromCase(FLOAT(), 9234567891.12f, 9234568192L)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123L)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123L)
                        .fromCase(DOUBLE(), 9234567891.12345d, 9234567891L)
                        .fromCase(BOOLEAN(), true, 1L)
                        .fromCase(BOOLEAN(), false, 0L),
                CastTestSpecBuilder.testCastTo(FLOAT())
                        .fromCase(FLOAT(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), 1.234f)
                        .fromCase(STRING(), StringData.fromString("123"), 123.0f)
                        .fromCase(
                                STRING(), StringData.fromString("-3276913443134"), -3.27691351E12f)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                9.87f)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(
                                DECIMAL(20, 3),
                                DecimalData.fromBigDecimal(
                                        new BigDecimal("3276913443134.87"), 20, 3),
                                3.27691351E12f)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                (float) DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                (float) DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_POSITIVE_SMALL_INT,
                                (float) DEFAULT_POSITIVE_SMALL_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_NEGATIVE_SMALL_INT,
                                (float) DEFAULT_NEGATIVE_SMALL_INT)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (float) DEFAULT_POSITIVE_INT)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (float) DEFAULT_NEGATIVE_INT)
                        .fromCase(
                                BIGINT(), DEFAULT_POSITIVE_BIGINT, (float) DEFAULT_POSITIVE_BIGINT)
                        .fromCase(
                                BIGINT(), DEFAULT_NEGATIVE_BIGINT, (float) DEFAULT_NEGATIVE_BIGINT)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, DEFAULT_POSITIVE_FLOAT)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, DEFAULT_NEGATIVE_FLOAT)
                        .fromCase(FLOAT(), 9234567891.12f, 9234567891.12f)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123.456789f)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123.456789f)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234d, 1.23923451E12f)
                        .fromCase(BOOLEAN(), true, 1.0f)
                        .fromCase(BOOLEAN(), false, 0.0f),
                CastTestSpecBuilder.testCastTo(DOUBLE())
                        .fromCase(DOUBLE(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("1.234"), 1.234d)
                        .fromCase(STRING(), StringData.fromString("123"), 123.0d)
                        .fromCase(
                                STRING(),
                                StringData.fromString("-3276913443134"),
                                -3.276913443134E12d)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                9.87d)
                        .fromCase(
                                DECIMAL(20, 3),
                                DecimalData.fromBigDecimal(
                                        new BigDecimal("3276913443134.87"), 20, 3),
                                3.27691344313487E12d)
                        .fromCase(
                                DECIMAL(30, 20),
                                DecimalData.fromBigDecimal(
                                        new BigDecimal("123456789.123456789123456789"), 30, 20),
                                1.2345678912345679E8d)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                (double) DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                (double) DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_POSITIVE_SMALL_INT,
                                (double) DEFAULT_POSITIVE_SMALL_INT)
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_NEGATIVE_SMALL_INT,
                                (double) DEFAULT_NEGATIVE_SMALL_INT)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (double) DEFAULT_POSITIVE_INT)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (double) DEFAULT_NEGATIVE_INT)
                        .fromCase(
                                BIGINT(), DEFAULT_POSITIVE_BIGINT, (double) DEFAULT_POSITIVE_BIGINT)
                        .fromCase(
                                BIGINT(), DEFAULT_NEGATIVE_BIGINT, (double) DEFAULT_NEGATIVE_BIGINT)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, 123.45600128173828d)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, -123.45600128173828)
                        .fromCase(FLOAT(), 9234567891.12f, 9.234568192E9)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, DEFAULT_POSITIVE_DOUBLE)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, DEFAULT_NEGATIVE_DOUBLE)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234d, 1.2392345678911235E12d)
                        .fromCase(BOOLEAN(), true, 1.0d)
                        .fromCase(BOOLEAN(), false, 0.0d),
                CastTestSpecBuilder.testCastTo(DATE())
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("123"),
                                DateTimeUtils.toInternal(LocalDate.of(123, 1, 1)))
                        .fromCase(
                                STRING(),
                                StringData.fromString("2021-09-27"),
                                DateTimeUtils.toInternal(LocalDate.of(2021, 9, 27)))
                        .fromCase(
                                STRING(),
                                StringData.fromString("2021-09-27 12:34:56.123456789"),
                                DateTimeUtils.toInternal(LocalDate.of(2021, 9, 27)))
                        .fail(STRING(), StringData.fromString("2021/09/27"), TableException.class),
                CastTestSpecBuilder.testCastTo(TIME())
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("23"),
                                DateTimeUtils.toInternal(LocalTime.of(23, 0, 0)))
                        .fromCase(
                                STRING(),
                                StringData.fromString("23:45"),
                                DateTimeUtils.toInternal(LocalTime.of(23, 45, 0)))
                        .fail(STRING(), StringData.fromString("2021-09-27"), TableException.class)
                        .fail(
                                STRING(),
                                StringData.fromString("2021-09-27 12:34:56"),
                                TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("12:34:56.123456789"),
                                DateTimeUtils.toInternal(LocalTime.of(12, 34, 56, 123_000_000)))
                        .fail(
                                STRING(),
                                StringData.fromString("2021-09-27 12:34:56.123456789"),
                                TableException.class),
                CastTestSpecBuilder.testCastTo(TIMESTAMP(9))
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("123"), TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("2021-09-27"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0)))
                        .fail(STRING(), StringData.fromString("2021/09/27"), TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("2021-09-27 12:34:56.123456789"),
                                TimestampData.fromLocalDateTime(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123456789))),
                CastTestSpecBuilder.testCastTo(TIMESTAMP_LTZ(9))
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("123"), TableException.class)
                        .fromCase(
                                STRING(),
                                CET_CONTEXT,
                                StringData.fromString("2021-09-27"),
                                TimestampData.fromInstant(
                                        LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0)
                                                .atZone(CET)
                                                .toInstant()))
                        .fromCase(
                                STRING(),
                                CET_CONTEXT,
                                StringData.fromString("2021-09-27 12:34:56.123"),
                                TimestampData.fromInstant(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123000000)
                                                .atZone(CET)
                                                .toInstant()))
                        // https://issues.apache.org/jira/browse/FLINK-24446 Fractional seconds are
                        // lost
                        .fromCase(
                                STRING(),
                                CET_CONTEXT,
                                StringData.fromString("2021-09-27 12:34:56.123456789"),
                                TimestampData.fromInstant(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 0)
                                                .atZone(CET)
                                                .toInstant())),
                CastTestSpecBuilder.testCastTo(STRING())
                        .fromCase(STRING(), null, null)
                        .fromCase(
                                CHAR(3), StringData.fromString("foo"), StringData.fromString("foo"))
                        .fromCase(
                                VARCHAR(5),
                                StringData.fromString("Flink"),
                                StringData.fromString("Flink"))
                        .fromCase(
                                VARCHAR(10),
                                StringData.fromString("Flink"),
                                StringData.fromString("Flink"))
                        .fromCase(
                                STRING(),
                                StringData.fromString("Apache Flink"),
                                StringData.fromString("Apache Flink"))
                        .fromCase(BOOLEAN(), true, StringData.fromString("true"))
                        .fromCase(BOOLEAN(), false, StringData.fromString("false"))
                        .fromCase(
                                BINARY(2), new byte[] {0, 1}, StringData.fromString("\u0000\u0001"))
                        .fromCase(
                                VARBINARY(3),
                                new byte[] {0, 1, 2},
                                StringData.fromString("\u0000\u0001\u0002"))
                        .fromCase(
                                VARBINARY(5),
                                new byte[] {0, 1, 2},
                                StringData.fromString("\u0000\u0001\u0002"))
                        .fromCase(
                                BYTES(),
                                new byte[] {0, 1, 2, 3, 4},
                                StringData.fromString("\u0000\u0001\u0002\u0003\u0004"))
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                StringData.fromString("9.870"))
                        .fromCase(
                                DECIMAL(5, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 5, 3),
                                StringData.fromString("9.870"))
                        .fromCase(TINYINT(), (byte) -125, StringData.fromString("-125"))
                        .fromCase(SMALLINT(), (short) 32767, StringData.fromString("32767"))
                        .fromCase(INT(), -12345678, StringData.fromString("-12345678"))
                        .fromCase(BIGINT(), 1234567891234L, StringData.fromString("1234567891234"))
                        .fromCase(FLOAT(), -123.456f, StringData.fromString("-123.456"))
                        .fromCase(DOUBLE(), 12345.678901d, StringData.fromString("12345.678901"))
                        .fromCase(
                                FLOAT(),
                                Float.MAX_VALUE,
                                StringData.fromString(String.valueOf(Float.MAX_VALUE)))
                        .fromCase(
                                DOUBLE(),
                                Double.MAX_VALUE,
                                StringData.fromString(String.valueOf(Double.MAX_VALUE)))
                        .fromCase(
                                STRING(),
                                StringData.fromString("Hello"),
                                StringData.fromString("Hello"))
                        .fromCase(TIMESTAMP(), TIMESTAMP, TIMESTAMP_STRING)
                        .fromCase(TIMESTAMP_LTZ(), CET_CONTEXT, TIMESTAMP, TIMESTAMP_STRING_CET)
                        .fromCase(DATE(), DATE, DATE_STRING)
                        .fromCase(TIME(5), TIME, TIME_STRING)
                        .fromCase(INTERVAL(YEAR()), 84, StringData.fromString("+7-00"))
                        .fromCase(INTERVAL(MONTH()), 5, StringData.fromString("+0-05"))
                        .fromCase(INTERVAL(MONTH()), 123, StringData.fromString("+10-03"))
                        .fromCase(INTERVAL(MONTH()), 12334, StringData.fromString("+1027-10"))
                        .fromCase(INTERVAL(DAY()), 10L, StringData.fromString("+0 00:00:00.010"))
                        .fromCase(
                                INTERVAL(DAY()),
                                123456789L,
                                StringData.fromString("+1 10:17:36.789"))
                        .fromCase(
                                INTERVAL(DAY()),
                                Duration.ofHours(36).toMillis(),
                                StringData.fromString("+1 12:00:00.000"))
                        .fromCase(
                                ARRAY(INTERVAL(MONTH())),
                                new GenericArrayData(new int[] {-123, 123}),
                                StringData.fromString("[-10-03, +10-03]"))
                        .fromCase(
                                ARRAY(INT()),
                                new GenericArrayData(new int[] {-123, 456}),
                                StringData.fromString("[-123, 456]"))
                        .fromCase(
                                ARRAY(INT().nullable()),
                                new GenericArrayData(new Integer[] {null, 456}),
                                StringData.fromString("[null, 456]"))
                        .fromCase(
                                ARRAY(INT()),
                                new GenericArrayData(new Integer[] {}),
                                StringData.fromString("[]"))
                        .fromCase(
                                MAP(STRING(), INTERVAL(MONTH())),
                                mapData(
                                        entry(StringData.fromString("a"), -123),
                                        entry(StringData.fromString("b"), 123)),
                                StringData.fromString("{a=-10-03, b=+10-03}"))
                        .fromCase(
                                MULTISET(STRING()),
                                mapData(
                                        entry(StringData.fromString("a"), 1),
                                        entry(StringData.fromString("b"), 1)),
                                StringData.fromString("{a=1, b=1}"))
                        .fromCase(
                                MAP(STRING().nullable(), INTERVAL(MONTH()).nullable()),
                                mapData(entry(null, -123), entry(StringData.fromString("b"), null)),
                                StringData.fromString("{null=-10-03, b=null}"))
                        .fromCase(
                                MAP(STRING().nullable(), INTERVAL(MONTH()).nullable()),
                                mapData(entry(null, null)),
                                StringData.fromString("{null=null}"))
                        .fromCase(
                                MAP(STRING(), INTERVAL(MONTH())),
                                mapData(),
                                StringData.fromString("{}"))
                        .fromCase(
                                ROW(FIELD("f0", INT()), FIELD("f1", STRING())),
                                GenericRowData.of(123, StringData.fromString("abc")),
                                StringData.fromString("(123, abc)"))
                        .fromCase(
                                ROW(FIELD("f0", STRING()), FIELD("f1", STRING())),
                                GenericRowData.of(
                                        StringData.fromString("abc"), StringData.fromString("def")),
                                StringData.fromString("(abc, def)"),
                                false)
                        .fromCase(
                                ROW(FIELD("f0", STRING()), FIELD("f1", STRING())),
                                GenericRowData.of(
                                        StringData.fromString("abc"), StringData.fromString("def")),
                                StringData.fromString("(abc,def)"),
                                true)
                        .fromCase(
                                ROW(FIELD("f0", INT().nullable()), FIELD("f1", STRING())),
                                GenericRowData.of(null, StringData.fromString("abc")),
                                StringData.fromString("(null, abc)"))
                        .fromCase(ROW(), GenericRowData.of(), StringData.fromString("()"))
                        .fromCase(
                                RAW(LocalDateTime.class, new LocalDateTimeSerializer()),
                                RawValueData.fromObject(
                                        LocalDateTime.parse("2020-11-11T18:08:01.123")),
                                StringData.fromString("2020-11-11T18:08:01.123")),
                CastTestSpecBuilder.testCastTo(BOOLEAN())
                        .fromCase(BOOLEAN(), null, null)
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fromCase(CHAR(4), StringData.fromString("true"), true)
                        .fromCase(VARCHAR(5), StringData.fromString("FalsE"), false)
                        .fail(STRING(), StringData.fromString("Apache Flink"), TableException.class)
                        .fromCase(STRING(), StringData.fromString("TRUE"), true)
                        .fail(STRING(), StringData.fromString(""), TableException.class)
                        // Should fail when https://issues.apache.org/jira/browse/FLINK-24576 is
                        // fixed
                        .fromCase(
                                DECIMAL(5, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("0.000"), 5, 3),
                                false)
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("1.987"), 4, 3),
                                true)
                        .fromCase(TINYINT(), DEFAULT_POSITIVE_TINY_INT, true)
                        .fromCase(TINYINT(), DEFAULT_NEGATIVE_TINY_INT, true)
                        .fromCase(TINYINT(), (byte) 0, false)
                        .fromCase(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT, true)
                        .fromCase(SMALLINT(), DEFAULT_NEGATIVE_SMALL_INT, true)
                        .fromCase(SMALLINT(), (short) 0, false)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, true)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, true)
                        .fromCase(INT(), 0, false)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, true)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, true)
                        .fromCase(BIGINT(), 0L, false)
                        // Should fail when https://issues.apache.org/jira/browse/FLINK-24576 is
                        // fixed
                        .fromCase(FLOAT(), 0f, false)
                        .fromCase(FLOAT(), 1.1234f, true)
                        .fromCase(DOUBLE(), 0.0d, false)
                        .fromCase(DOUBLE(), -0.12345678d, true),
                CastTestSpecBuilder.testCastTo(BINARY(2))
                        .fromCase(CHAR(3), StringData.fromString("foo"), new byte[] {102, 111, 111})
                        .fromCase(
                                VARCHAR(5),
                                StringData.fromString("Flink"),
                                new byte[] {70, 108, 105, 110, 107})
                        // https://issues.apache.org/jira/browse/FLINK-24419 - not trimmed to 2
                        // bytes
                        .fromCase(
                                STRING(),
                                StringData.fromString("Apache"),
                                new byte[] {65, 112, 97, 99, 104, 101}),
                CastTestSpecBuilder.testCastTo(VARBINARY(4))
                        .fromCase(CHAR(3), StringData.fromString("foo"), new byte[] {102, 111, 111})
                        .fromCase(
                                VARCHAR(5),
                                StringData.fromString("Flink"),
                                new byte[] {70, 108, 105, 110, 107})
                        // https://issues.apache.org/jira/browse/FLINK-24419 - not trimmed to 2
                        // bytes
                        .fromCase(
                                STRING(),
                                StringData.fromString("Apache"),
                                new byte[] {65, 112, 97, 99, 104, 101}),
                CastTestSpecBuilder.testCastTo(BYTES())
                        .fromCase(CHAR(3), StringData.fromString("foo"), new byte[] {102, 111, 111})
                        .fromCase(
                                VARCHAR(5),
                                StringData.fromString("Flink"),
                                new byte[] {70, 108, 105, 110, 107})
                        .fromCase(
                                STRING(),
                                StringData.fromString("Apache"),
                                new byte[] {65, 112, 97, 99, 104, 101}),
                CastTestSpecBuilder.testCastTo(DECIMAL(5, 3))
                        .fail(CHAR(3), StringData.fromString("foo"), TableException.class)
                        .fail(VARCHAR(5), StringData.fromString("Flink"), TableException.class)
                        .fail(STRING(), StringData.fromString("Apache"), TableException.class)
                        .fromCase(
                                STRING(),
                                StringData.fromString("1.234"),
                                DecimalData.fromBigDecimal(new BigDecimal("1.234"), 5, 3))
                        .fromCase(
                                STRING(),
                                StringData.fromString("1.2"),
                                DecimalData.fromBigDecimal(new BigDecimal("1.200"), 5, 3))
                        .fromCase(
                                DECIMAL(4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.87"), 4, 3),
                                DecimalData.fromBigDecimal(new BigDecimal("9.870"), 5, 3))
                        .fromCase(
                                TINYINT(),
                                (byte) -1,
                                DecimalData.fromBigDecimal(new BigDecimal("-1.000"), 5, 3))
                        .fromCase(
                                SMALLINT(),
                                (short) 3,
                                DecimalData.fromBigDecimal(new BigDecimal("3.000"), 5, 3))
                        .fromCase(
                                INT(),
                                42,
                                DecimalData.fromBigDecimal(new BigDecimal("42.000"), 5, 3))
                        .fromCase(
                                BIGINT(),
                                8L,
                                DecimalData.fromBigDecimal(new BigDecimal("8.000"), 5, 3))
                        .fromCase(
                                FLOAT(),
                                -12.345f,
                                DecimalData.fromBigDecimal(new BigDecimal("-12.345"), 5, 3))
                        .fromCase(
                                DOUBLE(),
                                12.678d,
                                DecimalData.fromBigDecimal(new BigDecimal("12.678"), 5, 3))
                        .fromCase(BOOLEAN(), true, DecimalData.fromBigDecimal(BigDecimal.ONE, 5, 3))
                        .fromCase(
                                BOOLEAN(),
                                false,
                                DecimalData.fromBigDecimal(BigDecimal.ZERO, 5, 3)),
                CastTestSpecBuilder.testCastTo(ARRAY(STRING().nullable()))
                        .fromCase(
                                ARRAY(TIMESTAMP().nullable()),
                                new GenericArrayData(
                                        new Object[] {
                                            TIMESTAMP,
                                            null,
                                            TimestampData.fromLocalDateTime(
                                                    LocalDateTime.parse(
                                                            "2021-09-24T14:34:56.123456"))
                                        }),
                                new GenericArrayData(
                                        new Object[] {
                                            TIMESTAMP_STRING,
                                            null,
                                            StringData.fromString("2021-09-24 14:34:56.123456")
                                        })),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().nullable()))
                        .fromCase(
                                ARRAY(INT().nullable()),
                                new GenericArrayData(new Object[] {1, null, 2}),
                                new GenericArrayData(new Object[] {1L, null, 2L})),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().notNull()))
                        .fromCase(
                                ARRAY(INT().notNull()),
                                new GenericArrayData(new int[] {1, 2}),
                                new GenericArrayData(new long[] {1L, 2L})),
                CastTestSpecBuilder.testCastTo(ARRAY(ARRAY(BIGINT().notNull())))
                        .fail(
                                ARRAY(ARRAY(INT().nullable())),
                                new GenericArrayData(
                                        new GenericArrayData[] {
                                            new GenericArrayData(new Integer[] {1, 2, null}),
                                            new GenericArrayData(new Integer[] {3})
                                        }),
                                NullPointerException.class));
    }

    @TestFactory
    Stream<DynamicTest> castTests() {
        return DynamicTest.stream(
                testCases().flatMap(CastTestSpecBuilder::toSpecs),
                CastTestSpec::toString,
                CastTestSpec::run);
    }

    private static <K, V> Map.Entry<K, V> entry(K k, V v) {
        return new AbstractMap.SimpleImmutableEntry<>(k, v);
    }

    @SafeVarargs
    private static <K, V> MapData mapData(Map.Entry<K, V>... entries) {
        if (entries == null) {
            return new GenericMapData(Collections.emptyMap());
        }
        Map<K, V> map = new HashMap<>();
        for (Map.Entry<K, V> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }
        return new GenericMapData(map);
    }

    @SuppressWarnings({"rawtypes"})
    private static class CastTestSpec {
        private final DataType inputType;
        private final DataType targetType;
        private final Consumer<CastExecutor> assertionExecutor;
        private final String description;
        private final CastRule.Context castContext;

        public CastTestSpec(
                DataType inputType,
                DataType targetType,
                Consumer<CastExecutor> assertionExecutor,
                String description,
                CastRule.Context castContext) {
            this.inputType = inputType;
            this.targetType = targetType;
            this.assertionExecutor = assertionExecutor;
            this.description = description;
            this.castContext = castContext;
        }

        public void run() throws Exception {
            CastExecutor executor =
                    CastRuleProvider.create(
                            this.castContext,
                            this.inputType.getLogicalType(),
                            this.targetType.getLogicalType());
            assertNotNull(
                    executor,
                    "Cannot resolve an executor for input "
                            + this.inputType
                            + " and target "
                            + this.targetType);

            this.assertionExecutor.accept(executor);
        }

        @Override
        public String toString() {
            return inputType + " => " + targetType + " " + description;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static class CastTestSpecBuilder {
        private DataType targetType;
        private final List<DataType> inputTypes = new ArrayList<>();
        private final List<Consumer<CastExecutor>> assertionExecutors = new ArrayList<>();
        private final List<String> descriptions = new ArrayList<>();
        private final List<CastRule.Context> castContexts = new ArrayList<>();

        private static CastTestSpecBuilder testCastTo(DataType targetType) {
            CastTestSpecBuilder tsb = new CastTestSpecBuilder();
            tsb.targetType = targetType;
            return tsb;
        }

        private CastTestSpecBuilder fromCase(DataType dataType, Object src, Object target) {
            return fromCase(dataType, src, target, false);
        }

        private CastTestSpecBuilder fromCase(
                DataType dataType, Object src, Object target, boolean legacyBehaviour) {
            return fromCase(
                    dataType,
                    CastRule.Context.create(
                            legacyBehaviour,
                            DateTimeUtils.UTC_ZONE.toZoneId(),
                            Thread.currentThread().getContextClassLoader()),
                    src,
                    target);
        }

        private CastTestSpecBuilder fromCase(
                DataType dataType, CastRule.Context castContext, Object src, Object target) {
            this.inputTypes.add(dataType);
            this.assertionExecutors.add(
                    executor -> {
                        if (target instanceof byte[]) {
                            assertArrayEquals((byte[]) target, (byte[]) executor.cast(src));
                            // Run twice to make sure rules are reusable without causing issues
                            assertArrayEquals(
                                    (byte[]) target,
                                    (byte[]) executor.cast(src),
                                    "Error when reusing the rule. Perhaps there is some state that needs to be reset");
                        } else {
                            assertEquals(target, executor.cast(src));
                            // Run twice to make sure rules are reusable without causing issues
                            assertEquals(
                                    target,
                                    executor.cast(src),
                                    "Error when reusing the rule. Perhaps there is some state that needs to be reset");
                        }
                    });
            this.descriptions.add("{" + src + " => " + target + "}");
            this.castContexts.add(castContext);
            return this;
        }

        private CastTestSpecBuilder fail(
                DataType dataType, Object src, Class<? extends Throwable> exception) {
            return fail(
                    dataType,
                    CastRule.Context.create(
                            false,
                            DateTimeUtils.UTC_ZONE.toZoneId(),
                            Thread.currentThread().getContextClassLoader()),
                    src,
                    exception);
        }

        private CastTestSpecBuilder fail(
                DataType dataType,
                CastRule.Context castContext,
                Object src,
                Class<? extends Throwable> exception) {
            this.inputTypes.add(dataType);
            this.assertionExecutors.add(
                    executor -> assertThrows(exception, () -> executor.cast(src)));
            this.descriptions.add("{" + src + " => " + exception.getName() + "}");
            this.castContexts.add(castContext);
            return this;
        }

        private Stream<CastTestSpec> toSpecs() {
            CastTestSpec[] testSpecs = new CastTestSpec[assertionExecutors.size()];
            for (int i = 0; i < assertionExecutors.size(); i++) {
                testSpecs[i] =
                        new CastTestSpec(
                                inputTypes.get(i),
                                targetType,
                                assertionExecutors.get(i),
                                descriptions.get(i),
                                castContexts.get(i));
            }
            return Arrays.stream(testSpecs);
        }
    }
}

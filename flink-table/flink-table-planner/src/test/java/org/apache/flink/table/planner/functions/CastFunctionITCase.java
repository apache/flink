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

package org.apache.flink.table.planner.functions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.util.CollectionUtil.entry;
import static org.apache.flink.util.CollectionUtil.map;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BuiltInFunctionDefinitions#CAST}. */
public class CastFunctionITCase extends BuiltInFunctionTestBase {

    private static final ZoneId TEST_TZ = ZoneId.of("Asia/Shanghai");
    private static final ZoneOffset TEST_OFFSET = ZoneOffset.ofHoursMinutes(-1, -20);

    private static final byte[] DEFAULT_BINARY = new byte[] {0, 1};
    private static final byte[] DEFAULT_VARBINARY = new byte[] {0, 1, 2};
    private static final byte[] DEFAULT_BYTES = new byte[] {0, 1, 2, 3, 4};

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

    private static final LocalDate DEFAULT_DATE = LocalDate.parse("2021-09-24");
    private static final LocalTime DEFAULT_TIME = LocalTime.parse("12:34:56.123");
    private static final LocalDateTime DEFAULT_TIMESTAMP =
            LocalDateTime.parse("2021-09-24T12:34:56.1234567");
    private static final Instant DEFAULT_TIMESTAMP_LTZ = fromLocalTZ("2021-09-24T22:34:56.1234567");

    private static final Period DEFAULT_INTERVAL_YEAR = Period.of(10, 4, 0);
    private static final Duration DEFAULT_INTERVAL_DAY = Duration.ofHours(12);

    private static final int[] DEFAULT_ARRAY = new int[] {0, 1, 2};

    @Override
    Configuration getConfiguration() {
        return super.getConfiguration().set(TableConfigOptions.LOCAL_TIME_ZONE, TEST_TZ.getId());
    }

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        final List<TestSetSpec> specs = new ArrayList<>();
        specs.addAll(allTypesBasic());
        specs.addAll(toStringCasts());
        specs.addAll(decimalCasts());
        specs.addAll(numericBounds());
        specs.addAll(constructedTypes());
        return specs.stream();
    }

    private static List<TestSetSpec> allTypesBasic() {
        return Arrays.asList(
                CastTestSpecBuilder.testCastTo(BOOLEAN())
                        .fromCase(BOOLEAN(), null, null)
                        .failRuntime(CHAR(3), "foo", TableException.class)
                        .fromCase(CHAR(4), "true", true)
                        .fromCase(VARCHAR(5), "FalsE", false)
                        .failRuntime(STRING(), "Apache Flink", TableException.class)
                        .fromCase(STRING(), "TRUE", true)
                        .failRuntime(STRING(), "", TableException.class)
                        .fromCase(BOOLEAN(), true, true)
                        .fromCase(BOOLEAN(), false, false)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        // https://issues.apache.org/jira/browse/FLINK-24576 should also fail for
                        // SQL
                        .failTableApiValidation(DECIMAL(4, 3), 4.3)
                        .fromCase(TINYINT(), DEFAULT_POSITIVE_TINY_INT, true)
                        .fromCase(TINYINT(), DEFAULT_NEGATIVE_TINY_INT, true)
                        .fromCase(TINYINT(), 0, false)
                        .fromCase(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT, true)
                        .fromCase(SMALLINT(), DEFAULT_NEGATIVE_SMALL_INT, true)
                        .fromCase(SMALLINT(), 0, false)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, true)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, true)
                        .fromCase(INT(), 0, false)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, true)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, true)
                        .fromCase(BIGINT(), 0, false)
                        // https://issues.apache.org/jira/browse/FLINK-24576 should also fail for
                        // SQL
                        .failTableApiValidation(FLOAT(), -123.456)
                        .failTableApiValidation(DOUBLE(), 0)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(BINARY(2))
                        .fromCase(BINARY(5), null, null)
                        .fromCase(CHAR(4), "666F", new byte[] {54, 54})
                        .fromCase(VARCHAR(8), "666f", new byte[] {54, 54})
                        .fromCase(STRING(), "a", new byte[] {97, 0})
                        .fromCase(VARCHAR(4), "FC", new byte[] {70, 67})
                        .fromCase(STRING(), "foobar", new byte[] {102, 111})
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        //
                        .fromCase(BINARY(2), DEFAULT_BINARY, DEFAULT_BINARY)
                        .fromCase(VARBINARY(3), DEFAULT_VARBINARY, new byte[] {0, 1})
                        .fromCase(BYTES(), DEFAULT_BYTES, new byte[] {0, 1})
                        .fromCase(BINARY(1), new byte[] {111}, new byte[] {111, 0})
                        .fromCase(VARBINARY(1), new byte[] {111}, new byte[] {111, 0})
                        .fromCase(BYTES(), new byte[] {11}, new byte[] {11, 0})
                        // Not supported - no fix
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_NEGATIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // RAW supported - check CastFunctionMiscITCase
                        .build(),
                CastTestSpecBuilder.testCastTo(VARBINARY(4))
                        .fromCase(VARBINARY(5), null, null)
                        .fromCase(CHAR(4), "foo", new byte[] {102, 111, 111, 32})
                        .fromCase(VARCHAR(8), "foobar", new byte[] {102, 111, 111, 98})
                        .fromCase(STRING(), "AAbbCcDdEe", new byte[] {65, 65, 98, 98})
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        //
                        .fromCase(BINARY(2), DEFAULT_BINARY, DEFAULT_BINARY)
                        .fromCase(VARBINARY(3), DEFAULT_VARBINARY, DEFAULT_VARBINARY)
                        .fromCase(VARBINARY(10), DEFAULT_VARBINARY, DEFAULT_VARBINARY)
                        .fromCase(BYTES(), DEFAULT_BYTES, new byte[] {0, 1, 2, 3})
                        // Not supported - no fix
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_NEGATIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // RAW supported - check CastFunctionMiscITCase
                        .build(),
                CastTestSpecBuilder.testCastTo(BYTES())
                        .fromCase(BYTES(), null, null)
                        .fromCase(CHAR(4), "foo", new byte[] {102, 111, 111, 32})
                        .fromCase(VARCHAR(8), "foobar", new byte[] {102, 111, 111, 98, 97, 114})
                        .fromCase(
                                STRING(),
                                "Apache Flink",
                                new byte[] {65, 112, 97, 99, 104, 101, 32, 70, 108, 105, 110, 107})
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        //
                        .fromCase(BINARY(2), DEFAULT_BINARY, DEFAULT_BINARY)
                        .fromCase(VARBINARY(3), DEFAULT_VARBINARY, DEFAULT_VARBINARY)
                        .fromCase(BYTES(), DEFAULT_BYTES, DEFAULT_BYTES)
                        // Not supported - no fix
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_NEGATIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // RAW supported - check CastFunctionMiscITCase
                        .build(),
                CastTestSpecBuilder.testCastTo(DECIMAL(5, 3))
                        .fromCase(DECIMAL(10, 2), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", new BigDecimal("1.234"))
                        .fromCase(STRING(), "1.2", new BigDecimal("1.200"))
                        .fromCase(BOOLEAN(), true, new BigDecimal("1.000"))
                        .fromCase(BOOLEAN(), false, new BigDecimal("0.000"))
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, new BigDecimal("9.870"))
                        .fromCase(TINYINT(), -1, new BigDecimal("-1.000"))
                        .fromCase(SMALLINT(), 3, new BigDecimal("3.000"))
                        .fromCase(INT(), 42, new BigDecimal("42.000"))
                        .fromCase(BIGINT(), 8, new BigDecimal("8.000"))
                        .fromCase(FLOAT(), -12.345, new BigDecimal("-12.345"))
                        .fromCase(DOUBLE(), 12.678, new BigDecimal("12.678"))
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TINYINT())
                        .fromCase(TINYINT(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", (byte) 1)
                        .fromCase(STRING(), "123", (byte) 123)
                        .failRuntime(STRING(), "-130", NumberFormatException.class)
                        .fromCase(BOOLEAN(), true, (byte) 1)
                        .fromCase(BOOLEAN(), false, (byte) 0)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, (byte) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(10, 3), 9123.87, (byte) -93)
                        .fromCase(TINYINT(), DEFAULT_POSITIVE_TINY_INT, DEFAULT_POSITIVE_TINY_INT)
                        .fromCase(TINYINT(), DEFAULT_NEGATIVE_TINY_INT, DEFAULT_NEGATIVE_TINY_INT)
                        .fromCase(SMALLINT(), 32, (byte) 32)
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
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(SMALLINT())
                        .fromCase(SMALLINT(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", (short) 1)
                        .fromCase(STRING(), "123", (short) 123)
                        .failRuntime(STRING(), "-32769", NumberFormatException.class)
                        .fromCase(BOOLEAN(), true, (short) 1)
                        .fromCase(BOOLEAN(), false, (short) 0)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, (short) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(10, 3), 91235.87, (short) 25699)
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
                        .fromCase(SMALLINT(), 32780, (short) -32756)
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, (short) -10617)
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, (short) 10617)
                        .fromCase(INT(), -12, (short) -12)
                        .fromCase(BIGINT(), 123, (short) 123)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, (short) 7221)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, (short) -7221)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, (short) 123)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, (short) -123)
                        .fromCase(FLOAT(), 123456.78, (short) -7616)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, (short) 123)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, (short) -123)
                        .fromCase(DOUBLE(), 123456.7890, (short) -7616)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(INT())
                        .fromCase(INT(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", 1)
                        .fromCase(STRING(), "123", 123)
                        .failRuntime(STRING(), "-3276913443134", NumberFormatException.class)
                        .fromCase(BOOLEAN(), true, 1)
                        .fromCase(BOOLEAN(), false, 0)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, -146603714)
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
                        .fromCase(BIGINT(), 123, 123)
                        .fromCase(BIGINT(), DEFAULT_POSITIVE_BIGINT, -539222987)
                        .fromCase(BIGINT(), DEFAULT_NEGATIVE_BIGINT, 539222987)
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, 123)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, -123)
                        .fromCase(FLOAT(), 9234567891.12f, 2147483647)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123)
                        .fromCase(DOUBLE(), 9234567891.12345, 2147483647)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(BIGINT())
                        .fromCase(BIGINT(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", 1L)
                        .fromCase(STRING(), "123", 123L)
                        .fromCase(STRING(), "-3276913443134", -3276913443134L)
                        .fromCase(BOOLEAN(), true, 1L)
                        .fromCase(BOOLEAN(), false, 0L)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, 9L)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3276913443134L)
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
                        .fromCase(FLOAT(), 9234567891.12f, (long) 9234567891.12f)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123L)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123L)
                        .fromCase(DOUBLE(), 9234567891.12345, 9234567891L)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(FLOAT())
                        .fromCase(FLOAT(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", 1.234f)
                        .fromCase(STRING(), "123", 123.0f)
                        .fromCase(STRING(), "-3276913443134", -3.27691403E12f)
                        .fromCase(BOOLEAN(), true, 1.0f)
                        .fromCase(BOOLEAN(), false, 0.0f)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87f)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87f)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3.27691351E12f)
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
                        .fromCase(FLOAT(), 9234567891.12, 9234567891.12f)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, 123.456789f)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, -123.456789f)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234, 1.23923451E12f)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(DOUBLE())
                        .fromCase(DOUBLE(), null, null)
                        .failRuntime(CHAR(3), "foo", NumberFormatException.class)
                        .failRuntime(VARCHAR(5), "Flink", NumberFormatException.class)
                        .failRuntime(STRING(), "Apache", NumberFormatException.class)
                        .fromCase(STRING(), "1.234", 1.234d)
                        .fromCase(STRING(), "123", 123.0d)
                        .fromCase(STRING(), "-3276913443134", -3.276913443134E12)
                        .fromCase(BOOLEAN(), true, 1.0d)
                        .fromCase(BOOLEAN(), false, 0.0d)
                        // Not supported - no fix
                        .failValidation(BINARY(2), DEFAULT_BINARY)
                        .failValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failValidation(BYTES(), DEFAULT_BYTES)
                        //
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87d)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3.27691344313487E12d)
                        .fromCase(
                                DECIMAL(30, 20),
                                new BigDecimal("123456789.123456789123456789"),
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
                        .fromCase(FLOAT(), DEFAULT_POSITIVE_FLOAT, 123.456d)
                        .fromCase(FLOAT(), DEFAULT_NEGATIVE_FLOAT, -123.456)
                        .fromCase(FLOAT(), 9234567891.12, 9234567891.12d)
                        .fromCase(DOUBLE(), DEFAULT_POSITIVE_DOUBLE, DEFAULT_POSITIVE_DOUBLE)
                        .fromCase(DOUBLE(), DEFAULT_NEGATIVE_DOUBLE, DEFAULT_NEGATIVE_DOUBLE)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234, 1.2392345678911235E12d)
                        // Not supported - no fix
                        .failValidation(DATE(), DEFAULT_DATE)
                        .failValidation(TIME(), DEFAULT_TIME)
                        .failValidation(TIMESTAMP(), DEFAULT_TIMESTAMP)
                        // TIMESTAMP_WITH_TIME_ZONE
                        .failValidation(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP_LTZ)
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(DATE())
                        .fromCase(DATE(), null, null)
                        .failRuntime(CHAR(3), "foo", DateTimeException.class)
                        .failRuntime(VARCHAR(5), "Flink", DateTimeException.class)
                        .fromCase(STRING(), "123", LocalDate.of(123, 1, 1))
                        .fromCase(STRING(), "2021-09-27", LocalDate.of(2021, 9, 27))
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                LocalDate.of(2021, 9, 27))
                        .failRuntime(STRING(), "2021/09/27", DateTimeException.class)
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        .failTableApiValidation(BINARY(2), DEFAULT_BINARY)
                        .failTableApiValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failTableApiValidation(BYTES(), DEFAULT_BYTES)
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_POSITIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        //
                        .fromCase(DATE(), DEFAULT_DATE, DEFAULT_DATE)
                        // Not supported - no fix
                        .failValidation(TIME(), DEFAULT_TIME)
                        //
                        .fromCase(TIMESTAMP(), DEFAULT_TIMESTAMP, LocalDate.of(2021, 9, 24))
                        .fromCase(TIMESTAMP(4), DEFAULT_TIMESTAMP, LocalDate.of(2021, 9, 24))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP, LocalDate.of(2021, 9, 24))
                        .fromCase(TIMESTAMP_LTZ(), DEFAULT_TIMESTAMP, LocalDate.of(2021, 9, 24))
                        // Not supported - no fix
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIME())
                        .fromCase(TIME(), null, null)
                        .failRuntime(CHAR(3), "foo", DateTimeException.class)
                        .failRuntime(VARCHAR(5), "Flink", DateTimeException.class)
                        .failRuntime(STRING(), "Flink", DateTimeException.class)
                        .fromCase(STRING(), "123", LocalTime.of(23, 0, 0))
                        .fromCase(STRING(), "123:45", LocalTime.of(23, 45, 0))
                        .failRuntime(STRING(), "2021-09-27", DateTimeException.class)
                        .failRuntime(STRING(), "2021-09-27 12:34:56", DateTimeException.class)
                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        .fromCase(STRING(), "12:34:56.123456789", LocalTime.of(12, 34, 56, 0))
                        .failRuntime(
                                STRING(), "2021-09-27 12:34:56.123456789", DateTimeException.class)
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        .failTableApiValidation(BINARY(2), DEFAULT_BINARY)
                        .failTableApiValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failTableApiValidation(BYTES(), DEFAULT_BYTES)
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_POSITIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        .failValidation(DATE(), DEFAULT_DATE)
                        //
                        .fromCase(TIME(5), DEFAULT_TIME, LocalTime.of(12, 34, 56, 0))
                        .fromCase(TIMESTAMP(), DEFAULT_TIMESTAMP, LocalTime.of(12, 34, 56, 0))
                        .fromCase(TIMESTAMP(4), DEFAULT_TIMESTAMP, LocalTime.of(12, 34, 56, 0))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(TIMESTAMP_LTZ(4), DEFAULT_TIMESTAMP, LocalTime.of(12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP_LTZ(4), DEFAULT_TIMESTAMP_LTZ, LocalTime.of(7, 54, 56, 0))
                        // Not supported - no fix
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIMESTAMP(9))
                        .fromCase(TIMESTAMP(), null, null)
                        .failRuntime(CHAR(3), "foo", DateTimeException.class)
                        .failRuntime(VARCHAR(5), "Flink", DateTimeException.class)
                        .failRuntime(STRING(), "123", DateTimeException.class)
                        .fromCase(STRING(), "2021-09-27", LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0))
                        .failRuntime(STRING(), "2021/09/27", DateTimeException.class)
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123456789))
                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        .failTableApiValidation(BINARY(2), DEFAULT_BINARY)
                        .failTableApiValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failTableApiValidation(BYTES(), DEFAULT_BYTES)
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_POSITIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        //
                        .fromCase(DATE(), DEFAULT_DATE, LocalDateTime.of(2021, 9, 24, 0, 0, 0, 0))

                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        // https://issues.apache.org/jira/browse/FLINK-24423 Continue using EPOCH
                        // date or use 0 for the year?
                        .fromCase(
                                TIME(5), DEFAULT_TIME, LocalDateTime.of(1970, 1, 1, 12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP(),
                                DEFAULT_TIMESTAMP,
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456000))
                        .fromCase(
                                TIMESTAMP(8),
                                DEFAULT_TIMESTAMP,
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456700))
                        .fromCase(
                                TIMESTAMP(4),
                                DEFAULT_TIMESTAMP,
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                DEFAULT_TIMESTAMP,
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000))
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                DEFAULT_TIMESTAMP_LTZ,
                                LocalDateTime.of(2021, 9, 25, 7, 54, 56, 123400000))
                        // Not supported - no fix
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIMESTAMP_LTZ(9))
                        .fromCase(TIMESTAMP_LTZ(), null, null)
                        .failRuntime(CHAR(3), "foo", DateTimeParseException.class)
                        .failRuntime(VARCHAR(5), "Flink", DateTimeParseException.class)
                        .failRuntime(STRING(), "Apache", DateTimeParseException.class)
                        .fromCase(
                                STRING(),
                                "2021-09-27",
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0)))
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123",
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123000000)))
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123456789)))

                        // Not supported - no fix
                        .failValidation(BOOLEAN(), true)
                        .failTableApiValidation(BINARY(2), DEFAULT_BINARY)
                        .failTableApiValidation(VARBINARY(5), DEFAULT_VARBINARY)
                        .failTableApiValidation(BYTES(), DEFAULT_BYTES)
                        .failValidation(DECIMAL(5, 3), 12.345)
                        .failValidation(TINYINT(), DEFAULT_POSITIVE_TINY_INT)
                        .failValidation(SMALLINT(), DEFAULT_POSITIVE_SMALL_INT)
                        .failValidation(INT(), DEFAULT_POSITIVE_INT)
                        .failValidation(BIGINT(), DEFAULT_POSITIVE_BIGINT)
                        .failValidation(FLOAT(), DEFAULT_POSITIVE_FLOAT)
                        .failValidation(DOUBLE(), DEFAULT_POSITIVE_DOUBLE)
                        //
                        .fromCase(
                                DATE(),
                                DEFAULT_DATE,
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 24, 0, 0, 0, 0)))

                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        // https://issues.apache.org/jira/browse/FLINK-24423 Continue using EPOCH
                        // date or use 0 for the year?
                        .fromCase(
                                TIME(5),
                                DEFAULT_TIME,
                                fromLocalToUTC(LocalDateTime.of(1970, 1, 1, 12, 34, 56, 0)))
                        .fromCase(
                                TIMESTAMP(),
                                DEFAULT_TIMESTAMP,
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456000)))
                        .fromCase(
                                TIMESTAMP(8),
                                DEFAULT_TIMESTAMP,
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456700)))
                        .fromCase(
                                TIMESTAMP(4),
                                DEFAULT_TIMESTAMP,
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000)))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                DEFAULT_TIMESTAMP,
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000)))
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                DEFAULT_TIMESTAMP_LTZ,
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 25, 7, 54, 56, 123400000)))
                        // Not supported - no fix
                        .failValidation(INTERVAL(YEAR(), MONTH()), DEFAULT_INTERVAL_YEAR)
                        .failValidation(INTERVAL(DAY(), SECOND()), DEFAULT_INTERVAL_DAY)
                        .failValidation(ARRAY(INT()), DEFAULT_ARRAY)
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                // CastTestSpecBuilder
                // .testCastTo(INTERVAL(YEAR()))
                // https://issues.apache.org/jira/browse/FLINK-24426 allow cast from string
                // .fromCase(CHAR(3), "foo", null)
                // .fromCase(VARCHAR(5), "Flink", null)
                // .fromCase(STRING(), "123", null)
                // .fromCase(STRING(), "INTERVAL '2 YEARS'", Period.of(2, 0, 0))
                // .fromCase(STRING(), "+01-05", Period.of(1, 5, 0))
                //
                // https://issues.apache.org/jira/browse/FLINK-24428
                // .fromCase(INTERVAL(YEAR()), 0, Period.of(0, 0, 0))
                // .fromCase(INTERVAL(YEAR()), 11, Period.of(0, 0, 0))
                // .fromCase(INTERVAL(YEAR()), 84, Period.of(7, 0, 0))
                // .fromCase(INTERVAL(YEAR()), 89, Period.of(7, 0, 0))
                // .fromCase(INTERVAL(MONTH()), 89, Period.of(7, 0, 0))
                // .fromCase(INTERVAL(DAY()), Duration.ofDays(300), Period.of(0, 0, 0))
                // .fromCase(INTERVAL(DAY()), Duration.ofDays(400), Period.of(1, 0, 0))
                // .fromCase(INTERVAL(HOUR()), Duration.ofDays(400), Period.of(1, 0, 0))
                // .build(),
                CastTestSpecBuilder
                        // https://issues.apache.org/jira/browse/FLINK-24426 allow cast from string
                        // .fromCase(STRING(), "'+01-05'".resultsIn(Period.of(0, 17, 0)
                        .testCastTo(INTERVAL(MONTH()))
                        .fromCase(INTERVAL(MONTH()), null, null)
                        .fromCase(INTERVAL(YEAR()), 0, Period.of(0, 0, 0))
                        .fromCase(INTERVAL(YEAR()), 11, Period.of(0, 11, 0))
                        .fromCase(INTERVAL(YEAR()), 84, Period.of(0, 84, 0))
                        .fromCase(INTERVAL(YEAR()), 89, Period.of(0, 89, 0))
                        .fromCase(INTERVAL(MONTH()), 89, Period.of(0, 89, 0))
                        // https://issues.apache.org/jira/browse/FLINK-24428
                        // .fromCase(INTERVAL(DAY()), Duration.ofDays(300), Period.of(0, 0, 0))
                        // .fromCase(INTERVAL(DAY()), Duration.ofDays(400), Period.of(1, 0, 0))
                        // .fromCase(INTERVAL(HOUR()), Duration.ofDays(400), Period.of(1, 0, 0))
                        .build()
                // CastTestSpecBuilder
                // .testCastTo(INTERVAL(DAY()))
                // https://issues.apache.org/jira/browse/FLINK-24426 allow cast from string
                // .fromCase(STRING(), "+41 10:17:36.789", Duration.of(...))
                // https://issues.apache.org/jira/browse/FLINK-24428
                // .build()
                //
                );
    }

    private static List<TestSetSpec> toStringCasts() {
        return Arrays.asList(
                CastTestSpecBuilder.testCastTo(CHAR(3))
                        .fromCase(CHAR(5), null, null)
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(VARCHAR(3), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo", "foo")
                        .fromCase(STRING(), "abcdef", "abc")
                        .fromCase(DATE(), DEFAULT_DATE, "202")
                        .build(),
                CastTestSpecBuilder.testCastTo(CHAR(5))
                        .fromCase(CHAR(5), null, null)
                        .fromCase(CHAR(3), "foo", "foo  ")
                        .build(),
                CastTestSpecBuilder.testCastTo(VARCHAR(3))
                        .fromCase(VARCHAR(5), null, null)
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(CHAR(4), "foo", "foo")
                        .fromCase(VARCHAR(3), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo", "foo")
                        .fromCase(STRING(), "abcdef", "abc")
                        .build(),
                CastTestSpecBuilder.testCastTo(STRING())
                        .fromCase(STRING(), null, null)
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(CHAR(5), "foo", "foo  ")
                        .fromCase(VARCHAR(5), "Flink", "Flink")
                        .fromCase(VARCHAR(10), "Flink", "Flink")
                        .fromCase(STRING(), "Apache Flink", "Apache Flink")
                        .fromCase(STRING(), null, null)
                        .fromCase(BOOLEAN(), true, "TRUE")
                        .fromCase(BINARY(2), DEFAULT_BINARY, "\u0000\u0001")
                        .fromCase(BINARY(3), DEFAULT_BINARY, "\u0000\u0001\u0000")
                        .fromCase(VARBINARY(3), DEFAULT_VARBINARY, "\u0000\u0001\u0002")
                        .fromCase(VARBINARY(5), DEFAULT_VARBINARY, "\u0000\u0001\u0002")
                        .fromCase(BYTES(), DEFAULT_BYTES, "\u0000\u0001\u0002\u0003\u0004")
                        .fromCase(DECIMAL(4, 3), 9.87, "9.870")
                        .fromCase(DECIMAL(10, 5), 1, "1.00000")
                        .fromCase(
                                TINYINT(),
                                DEFAULT_POSITIVE_TINY_INT,
                                String.valueOf(DEFAULT_POSITIVE_TINY_INT))
                        .fromCase(
                                TINYINT(),
                                DEFAULT_NEGATIVE_TINY_INT,
                                String.valueOf(DEFAULT_NEGATIVE_TINY_INT))
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_POSITIVE_SMALL_INT,
                                String.valueOf(DEFAULT_POSITIVE_SMALL_INT))
                        .fromCase(
                                SMALLINT(),
                                DEFAULT_NEGATIVE_SMALL_INT,
                                String.valueOf(DEFAULT_NEGATIVE_SMALL_INT))
                        .fromCase(INT(), DEFAULT_POSITIVE_INT, String.valueOf(DEFAULT_POSITIVE_INT))
                        .fromCase(INT(), DEFAULT_NEGATIVE_INT, String.valueOf(DEFAULT_NEGATIVE_INT))
                        .fromCase(
                                BIGINT(),
                                DEFAULT_POSITIVE_BIGINT,
                                String.valueOf(DEFAULT_POSITIVE_BIGINT))
                        .fromCase(
                                BIGINT(),
                                DEFAULT_NEGATIVE_BIGINT,
                                String.valueOf(DEFAULT_NEGATIVE_BIGINT))
                        .fromCase(
                                FLOAT(),
                                DEFAULT_POSITIVE_FLOAT,
                                String.valueOf(DEFAULT_POSITIVE_FLOAT))
                        .fromCase(
                                FLOAT(),
                                DEFAULT_NEGATIVE_FLOAT,
                                String.valueOf(DEFAULT_NEGATIVE_FLOAT))
                        .fromCase(
                                DOUBLE(),
                                DEFAULT_POSITIVE_DOUBLE,
                                String.valueOf(DEFAULT_POSITIVE_DOUBLE))
                        .fromCase(
                                DOUBLE(),
                                DEFAULT_NEGATIVE_DOUBLE,
                                String.valueOf(DEFAULT_NEGATIVE_DOUBLE))
                        .fromCase(DATE(), DEFAULT_DATE, "2021-09-24")
                        // https://issues.apache.org/jira/browse/FLINK-17224 Currently, fractional
                        // seconds are lost
                        .fromCase(TIME(5), DEFAULT_TIME, "12:34:56")
                        .fromCase(TIMESTAMP(), DEFAULT_TIMESTAMP, "2021-09-24 12:34:56.123456")
                        .fromCase(TIMESTAMP(9), DEFAULT_TIMESTAMP, "2021-09-24 12:34:56.123456700")
                        .fromCase(TIMESTAMP(4), DEFAULT_TIMESTAMP, "2021-09-24 12:34:56.1234")
                        .fromCase(
                                TIMESTAMP(3),
                                LocalDateTime.parse("2021-09-24T12:34:56.1"),
                                "2021-09-24 12:34:56.100")
                        .fromCase(TIMESTAMP(4).nullable(), null, null)

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        .fromCase(
                                TIMESTAMP_LTZ(5),
                                DEFAULT_TIMESTAMP_LTZ,
                                "2021-09-25 07:54:56.12345")
                        .fromCase(
                                TIMESTAMP_LTZ(9),
                                DEFAULT_TIMESTAMP_LTZ,
                                "2021-09-25 07:54:56.123456700")
                        .fromCase(
                                TIMESTAMP_LTZ(3),
                                fromLocalTZ("2021-09-24T22:34:56.1"),
                                "2021-09-25 07:54:56.100")
                        .fromCase(INTERVAL(YEAR()), 84, "+7-00")
                        .fromCase(INTERVAL(MONTH()), 5, "+0-05")
                        .fromCase(INTERVAL(MONTH()), 123, "+10-03")
                        .fromCase(INTERVAL(MONTH()), 12334, "+1027-10")
                        .fromCase(INTERVAL(DAY()), 10, "+0 00:00:00.010")
                        .fromCase(INTERVAL(DAY()), 123456789L, "+1 10:17:36.789")
                        .fromCase(INTERVAL(DAY()), Duration.ofHours(36), "+1 12:00:00.000")
                        .fromCase(ARRAY(INT().nullable()), new Integer[] {null, 456}, "[NULL, 456]")
                        .fromCase(
                                MAP(STRING(), INTERVAL(MONTH())),
                                map(entry("a", -123)),
                                "{a=-10-03}")
                        .fromCase(
                                ROW(FIELD("f0", INT().nullable()), FIELD("f1", STRING())),
                                Row.of(null, "abc"),
                                "(NULL, abc)")
                        // MULTISET, RAW and STRUCTURED are tested in CastFunctionMiscITCase,
                        // because we need to work around the limitations of fromValues
                        .build());
    }

    private static List<TestSetSpec> decimalCasts() {
        return Collections.singletonList(
                CastTestSpecBuilder.testCastTo(DECIMAL(8, 4))
                        .fromCase(STRING(), null, null)
                        // rounding
                        .fromCase(DOUBLE(), 3.123456, new BigDecimal("3.1235"))
                        .fromCase(DECIMAL(10, 8), 12.34561234, new BigDecimal("12.3456"))
                        // out of precision/scale bounds
                        // Should these fail? https://issues.apache.org/jira/browse/FLINK-24847
                        .fromCase(INT(), 12345, null)
                        .fromCase(FLOAT(), 12345.678912, null)
                        .failRuntime(STRING(), "12345.6789", NumberFormatException.class)
                        .build());
    }

    @SuppressWarnings("NumericOverflow")
    private static List<TestSetSpec> numericBounds() {
        return Arrays.asList(
                CastTestSpecBuilder.testCastTo(TINYINT())
                        .fromCase(TINYINT(), Byte.MIN_VALUE, Byte.MIN_VALUE)
                        .fromCase(TINYINT(), Byte.MAX_VALUE, Byte.MAX_VALUE)
                        .fromCase(TINYINT(), (byte) (Byte.MIN_VALUE - 1), Byte.MAX_VALUE)
                        .fromCase(TINYINT(), (byte) (Byte.MAX_VALUE + 1), Byte.MIN_VALUE)
                        .build(),
                CastTestSpecBuilder.testCastTo(SMALLINT())
                        .fromCase(SMALLINT(), Short.MIN_VALUE, Short.MIN_VALUE)
                        .fromCase(SMALLINT(), Short.MAX_VALUE, Short.MAX_VALUE)
                        .fromCase(SMALLINT(), (short) (Short.MIN_VALUE - 1), Short.MAX_VALUE)
                        .fromCase(SMALLINT(), (short) (Short.MAX_VALUE + 1), Short.MIN_VALUE)
                        .build(),
                CastTestSpecBuilder.testCastTo(INT())
                        .fromCase(INT(), Integer.MIN_VALUE, Integer.MIN_VALUE)
                        .fromCase(INT(), Integer.MAX_VALUE, Integer.MAX_VALUE)
                        .fromCase(INT(), Integer.MIN_VALUE - 1, Integer.MAX_VALUE)
                        .fromCase(INT(), Integer.MAX_VALUE + 1, Integer.MIN_VALUE)
                        .build(),
                CastTestSpecBuilder.testCastTo(BIGINT())
                        .fromCase(BIGINT(), Long.MIN_VALUE, Long.MIN_VALUE)
                        .fromCase(BIGINT(), Long.MAX_VALUE, Long.MAX_VALUE)
                        .fromCase(BIGINT(), Long.MIN_VALUE - 1, Long.MAX_VALUE)
                        .fromCase(BIGINT(), Long.MAX_VALUE + 1, Long.MIN_VALUE)
                        .build(),
                CastTestSpecBuilder.testCastTo(FLOAT())
                        .fromCase(DOUBLE(), -1.7976931348623157E308d, Float.NEGATIVE_INFINITY)
                        .build(),
                CastTestSpecBuilder.testCastTo(DECIMAL(38, 0))
                        .fromCase(
                                TINYINT(),
                                (byte) (Byte.MIN_VALUE - 1),
                                BigDecimal.valueOf(Byte.MAX_VALUE))
                        .fromCase(
                                TINYINT(),
                                (byte) (Byte.MAX_VALUE + 1),
                                BigDecimal.valueOf(Byte.MIN_VALUE))
                        .fromCase(
                                SMALLINT(),
                                (short) (Short.MIN_VALUE - 1),
                                BigDecimal.valueOf(Short.MAX_VALUE))
                        .fromCase(
                                SMALLINT(),
                                (short) (Short.MAX_VALUE + 1),
                                BigDecimal.valueOf(Short.MIN_VALUE))
                        .fromCase(
                                INT(), Integer.MIN_VALUE - 1, BigDecimal.valueOf(Integer.MAX_VALUE))
                        .fromCase(
                                INT(), Integer.MAX_VALUE + 1, BigDecimal.valueOf(Integer.MIN_VALUE))
                        .fromCase(BIGINT(), Long.MIN_VALUE - 1, BigDecimal.valueOf(Long.MAX_VALUE))
                        .fromCase(BIGINT(), Long.MAX_VALUE + 1, BigDecimal.valueOf(Long.MIN_VALUE))
                        .build(),
                CastTestSpecBuilder.testCastTo(DECIMAL(38, 32))
                        .fromCase(FLOAT(), -Float.MAX_VALUE, null)
                        .fromCase(FLOAT(), Float.MAX_VALUE, null)
                        .fromCase(DOUBLE(), -Double.MAX_VALUE, null)
                        .fromCase(DOUBLE(), Double.MAX_VALUE, null)
                        .build());
    }

    private static List<TestSetSpec> constructedTypes() {
        return Arrays.asList(
                CastTestSpecBuilder.testCastTo(MAP(STRING(), STRING()))
                        .fromCase(MAP(FLOAT(), DOUBLE()), null, null)
                        .fromCase(
                                MAP(INT(), INT()),
                                Collections.singletonMap(1, 2),
                                Collections.singletonMap("1", "2"))
                        .build(),
                // https://issues.apache.org/jira/browse/FLINK-25567
                // CastTestSpecBuilder.testCastTo(MULTISET(STRING()))
                //        .fromCase(MULTISET(TIMESTAMP()), null, null)
                //        .fromCase(
                //                MULTISET(INT()),
                //                map(entry(1, 2), entry(3, 4)),
                //                map(entry("1", 2), entry("3", 4)))
                //        .build(),
                CastTestSpecBuilder.testCastTo(ARRAY(INT()))
                        .fromCase(ARRAY(INT()), null, null)
                        .fromCase(
                                ARRAY(STRING()),
                                new String[] {"1", "2", "3"},
                                new Integer[] {1, 2, 3})
                        // https://issues.apache.org/jira/browse/FLINK-24425 Cast from corresponding
                        // single type
                        // .fromCase(INT(), DEFAULT_POSITIVE_INT, new int[] {DEFAULT_POSITIVE_INT})
                        .fromCase(ARRAY(INT()), new int[] {1, 2, 3}, new Integer[] {1, 2, 3})
                        .build(),
                CastTestSpecBuilder.testCastTo(ARRAY(STRING().nullable()))
                        .fromCase(
                                ARRAY(TIMESTAMP(4).nullable()),
                                new LocalDateTime[] {
                                    LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                    null,
                                    LocalDateTime.parse("2021-09-24T14:34:56.123456")
                                },
                                new String[] {
                                    "2021-09-24 12:34:56.1234", null, "2021-09-24 14:34:56.1234"
                                })
                        .build(),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().nullable()))
                        .fromCase(
                                ARRAY(INT().nullable()),
                                new Integer[] {1, null, 2},
                                new Long[] {1L, null, 2L})
                        .build(),
                CastTestSpecBuilder.testCastTo(ARRAY(BIGINT().notNull()))
                        .fromCase(ARRAY(INT().notNull()), new Integer[] {1, 2}, new long[] {1L, 2L})
                        .build(),
                CastTestSpecBuilder.testCastTo(ARRAY(ROW(INT(), STRING()).notNull()))
                        .fromCase(
                                ARRAY(ROW(INT(), VARCHAR(4)).notNull()),
                                new Row[] {Row.of(1, "two"), Row.of(3, "four")},
                                new Row[] {Row.of(1, "two"), Row.of(3, "four")})
                        .build(),
                CastTestSpecBuilder.testCastTo(MAP(ROW(INT()), STRING()))
                        .fromCase(
                                MAP(ROW(INT()), VARCHAR(4)),
                                map(entry(Row.of(1), "two"), entry(Row.of(3), "four")),
                                map(entry(Row.of(1), "two"), entry(Row.of(3), "four")))
                        .build(),
                CastTestSpecBuilder.testCastTo(ROW(BIGINT(), BIGINT(), STRING(), ARRAY(STRING())))
                        .fromCase(
                                ROW(INT(), INT(), TIME(), ARRAY(CHAR(1))),
                                Row.of(10, null, DEFAULT_TIME, new String[] {"a", "b", "c"}),
                                Row.of(10L, null, "12:34:56", new String[] {"a", "b", "c"}))
                        .build());
    }

    private static class CastTestSpecBuilder {
        private TestSetSpec testSetSpec;
        private DataType targetType;
        private final List<Object> columnData = new ArrayList<>();
        private final List<DataType> columnTypes = new ArrayList<>();
        private final List<Object> expectedValues = new ArrayList<>();
        private final List<TestType> testTypes = new ArrayList<>();

        private enum TestType {
            RESULT,
            ERROR_SQL,
            ERROR_TABLE_API,
            ERROR_RUNTIME
        }

        private static CastTestSpecBuilder testCastTo(DataType targetType) {
            CastTestSpecBuilder tsb = new CastTestSpecBuilder();
            tsb.targetType = targetType;
            tsb.testSetSpec =
                    TestSetSpec.forFunction(
                            BuiltInFunctionDefinitions.CAST, "To " + targetType.toString());
            return tsb;
        }

        private CastTestSpecBuilder fromCase(DataType dataType, Object src, Object target) {
            this.testTypes.add(TestType.RESULT);
            this.columnTypes.add(dataType);
            this.columnData.add(src);
            this.expectedValues.add(target);
            assertThat(
                            LogicalTypeCasts.supportsExplicitCast(
                                    dataType.getLogicalType(), targetType.getLogicalType()))
                    .as("Should support explicit casting")
                    .isTrue();
            return this;
        }

        private CastTestSpecBuilder failTableApiValidation(DataType dataType, Object src) {
            return failValidation(TestType.ERROR_TABLE_API, dataType, src);
        }

        private CastTestSpecBuilder failSqlValidation(DataType dataType, Object src) {
            return failValidation(TestType.ERROR_TABLE_API, dataType, src);
        }

        private CastTestSpecBuilder failValidation(DataType dataType, Object src) {
            failValidation(TestType.ERROR_TABLE_API, dataType, src);
            return failValidation(TestType.ERROR_SQL, dataType, src);
        }

        private CastTestSpecBuilder failRuntime(
                DataType dataType, Object src, Class<? extends Throwable> failureClass) {
            this.testTypes.add(TestType.ERROR_RUNTIME);
            this.columnTypes.add(dataType);
            this.columnData.add(src);
            this.expectedValues.add(failureClass);
            return this;
        }

        private CastTestSpecBuilder failValidation(TestType type, DataType dataType, Object src) {
            this.testTypes.add(type);
            this.columnTypes.add(dataType);
            this.columnData.add(src);
            return this;
        }

        private TestSetSpec build() {
            List<ResultSpec> testSpecs = new ArrayList<>(columnData.size());
            // expectedValues may contain less elements if there are also error test cases
            int idxOffset = 0;
            for (int i = 0; i < columnData.size(); i++) {
                String colName = "f" + i;
                LogicalType colType = columnTypes.get(i).getLogicalType();
                String errorMsg;
                switch (testTypes.get(i)) {
                    case ERROR_TABLE_API:
                        errorMsg =
                                specificErrorMsg(
                                        colType,
                                        String.format(
                                                "Invalid function call:%ncast("
                                                        + columnTypes.get(i).toString()
                                                        + ", "
                                                        + targetType.toString()
                                                        + ")"));
                        testSetSpec.testTableApiValidationError(
                                $(colName).cast(targetType), errorMsg);
                        idxOffset++;
                        break;
                    case ERROR_SQL:
                        errorMsg =
                                specificErrorMsg(
                                        colType, "Cast function cannot convert value of type ");
                        testSetSpec.testSqlValidationError(
                                "CAST(" + colName + " AS " + targetType.toString() + ")", errorMsg);
                        idxOffset++;
                        break;
                    case ERROR_RUNTIME:
                        @SuppressWarnings("unchecked")
                        Class<? extends Throwable> throwableClazz =
                                (Class<? extends Throwable>) expectedValues.get(i - idxOffset);
                        testSetSpec.testSqlRuntimeError(
                                "CAST(" + colName + " AS " + targetType.toString() + ")",
                                throwableClazz);
                        testSetSpec.testTableApiRuntimeError(
                                $(colName).cast(targetType), throwableClazz);
                        break;
                    case RESULT:
                        testSpecs.add(
                                resultSpec(
                                        $(colName).cast(targetType),
                                        "CAST(" + colName + " AS " + targetType.toString() + ")",
                                        expectedValues.get(i - idxOffset),
                                        targetType));
                        break;
                }
            }
            testSetSpec
                    .onFieldsWithData(columnData.toArray())
                    .andDataTypes(columnTypes.toArray(new AbstractDataType<?>[] {}))
                    .testResult(testSpecs.toArray(new ResultSpec[0]));
            return testSetSpec;
        }

        private String specificErrorMsg(LogicalType colType, String defaultMsg) {
            if (isTimestampLtzToNumeric(colType, targetType.getLogicalType())) {
                return "The cast from TIMESTAMP_LTZ type to NUMERIC type is not allowed.";
            } else if (isNumericToTimestamp(colType, targetType.getLogicalType())) {
                return "type is not allowed. It's recommended to use TO_TIMESTAMP";
            } else if (isTimestampToNumeric(colType, targetType.getLogicalType())) {
                return "type is not allowed. It's recommended to use "
                        + "UNIX_TIMESTAMP(CAST(timestamp_col AS STRING)) instead.";
            } else {
                return defaultMsg;
            }
        }
    }

    private static Instant fromLocalToUTC(LocalDateTime localDateTime) {
        return localDateTime.atZone(TEST_TZ).toInstant();
    }

    private static Instant fromLocalTZ(String str) {
        return LocalDateTime.parse(str).toInstant(TEST_OFFSET);
    }

    private static boolean isTimestampLtzToNumeric(LogicalType srcType, LogicalType trgType) {
        return srcType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)
                && trgType.is(LogicalTypeFamily.NUMERIC);
    }

    private static boolean isNumericToTimestamp(LogicalType srcType, LogicalType trgType) {
        return srcType.is(LogicalTypeFamily.NUMERIC) && trgType.is(LogicalTypeFamily.TIMESTAMP);
    }

    private static boolean isTimestampToNumeric(LogicalType srcType, LogicalType trgType) {
        return srcType.is(LogicalTypeFamily.TIMESTAMP) && trgType.is(LogicalTypeFamily.NUMERIC);
    }
}

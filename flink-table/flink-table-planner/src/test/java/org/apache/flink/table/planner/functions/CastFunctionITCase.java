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
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.AbstractDataType;
import org.apache.flink.table.types.DataType;

import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MONTH;
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

/** Tests for {@link BuiltInFunctionDefinitions#CAST}. */
public class CastFunctionITCase extends BuiltInFunctionTestBase {

    private static final ZoneId TEST_TZ = ZoneId.of("Asia/Shanghai");
    private static final ZoneOffset TEST_OFFSET = ZoneOffset.ofHoursMinutes(-1, -20);

    @Override
    protected Configuration configuration() {
        return super.configuration().set(TableConfigOptions.LOCAL_TIME_ZONE, TEST_TZ.getId());
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                CastTestSpecBuilder.testCastTo(CHAR(3))
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(CHAR(4), "foo", "foo ")
                        .fromCase(CHAR(4), "foo ", "foo ")
                        .fromCase(VARCHAR(3), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo ", "foo ")
                        // https://issues.apache.org/jira/browse/FLINK-24413 - Trim to precision
                        // in this case down to 3 chars
                        .fromCase(STRING(), "abcdef", "abcdef") // "abc"
                        .fromCase(DATE(), LocalDate.parse("2021-09-30"), "2021-09-30") // "202"
                        .build(),
                CastTestSpecBuilder.testCastTo(VARCHAR(3))
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(CHAR(4), "foo", "foo ")
                        .fromCase(CHAR(4), "foo ", "foo ")
                        .fromCase(VARCHAR(3), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo", "foo")
                        .fromCase(VARCHAR(5), "foo ", "foo ")
                        // https://issues.apache.org/jira/browse/FLINK-24413 - Trim to precision
                        // in this case down to 3 chars
                        .fromCase(STRING(), "abcdef", "abcdef")
                        .build(),
                CastTestSpecBuilder.testCastTo(STRING())
                        .fromCase(STRING(), null, null)
                        .fromCase(CHAR(3), "foo", "foo")
                        .fromCase(CHAR(5), "foo", "foo  ")
                        .fromCase(VARCHAR(5), "Flink", "Flink")
                        .fromCase(VARCHAR(10), "Flink", "Flink")
                        .fromCase(STRING(), "Apache Flink", "Apache Flink")
                        .fromCase(STRING(), null, null)
                        .fromCase(BOOLEAN(), true, "true")
                        .fromCase(BINARY(2), new byte[] {0, 1}, "\u0000\u0001")
                        .fromCase(BINARY(3), new byte[] {0, 1}, "\u0000\u0001\u0000")
                        .fromCase(VARBINARY(3), new byte[] {0, 1, 2}, "\u0000\u0001\u0002")
                        .fromCase(VARBINARY(5), new byte[] {0, 1, 2}, "\u0000\u0001\u0002")
                        .fromCase(
                                BYTES(),
                                new byte[] {0, 1, 2, 3, 4},
                                "\u0000\u0001\u0002\u0003\u0004")
                        .fromCase(DECIMAL(4, 3), 9.87, "9.870")
                        // https://issues.apache.org/jira/browse/FLINK-24403 - Left zero padding
                        // currently not working
                        // .fromCase(DECIMAL(5, 3), 09.87, "09.870")
                        .fromCase(TINYINT(), -125, "-125")
                        .fromCase(SMALLINT(), 32767, "32767")
                        .fromCase(INT(), -12345678, "-12345678")
                        .fromCase(BIGINT(), 1234567891234L, "1234567891234")
                        .fromCase(FLOAT(), -123.456, "-123.456")
                        .fromCase(DOUBLE(), 12345.678901, "12345.678901")
                        .fromCase(DATE(), LocalDate.parse("2021-09-24"), "2021-09-24")
                        // https://issues.apache.org/jira/browse/FLINK-17224 Currently, fractional
                        // seconds are lost
                        .fromCase(TIME(5), LocalTime.parse("12:34:56.1234567"), "12:34:56")
                        .fromCase(
                                TIMESTAMP(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                "2021-09-24 12:34:56.123456")
                        .fromCase(
                                TIMESTAMP(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                "2021-09-24 12:34:56.1234")

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        .fromCase(
                                TIMESTAMP_LTZ(5),
                                fromLocalTZ("2021-09-24T18:34:56.123456"),
                                "2021-09-25 03:54:56.12345")
                        .fromCase(INTERVAL(YEAR()), 84, "+7-00")
                        .fromCase(INTERVAL(MONTH()), 5, "+0-05")
                        .fromCase(INTERVAL(MONTH()), 123, "+10-03")
                        .fromCase(INTERVAL(MONTH()), 12334, "+1027-10")
                        .fromCase(INTERVAL(DAY()), 10, "+0 00:00:00.010")
                        .fromCase(INTERVAL(DAY()), 123456789L, "+1 10:17:36.789")
                        .fromCase(INTERVAL(DAY()), Duration.ofHours(36), "+1 12:00:00.000")
                        // https://issues.apache.org/jira/browse/FLINK-21456 Not supported currently
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(BOOLEAN())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(CHAR(4), "true", true)
                        .fromCase(VARCHAR(5), "FalsE", false)
                        .fromCase(STRING(), "Apache Flink", null)
                        .fromCase(STRING(), "TRUE", true)
                        .fromCase(STRING(), "", null)
                        .fromCase(BOOLEAN(), true, true)
                        .fromCase(BOOLEAN(), false, false)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, true)
                        .fromCase(DECIMAL(2, 1), 0.0, false)
                        .fromCase(TINYINT(), -125, true)
                        .fromCase(TINYINT(), 0, false)
                        .fromCase(SMALLINT(), 32767, true)
                        .fromCase(SMALLINT(), 0, false)
                        .fromCase(INT(), -12345678, true)
                        .fromCase(INT(), 0, false)
                        .fromCase(BIGINT(), 1234567891234L, true)
                        .fromCase(BIGINT(), 0, false)
                        .fromCase(FLOAT(), -123.456, true)
                        .fromCase(FLOAT(), 0, false)
                        .fromCase(DOUBLE(), 12345.67890, true)
                        .fromCase(DOUBLE(), 0, false)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(BINARY(2))
                        .fromCase(CHAR(3), "foo", new byte[] {102, 111, 111})
                        .fromCase(VARCHAR(5), "Flink", new byte[] {70, 108, 105, 110, 107})
                        // https://issues.apache.org/jira/browse/FLINK-24419 - not trimmed to 2
                        // bytes
                        .fromCase(STRING(), "Apache", new byte[] {65, 112, 97, 99, 104, 101})
                        // Not supported - no fix
                        // BOOLEAN
                        .fromCase(BINARY(2), new byte[] {0, 1}, new byte[] {0, 1})
                        .fromCase(VARBINARY(3), new byte[] {0, 1, 2}, new byte[] {0, 1, 2})
                        .fromCase(BYTES(), new byte[] {0, 1, 2, 3, 4}, new byte[] {0, 1, 2, 3, 4})
                        // Not supported - no fix
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // https://issues.apache.org/jira/browse/FLINK-24418
                        // .fromCase(RAW(byte[].class), new byte[] {1}, new byte[] {1})
                        .build(),
                CastTestSpecBuilder.testCastTo(VARBINARY(4))
                        .fromCase(CHAR(3), "foo", new byte[] {102, 111, 111})
                        .fromCase(VARCHAR(5), "Flink", new byte[] {70, 108, 105, 110, 107})
                        // https://issues.apache.org/jira/browse/FLINK-24419 - not trimmed to 2
                        // bytes
                        .fromCase(STRING(), "Apache", new byte[] {65, 112, 97, 99, 104, 101})
                        // Not supported
                        // BOOLEAN
                        .fromCase(BINARY(2), new byte[] {0, 1}, new byte[] {0, 1})
                        .fromCase(VARBINARY(3), new byte[] {0, 1, 2}, new byte[] {0, 1, 2})
                        .fromCase(BYTES(), new byte[] {0, 1, 2, 3, 4}, new byte[] {0, 1, 2, 3, 4})
                        // Not supported - no fix
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // https://issues.apache.org/jira/browse/FLINK-24418
                        // .fromCase(RAW(byte[].class), new byte[] {1}, new byte[] {1})
                        .build(),
                CastTestSpecBuilder.testCastTo(BYTES())
                        .fromCase(CHAR(3), "foo", new byte[] {102, 111, 111})
                        .fromCase(VARCHAR(5), "Flink", new byte[] {70, 108, 105, 110, 107})
                        .fromCase(STRING(), "Apache", new byte[] {65, 112, 97, 99, 104, 101})
                        // Not supported - no fix
                        // BOOLEAN
                        .fromCase(BINARY(2), new byte[] {0, 1}, new byte[] {0, 1})
                        .fromCase(VARBINARY(3), new byte[] {0, 1, 2}, new byte[] {0, 1, 2})
                        .fromCase(BYTES(), new byte[] {0, 1, 2, 3, 4}, new byte[] {0, 1, 2, 3, 4})
                        // Not supported - no fix
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        //
                        // https://issues.apache.org/jira/browse/FLINK-24418
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(DECIMAL(4, 3))
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", new BigDecimal("1.234".toCharArray(), 0, 5))
                        .fromCase(BOOLEAN(), true, new BigDecimal("1.000".toCharArray(), 0, 5))
                        .fromCase(BOOLEAN(), false, new BigDecimal("0.000".toCharArray(), 0, 5))
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, new BigDecimal("9.870"))
                        .fromCase(TINYINT(), -125, null)
                        .fromCase(SMALLINT(), 32767, null)
                        .fromCase(INT(), -12345678, null)
                        .fromCase(BIGINT(), 1234567891234L, null)
                        .fromCase(FLOAT(), -123.456, null)
                        .fromCase(DOUBLE(), 12345.67890, null)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TINYINT())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", (byte) 1)
                        .fromCase(STRING(), "123", (byte) 123)
                        .fromCase(STRING(), "-130", null)
                        .fromCase(BOOLEAN(), true, (byte) 1)
                        .fromCase(BOOLEAN(), false, (byte) 0)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, (byte) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(10, 3), 9123.87, (byte) -93)
                        .fromCase(TINYINT(), -125, (byte) -125)
                        .fromCase(SMALLINT(), 32, (byte) 32)
                        .fromCase(SMALLINT(), 32767, (byte) -1)
                        .fromCase(INT(), -12, (byte) -12)
                        .fromCase(INT(), -12345678, (byte) -78)
                        .fromCase(BIGINT(), 123, (byte) 123)
                        .fromCase(BIGINT(), 1234567891234L, (byte) 34)
                        .fromCase(FLOAT(), -123.456, (byte) -123)
                        .fromCase(FLOAT(), 128.456, (byte) -128)
                        .fromCase(DOUBLE(), 123.4567890, (byte) 123)
                        .fromCase(DOUBLE(), 12345.67890, (byte) 57)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(SMALLINT())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", (short) 1)
                        .fromCase(STRING(), "123", (short) 123)
                        .fromCase(STRING(), "-32769", null)
                        .fromCase(BOOLEAN(), true, (short) 1)
                        .fromCase(BOOLEAN(), false, (short) 0)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, (short) 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(10, 3), 91235.87, (short) 25699)
                        .fromCase(TINYINT(), -125, (short) -125)
                        .fromCase(SMALLINT(), 32, (short) 32)
                        .fromCase(SMALLINT(), 32780, (short) -32756)
                        .fromCase(INT(), -12, (short) -12)
                        .fromCase(INT(), -12345678, (short) -24910)
                        .fromCase(BIGINT(), 123, (short) 123)
                        .fromCase(BIGINT(), 1234567891234L, (short) 2338)
                        .fromCase(FLOAT(), -123.456, (short) -123)
                        .fromCase(FLOAT(), 123456.78, (short) -7616)
                        .fromCase(DOUBLE(), 123.4567890, (short) 123)
                        .fromCase(DOUBLE(), 123456.7890, (short) -7616)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(INT())
                        .fromCase(INT(), null, null)
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", 1)
                        .fromCase(STRING(), "123", 123)
                        .fromCase(STRING(), "-3276913443134", null)
                        .fromCase(BOOLEAN(), true, 1)
                        .fromCase(BOOLEAN(), false, 0)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, 9)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, -146603714)
                        .fromCase(TINYINT(), -125, -125)
                        .fromCase(SMALLINT(), 32, 32)
                        .fromCase(INT(), -12345678, -12345678)
                        .fromCase(BIGINT(), 123, 123)
                        .fromCase(BIGINT(), 1234567891234L, 1912277282)
                        .fromCase(FLOAT(), -123.456, -123)
                        .fromCase(FLOAT(), 9234567891.12, 644633299)
                        .fromCase(DOUBLE(), 123.4567890, 123)
                        .fromCase(DOUBLE(), 9234567891.12345, 644633299)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(BIGINT())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", 1L)
                        .fromCase(STRING(), "123", 123L)
                        .fromCase(STRING(), "-3276913443134", -3276913443134L)
                        .fromCase(BOOLEAN(), true, 1L)
                        .fromCase(BOOLEAN(), false, 0L)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, 9L)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3276913443134L)
                        .fromCase(TINYINT(), -125, -125L)
                        .fromCase(SMALLINT(), 32, 32L)
                        .fromCase(INT(), -12345678, -12345678L)
                        .fromCase(BIGINT(), 1234567891234L, 1234567891234L)
                        .fromCase(FLOAT(), -123.456, -123L)
                        .fromCase(FLOAT(), 9234567891.12, 9234567891L)
                        .fromCase(DOUBLE(), 123.4567890, 123L)
                        .fromCase(DOUBLE(), 9234567891.12345, 9234567891L)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(FLOAT())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", 1.234f)
                        .fromCase(STRING(), "123", 123.0f)
                        .fromCase(STRING(), "-3276913443134", -3.27691403E12f)
                        .fromCase(BOOLEAN(), true, 1.0f)
                        .fromCase(BOOLEAN(), false, 0.0f)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87f)
                        // https://issues.apache.org/jira/browse/FLINK-24420 - Check out of range
                        // instead of overflow
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87f)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3.27691351E12f)
                        .fromCase(TINYINT(), -125, -125f)
                        .fromCase(SMALLINT(), 32, 32f)
                        .fromCase(INT(), -12345678, -12345678f)
                        .fromCase(BIGINT(), 1234567891234L, 1234567891234f)
                        .fromCase(FLOAT(), -123.456, -123.456f)
                        .fromCase(FLOAT(), 9234567891.12, 9234567891.12f)
                        .fromCase(DOUBLE(), 123.4567890, 123.45679f)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234, 1.23923451E12f)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(DOUBLE())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "Apache", null)
                        .fromCase(STRING(), "1.234", 1.234d)
                        .fromCase(STRING(), "123", 123.0d)
                        .fromCase(STRING(), "-3276913443134", -3.276913443134E12)
                        .fromCase(BOOLEAN(), true, 1.0d)
                        .fromCase(BOOLEAN(), false, 0.0d)
                        // Not supported - no fix
                        // BINARY
                        // VARBINARY
                        // BYTES
                        .fromCase(DECIMAL(4, 3), 9.87, 9.87d)
                        .fromCase(DECIMAL(20, 3), 3276913443134.87, 3.27691344313487E12d)
                        .fromCase(TINYINT(), -125, -125d)
                        .fromCase(SMALLINT(), 32, 32d)
                        .fromCase(INT(), -12345678, -12345678d)
                        .fromCase(BIGINT(), 1234567891234L, 1234567891234d)
                        .fromCase(FLOAT(), -123.456, -123.456d)
                        .fromCase(FLOAT(), 9234567891.12, 9234567891.12d)
                        .fromCase(DOUBLE(), 123.4567890, 123.456789d)
                        .fromCase(DOUBLE(), 1239234567891.1234567891234, 1.2392345678911235E12d)
                        // Not supported - no fix
                        // DATE
                        // TIME
                        // TIMESTAMP
                        // TIMESTAMP_WITH_TIME_ZONE
                        // TIMESTAMP_LTZ
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(DATE())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "123", LocalDate.of(123, 1, 1))
                        .fromCase(STRING(), "2021-09-27", LocalDate.of(2021, 9, 27))
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                LocalDate.of(2021, 9, 27))
                        .fromCase(STRING(), "2021/09/27", null)
                        // Not supported - no fix
                        // BOOLEAN
                        // BINARY
                        // VARBINARY
                        // BYTES
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        .fromCase(DATE(), LocalDate.parse("2021-09-24"), LocalDate.of(2021, 9, 24))
                        // Not supported - no fix
                        // TIME
                        .fromCase(
                                TIMESTAMP(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDate.of(2021, 9, 24))
                        .fromCase(
                                TIMESTAMP(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDate.of(2021, 9, 24))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDate.of(2021, 9, 24))
                        .fromCase(
                                TIMESTAMP_LTZ(),
                                fromLocalTZ("2021-09-24T18:34:56.123456"),
                                LocalDate.of(2021, 9, 25))
                        // Not supported - no fix
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIME())
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "123", LocalTime.of(23, 0, 0))
                        .fromCase(STRING(), "123:45", LocalTime.of(23, 45, 0))
                        .fromCase(STRING(), "2021-09-27", null)
                        .fromCase(STRING(), "2021-09-27 12:34:56", null)
                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        .fromCase(STRING(), "12:34:56.123456789", LocalTime.of(12, 34, 56, 0))
                        .fromCase(STRING(), "2021-09-27 12:34:56.123456789", null)
                        // Not supported - no fix
                        // BOOLEAN
                        // BINARY
                        // VARBINARY
                        // BYTES
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        // DATE
                        .fromCase(
                                TIME(5),
                                LocalTime.parse("12:34:56.1234567"),
                                LocalTime.of(12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalTime.of(12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalTime.of(12, 34, 56, 0))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalTime.of(12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                fromLocalTZ("2021-09-24T22:34:56.123456"),
                                LocalTime.of(7, 54, 56, 0))
                        // Not supported - no fix
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIMESTAMP(9))
                        .fromCase(CHAR(3), "foo", null)
                        .fromCase(VARCHAR(5), "Flink", null)
                        .fromCase(STRING(), "123", null)
                        .fromCase(STRING(), "2021-09-27", LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0))
                        .fromCase(STRING(), "2021/09/27", null)
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123456789))
                        // Not supported - no fix
                        // BOOLEAN
                        // BINARY
                        // VARBINARY
                        // BYTES
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        .fromCase(
                                DATE(),
                                LocalDate.parse("2021-09-24"),
                                LocalDateTime.of(2021, 9, 24, 0, 0, 0, 0))

                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        // https://issues.apache.org/jira/browse/FLINK-24423 Continue using EPOCH
                        // date or use 0 for the year?
                        .fromCase(
                                TIME(5),
                                LocalTime.parse("12:34:56.1234567"),
                                LocalDateTime.of(1970, 1, 1, 12, 34, 56, 0))
                        .fromCase(
                                TIMESTAMP(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456000))
                        .fromCase(
                                TIMESTAMP(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000))
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                fromLocalTZ("2021-09-24T22:34:56.123456"),
                                LocalDateTime.of(2021, 9, 25, 7, 54, 56, 123400000))
                        // Not supported - no fix
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                CastTestSpecBuilder.testCastTo(TIMESTAMP_LTZ(9))
                        // https://issues.apache.org/jira/browse/FLINK-24424 - Throws NPE
                        // .fromCase(CHAR(3), "foo", null)
                        // .fromCase(VARCHAR(5), "Flink", null)
                        // .fromCase(STRING(), "123", null)
                        .fromCase(
                                STRING(),
                                "2021-09-27",
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 27, 0, 0, 0, 0)))
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123",
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 27, 12, 34, 56, 123000000)))
                        // https://issues.apache.org/jira/browse/FLINK-24403 Fractional seconds are
                        // lost
                        .fromCase(
                                STRING(),
                                "2021-09-27 12:34:56.123456789",
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 27, 12, 34, 56, 0)))

                        // Not supported - no fix
                        // BOOLEAN
                        // BINARY
                        // VARBINARY
                        // BYTES
                        // DECIMAL
                        // TINYINT
                        // SMALLINT
                        // INT
                        // BIGINT
                        // FLOAT
                        // DOUBLE
                        .fromCase(
                                DATE(),
                                LocalDate.parse("2021-09-24"),
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 24, 0, 0, 0, 0)))

                        // https://issues.apache.org/jira/browse/FLINK-17224 Fractional seconds are
                        // lost
                        // https://issues.apache.org/jira/browse/FLINK-24423 Continue using EPOCH
                        // date or use 0 for the year?
                        .fromCase(
                                TIME(5),
                                LocalTime.parse("12:34:56.1234567"),
                                fromLocalToUTC(LocalDateTime.of(1970, 1, 1, 12, 34, 56, 0)))
                        .fromCase(
                                TIMESTAMP(),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123456000)))
                        .fromCase(
                                TIMESTAMP(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000)))

                        // https://issues.apache.org/jira/browse/FLINK-20869
                        // TIMESTAMP_WITH_TIME_ZONE

                        // https://issues.apache.org/jira/browse/FLINK-24422 - Accept only Instant
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                LocalDateTime.parse("2021-09-24T12:34:56.123456"),
                                fromLocalToUTC(
                                        LocalDateTime.of(2021, 9, 24, 12, 34, 56, 123400000)))
                        .fromCase(
                                TIMESTAMP_LTZ(4),
                                fromLocalTZ("2021-09-24T22:34:56.123456"),
                                fromLocalToUTC(LocalDateTime.of(2021, 9, 25, 7, 54, 56, 123400000)))
                        // Not supported - no fix
                        // INTERVAL_YEAR_MONTH
                        // INTERVAL_DAY_TIME
                        // ARRAY
                        // MULTISET
                        // MAP
                        // ROW
                        // RAW
                        .build(),
                // CastTestSpecBuilder
                // .testCastTo(INTERVAL(YEAR()))
                // https://issues.apache.org/jira/browse/FLINK-24403 allow cast from string
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
                        // https://issues.apache.org/jira/browse/FLINK-24403 allow cast from string
                        // .fromCase(STRING(), "'+01-05'".resultsIn(Period.of(0, 17, 0)
                        .testCastTo(INTERVAL(MONTH()))
                        .fromCase(INTERVAL(YEAR()), 0, Period.of(0, 0, 0))
                        .fromCase(INTERVAL(YEAR()), 11, Period.of(0, 11, 0))
                        .fromCase(INTERVAL(YEAR()), 84, Period.of(0, 84, 0))
                        .fromCase(INTERVAL(YEAR()), 89, Period.of(0, 89, 0))
                        .fromCase(INTERVAL(MONTH()), 89, Period.of(0, 89, 0))
                        // https://issues.apache.org/jira/browse/FLINK-24428
                        // .fromCase(INTERVAL(DAY()), Duration.ofDays(300), Period.of(0, 0, 0))
                        // .fromCase(INTERVAL(DAY()), Duration.ofDays(400), Period.of(1, 0, 0))
                        // .fromCase(INTERVAL(HOUR()), Duration.ofDays(400), Period.of(1, 0, 0))
                        .build(),
                // CastTestSpecBuilder
                // .testCastTo(INTERVAL(DAY()))
                // https://issues.apache.org/jira/browse/FLINK-24403 allow cast from string
                // .fromCase(STRING(), "+41 10:17:36.789", Duration.of(...))
                // https://issues.apache.org/jira/browse/FLINK-24428
                // .build()
                CastTestSpecBuilder.testCastTo(ARRAY(INT()))
                        // https://issues.apache.org/jira/browse/FLINK-17321
                        // .fromCase(ARRAY(STRING()), new String[] {'1', '2', '3'}, new Integer[]
                        // {1, 2, 3})
                        // https://issues.apache.org/jira/browse/FLINK-24425 Cast from corresponding
                        // single type
                        // .fromCase(INT(), 10, new int[] {10})
                        .fromCase(ARRAY(INT()), new int[] {1, 2, 3}, new Integer[] {1, 2, 3})
                        .build()
                // https://issues.apache.org/jira/browse/FLINK-17321
                // ARRAY
                // MULTISET
                // MAP
                // RAW
                // ROW
                );
    }

    private static class CastTestSpecBuilder {
        private TestSpec testSpec;
        private DataType targetType;
        private final List<Object> columnData = new ArrayList<>();
        private final List<AbstractDataType<?>> columnTypes = new ArrayList<>();
        private final List<Object> expectedValues = new ArrayList<>();

        private static CastTestSpecBuilder testCastTo(DataType targetType) {
            CastTestSpecBuilder tsb = new CastTestSpecBuilder();
            tsb.targetType = targetType;
            tsb.testSpec =
                    TestSpec.forFunction(
                            BuiltInFunctionDefinitions.CAST, "To " + targetType.toString());
            return tsb;
        }

        private CastTestSpecBuilder fromCase(
                AbstractDataType<?> dataType, Object src, Object target) {
            this.columnTypes.add(dataType);
            this.columnData.add(src);
            this.expectedValues.add(target);
            return this;
        }

        private TestSpec build() {
            ResultSpec[] testSpecs = new ResultSpec[columnData.size()];
            for (int i = 0; i < columnData.size(); i++) {
                String colName = "f" + i;
                testSpecs[i] =
                        resultSpec(
                                $(colName).cast(targetType),
                                "CAST(" + colName + " AS " + targetType.toString() + ")",
                                expectedValues.get(i),
                                targetType);
            }
            testSpec.onFieldsWithData(columnData.toArray())
                    .andDataTypes(columnTypes.toArray(new AbstractDataType<?>[] {}))
                    .testResult(testSpecs);
            return testSpec;
        }
    }

    private static Instant fromLocalToUTC(LocalDateTime localDateTime) {
        return localDateTime.atZone(TEST_TZ).toInstant();
    }

    private static Instant fromLocalTZ(String str) {
        return LocalDateTime.parse(str).toInstant(TEST_OFFSET);
    }
}

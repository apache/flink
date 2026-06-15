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

package org.apache.flink.table.planner.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.expressions.DefaultSqlFactory;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.SqlFactory;
import org.apache.flink.table.operations.ProjectQueryOperation;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ResolvedExpression#asSerializableString(SqlFactory)}. */
@ExtendWith(MiniClusterExtension.class)
public class LiteralExpressionsSerializationITCase {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    private static final String POINT_WKB_HEX = "0101000000000000000000f03f0000000000000040";

    @Test
    void testGeographySqlSerialization() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final GeographyData geography = GeographyData.fromBytes(POINT_WKB);
        final Table t =
                env.fromValues(1)
                        .select(
                                lit(geography, DataTypes.GEOGRAPHY().notNull()),
                                lit(null, DataTypes.GEOGRAPHY()),
                                array(
                                        lit(geography, DataTypes.GEOGRAPHY().notNull()),
                                        lit(null, DataTypes.GEOGRAPHY())),
                                map(
                                        lit("a", DataTypes.STRING().notNull()),
                                        lit(geography, DataTypes.GEOGRAPHY().notNull())),
                                row(
                                        lit(geography, DataTypes.GEOGRAPHY().notNull()),
                                        lit(null, DataTypes.GEOGRAPHY())));

        final ProjectQueryOperation operation = (ProjectQueryOperation) t.getQueryOperation();
        final List<String> expressions =
                operation.getProjectList().stream()
                        .map(
                                resolvedExpression ->
                                        resolvedExpression.asSerializableString(
                                                DefaultSqlFactory.INSTANCE))
                        .collect(Collectors.toList());

        assertThat(expressions.get(0)).isEqualTo("ST_GEOGFROMWKB(X'" + POINT_WKB_HEX + "')");
        assertThat(expressions.get(1)).isEqualTo("CAST(NULL AS GEOGRAPHY)");
        assertThat(expressions.get(2))
                .contains("ST_GEOGFROMWKB(X'" + POINT_WKB_HEX + "')")
                .contains("CAST(NULL AS GEOGRAPHY)");
        assertThat(expressions.get(3))
                .isEqualTo("MAP['a', ST_GEOGFROMWKB(X'" + POINT_WKB_HEX + "')]");
        assertThat(expressions.get(4))
                .contains("ST_GEOGFROMWKB(X'" + POINT_WKB_HEX + "')")
                .contains("CAST(NULL AS GEOGRAPHY)");

        final TableResult tableResult =
                env.sqlQuery(String.format("SELECT %s", String.join(", ", expressions))).execute();
        final Row result = CollectionUtil.iteratorToList(tableResult.collect()).get(0);

        assertGeography(result.getField(0));
        assertThat(result.getField(1)).isNull();

        final Object arrayField = result.getField(2);
        final List<?> arrayValue =
                arrayField instanceof List
                        ? (List<?>) arrayField
                        : Arrays.asList((Object[]) arrayField);
        assertThat(arrayValue).hasSize(2);
        assertGeography(arrayValue.get(0));
        assertThat(arrayValue.get(1)).isNull();

        final Map<?, ?> mapValue = (Map<?, ?>) result.getField(3);
        assertThat(mapValue).hasSize(1);
        assertGeography(mapValue.get("a"));

        final Row rowValue = (Row) result.getField(4);
        assertGeography(rowValue.getField(0));
        assertThat(rowValue.getField(1)).isNull();
    }

    @Test
    void testSqlSerialization() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final LocalTime localTime = LocalTime.of(12, 12, 12).plus(333, ChronoUnit.MILLIS);
        final LocalTime localTimeWithoutSeconds = LocalTime.of(12, 12);
        final LocalDate localDate = LocalDate.of(2024, 2, 3);
        final LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        final LocalDateTime localDateTimeWithoutSeconds =
                LocalDateTime.of(localDate, localTimeWithoutSeconds);
        final Instant instant = Instant.ofEpochMilli(1234567);
        final Duration defaultDuration = Duration.ofDays(99).plusSeconds(34).plusMillis(999);
        final Period defaultPeriod = Period.ofMonths(470);
        final Period yearMonthPeriod = Period.ofMonths(9999 * 12 + 11);
        final Duration dayHourDuration = Duration.ofDays(100).plusHours(5);
        final Duration secondPrecisionDuration = Duration.ofSeconds(45).plusMillis(750);
        final Duration maxDayDuration = Duration.ofDays(999_999);
        final Period derivedLargePeriod = Period.ofMonths(1200);
        final Table t =
                env.fromValues(1)
                        .select(
                                lit((byte) 1, DataTypes.TINYINT().notNull()),
                                lit((short) 1, DataTypes.SMALLINT().notNull()),
                                lit(1, DataTypes.INT().notNull()),
                                lit(1L, DataTypes.BIGINT().notNull()),
                                lit(1d, DataTypes.DOUBLE().notNull()),
                                lit(1f, DataTypes.FLOAT().notNull()),
                                lit("abc", DataTypes.STRING().notNull()),
                                lit(new byte[] {1, 2, 3}, DataTypes.BYTES().notNull()),
                                lit(true, DataTypes.BOOLEAN().notNull()),
                                lit(new BigDecimal("123.456"), DataTypes.DECIMAL(6, 3).notNull()),
                                lit(new BigDecimal("123.456"), DataTypes.DECIMAL(6, 2).notNull()),
                                nullOf(DataTypes.STRING()),
                                lit(localDate, DataTypes.DATE().notNull()),
                                lit(localTime, DataTypes.TIME().notNull()),
                                lit(localTimeWithoutSeconds, DataTypes.TIME().notNull()),
                                lit(localDateTime, DataTypes.TIMESTAMP(3).notNull()),
                                lit(localDateTimeWithoutSeconds, DataTypes.TIMESTAMP(3).notNull()),
                                lit(instant, DataTypes.TIMESTAMP_LTZ(0).notNull()),
                                lit(instant, DataTypes.TIMESTAMP_LTZ(3).notNull()),
                                lit(instant, DataTypes.TIMESTAMP_LTZ(6).notNull()),
                                lit(instant, DataTypes.TIMESTAMP_LTZ(9).notNull()),
                                lit(
                                        defaultDuration,
                                        DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(9))
                                                .notNull()),
                                lit(
                                        defaultPeriod,
                                        DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH())
                                                .notNull()),
                                lit(
                                        yearMonthPeriod,
                                        DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH())
                                                .notNull()),
                                lit(
                                        dayHourDuration,
                                        DataTypes.INTERVAL(DataTypes.DAY(3), DataTypes.HOUR())
                                                .notNull()),
                                lit(
                                        secondPrecisionDuration,
                                        DataTypes.INTERVAL(DataTypes.SECOND(4)).notNull()),
                                lit(maxDayDuration, DataTypes.INTERVAL(DataTypes.DAY(6)).notNull()),
                                lit(defaultPeriod),
                                lit(derivedLargePeriod));
        final ProjectQueryOperation operation = (ProjectQueryOperation) t.getQueryOperation();
        final String exprStr =
                operation.getProjectList().stream()
                        .map(
                                resolvedExpression ->
                                        resolvedExpression.asSerializableString(
                                                DefaultSqlFactory.INSTANCE))
                        .collect(Collectors.joining(",\n"));

        assertThat(exprStr)
                .isEqualTo(
                        "CAST(1 AS TINYINT),\n"
                                + "CAST(1 AS SMALLINT),\n"
                                + "1,\n"
                                + "CAST(1 AS BIGINT),\n"
                                + "CAST(1.0 AS DOUBLE),\n"
                                + "CAST(1.0 AS FLOAT),\n"
                                + "'abc',\n"
                                + "X'010203',\n"
                                + "TRUE,\n"
                                + "123.456,\n"
                                + "CAST(123.456 AS DECIMAL(6, 2)),\n"
                                + "CAST(NULL AS VARCHAR(2147483647)),\n"
                                + "DATE '2024-02-03',\n"
                                + "TIME '12:12:12.333',\n"
                                + "TIME '12:12:00',\n"
                                + "TIMESTAMP '2024-02-03 12:12:12.333',\n"
                                + "TIMESTAMP '2024-02-03 12:12:00',\n"
                                + "CAST(TO_TIMESTAMP_LTZ(1234, 0) AS TIMESTAMP_LTZ(0)),\n"
                                + "TO_TIMESTAMP_LTZ(1234567, 3),\n"
                                + "TO_TIMESTAMP_LTZ(1234567000, 6),\n"
                                + "TO_TIMESTAMP_LTZ(1234567000000, 9),\n"
                                + "INTERVAL '99 00:00:34.999000000' DAY(2) TO SECOND(9),\n"
                                + "INTERVAL '39-2' YEAR(2) TO MONTH,\n"
                                + "INTERVAL '9999-11' YEAR(4) TO MONTH,\n"
                                + "INTERVAL '100 05:00:00.000000' DAY(3) TO SECOND(6),\n"
                                + "INTERVAL '0 00:00:45.7500' DAY(2) TO SECOND(4),\n"
                                + "INTERVAL '999999 00:00:00.000000' DAY(6) TO SECOND(6),\n"
                                + "INTERVAL '39-2' YEAR(2) TO MONTH,\n"
                                + "INTERVAL '100-0' YEAR(3) TO MONTH");

        final TableResult tableResult = env.sqlQuery(String.format("SELECT %s", exprStr)).execute();
        final List<Row> results = CollectionUtil.iteratorToList(tableResult.collect());
        assertThat(results)
                .containsExactly(
                        Row.of(
                                (byte) 1,
                                (short) 1,
                                1,
                                1L,
                                1d,
                                1f,
                                "abc",
                                new byte[] {1, 2, 3},
                                true,
                                new BigDecimal("123.456"),
                                new BigDecimal("123.46"),
                                null,
                                localDate,
                                localTime,
                                localTimeWithoutSeconds,
                                localDateTime,
                                localDateTimeWithoutSeconds,
                                Instant.ofEpochSecond(1234),
                                instant,
                                instant,
                                instant,
                                defaultDuration,
                                defaultPeriod,
                                yearMonthPeriod,
                                dayHourDuration,
                                secondPrecisionDuration,
                                maxDayDuration,
                                defaultPeriod,
                                derivedLargePeriod));
    }

    private static void assertGeography(Object value) {
        assertThat(value).isInstanceOf(GeographyData.class);
        assertThat(((GeographyData) value).toBytes()).isEqualTo(POINT_WKB);
    }
}

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
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
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
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ValueLiteralExpression#asSerializableString()}. */
@ExtendWith(MiniClusterExtension.class)
public class LiteralExpressionsSerializationITCase {

    @Test
    void testSqlSerialization() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final LocalTime localTime = LocalTime.of(12, 12, 12).plus(333, ChronoUnit.MILLIS);
        final LocalDate localDate = LocalDate.of(2024, 2, 3);
        final LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
        final Instant instant = Instant.ofEpochMilli(1234567);
        final Duration duration = Duration.ofDays(99).plusSeconds(34).plusMillis(999);
        final Period period = Period.ofMonths(470);
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
                                lit(localDateTime, DataTypes.TIMESTAMP(3).notNull()),
                                lit(instant, DataTypes.TIMESTAMP_LTZ(3).notNull()),
                                lit(
                                        duration,
                                        DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(9))
                                                .notNull()),
                                lit(
                                        period,
                                        DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH())
                                                .notNull()));
        final ProjectQueryOperation operation = (ProjectQueryOperation) t.getQueryOperation();
        final String exprStr =
                operation.getProjectList().stream()
                        .map(ResolvedExpression::asSerializableString)
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
                                + "TIMESTAMP '2024-02-03 12:12:12.333',\n"
                                + "TO_TIMESTAMP_LTZ(1234567, 3),\n"
                                + "INTERVAL '99 00:00:34.999' DAY TO SECOND(3),\n"
                                + "INTERVAL '39-2' YEAR TO MONTH");

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
                                localDateTime,
                                instant,
                                duration,
                                period));
    }
}

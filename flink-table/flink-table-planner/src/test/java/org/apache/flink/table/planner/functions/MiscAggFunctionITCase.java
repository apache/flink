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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.types.Row;

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
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.types.RowKind.INSERT;

/** Tests for built-in ARRAY_AGG aggregation functions. */
class MiscAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    private static final ZoneOffset TEST_OFFSET = ZoneOffset.ofHoursMinutes(-1, -20);
    private static final LocalDate DEFAULT_DATE = LocalDate.parse("2021-09-24");

    private static final LocalTime DEFAULT_TIME = LocalTime.parse("12:34:56.123");
    private static final LocalDateTime DEFAULT_TIMESTAMP =
            LocalDateTime.parse("2021-09-24T12:34:56.1234567");

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                TestSpec.forExpression("SINGLE_VALUE")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", 2),
                                        Row.ofKind(INSERT, "B", 2)))
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT f0, SINGLE_VALUE(f1) FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), INT()),
                                TableRuntimeException.class,
                                "SingleValueAggFunction received more than one element."),
                TestSpec.forExpression("FIRST_VALUE")
                        .withSource(
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(DataTypes.MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                Arrays.asList(
                                        Row.ofKind(
                                                INSERT,
                                                "A",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(LocalDate.EPOCH, LocalTime.NOON),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 2, 4),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {1, 2, 3}),
                                        Row.ofKind(
                                                INSERT,
                                                "A",
                                                DEFAULT_TIME,
                                                DEFAULT_DATE,
                                                DEFAULT_TIMESTAMP,
                                                fromLocalTZ("2100-09-24T22:34:56.1"),
                                                Period.of(0, 5, 4),
                                                Duration.of(321, ChronoUnit.MINUTES),
                                                new Integer[] {4, 5, 6}),
                                        Row.ofKind(
                                                INSERT,
                                                "B",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(
                                                        LocalDate.EPOCH, LocalTime.MIDNIGHT),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 3, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {7, 8, 9})))
                        .testResult(
                                source ->
                                        "SELECT f0, FIRST_VALUE(f1), FIRST_VALUE(f2), FIRST_VALUE(f3), FIRST_VALUE(f4), "
                                                + "FIRST_VALUE(f5), FIRST_VALUE(f6), FIRST_VALUE(f7) FROM "
                                                + source
                                                + " GROUP BY f0",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f0")),
                                        $("f0"),
                                        $("f1").firstValue(),
                                        $("f2").firstValue(),
                                        $("f3").firstValue(),
                                        $("f4").firstValue(),
                                        $("f5").firstValue(),
                                        $("f6").firstValue(),
                                        $("f7").firstValue()),
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                Arrays.asList(
                                        Row.of(
                                                "A",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(LocalDate.EPOCH, LocalTime.NOON),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 2, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {1, 2, 3}),
                                        Row.of(
                                                "B",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(
                                                        LocalDate.EPOCH, LocalTime.MIDNIGHT),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 3, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {7, 8, 9}))),
                TestSpec.forExpression("LAST_VALUE")
                        .withSource(
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(DataTypes.MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                Arrays.asList(
                                        Row.ofKind(
                                                INSERT,
                                                "A",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(LocalDate.EPOCH, LocalTime.NOON),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 2, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {1, 2, 3}),
                                        Row.ofKind(
                                                INSERT,
                                                "A",
                                                DEFAULT_TIME,
                                                DEFAULT_DATE,
                                                DEFAULT_TIMESTAMP,
                                                fromLocalTZ("2100-09-24T22:34:56.1"),
                                                Period.of(0, 5, 4),
                                                Duration.of(321, ChronoUnit.MINUTES),
                                                new Integer[] {4, 5, 6}),
                                        Row.ofKind(
                                                INSERT,
                                                "B",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(
                                                        LocalDate.EPOCH, LocalTime.MIDNIGHT),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 3, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {7, 8, 9})))
                        .testResult(
                                source ->
                                        "SELECT f0, LAST_VALUE(f1), LAST_VALUE(f2), LAST_VALUE(f3), LAST_VALUE(f4), "
                                                + "LAST_VALUE(f5), LAST_VALUE(f6), LAST_VALUE(f7) FROM "
                                                + source
                                                + " GROUP BY f0",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f0")),
                                        $("f0"),
                                        $("f1").lastValue(),
                                        $("f2").lastValue(),
                                        $("f3").lastValue(),
                                        $("f4").lastValue(),
                                        $("f5").lastValue(),
                                        $("f6").lastValue(),
                                        $("f7").lastValue()),
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                ROW(
                                        STRING(),
                                        TIME(),
                                        DATE(),
                                        TIMESTAMP(),
                                        TIMESTAMP_LTZ(),
                                        INTERVAL(MONTH()),
                                        INTERVAL(SECOND(3)),
                                        ARRAY(INT())),
                                Arrays.asList(
                                        Row.of(
                                                "A",
                                                DEFAULT_TIME,
                                                DEFAULT_DATE,
                                                DEFAULT_TIMESTAMP,
                                                fromLocalTZ("2100-09-24T22:34:56.1"),
                                                Period.of(0, 5, 0),
                                                Duration.of(321, ChronoUnit.MINUTES),
                                                new Integer[] {4, 5, 6}),
                                        Row.of(
                                                "B",
                                                LocalTime.NOON,
                                                LocalDate.EPOCH,
                                                LocalDateTime.of(
                                                        LocalDate.EPOCH, LocalTime.MIDNIGHT),
                                                fromLocalTZ("1900-09-24T22:34:56.1"),
                                                Period.of(0, 3, 0),
                                                Duration.of(123, ChronoUnit.MINUTES),
                                                new Integer[] {7, 8, 9}))));
    }

    private static Instant fromLocalTZ(String str) {
        return LocalDateTime.parse(str).toInstant(TEST_OFFSET);
    }
}

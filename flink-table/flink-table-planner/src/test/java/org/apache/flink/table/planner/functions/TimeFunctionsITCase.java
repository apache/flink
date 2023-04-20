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

import org.apache.flink.table.api.JsonExistsOnError;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DAY;
import static org.apache.flink.table.api.DataTypes.HOUR;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.temporalOverlaps;

/** Test time-related built-in functions. */
class TimeFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        extractTestCases(),
                        temporalOverlapsTestCases(),
                        ceilTestCases(),
                        floorTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> extractTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.EXTRACT)
                        .onFieldsWithData(
                                LocalDateTime.of(2000, 1, 31, 11, 22, 33, 123456789),
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987654321),
                                null,
                                LocalDate.of(1990, 10, 14),
                                Instant.ofEpochMilli(100000012),
                                true)
                        .andDataTypes(
                                TIMESTAMP(),
                                TIMESTAMP(),
                                TIMESTAMP(),
                                DATE(),
                                TIMESTAMP_LTZ(3),
                                BOOLEAN())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.NANOSECOND),
                                "EXTRACT(NANOSECOND FROM f0)",
                                123456000L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.NANOSECOND),
                                "EXTRACT(NANOSECOND FROM f1)",
                                987654000L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.NANOSECOND),
                                "EXTRACT(NANOSECOND FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testSqlValidationError(
                                "EXTRACT(NANOSECOND FROM f3)", "NANOSECOND can not be applied")
                        .testResult(
                                $("f4").extract(TimeIntervalUnit.NANOSECOND),
                                "EXTRACT(NANOSECOND FROM f4)",
                                12000000L,
                                BIGINT())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.MICROSECOND),
                                "EXTRACT(MICROSECOND FROM f0)",
                                123456L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.MICROSECOND),
                                "EXTRACT(MICROSECOND FROM f1)",
                                987654L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.MICROSECOND),
                                "EXTRACT(MICROSECOND FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.MILLISECOND),
                                "EXTRACT(MILLISECOND FROM f0)",
                                123L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.MILLISECOND),
                                "EXTRACT(MILLISECOND FROM f1)",
                                987L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.MILLISECOND),
                                "EXTRACT(MILLISECOND FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f4").extract(TimeIntervalUnit.MILLISECOND),
                                "EXTRACT(MILLISECOND FROM f4)",
                                12L,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.SECOND),
                                "EXTRACT(SECOND FROM f0)",
                                33L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.SECOND),
                                "EXTRACT(SECOND FROM f1)",
                                59L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.SECOND),
                                "EXTRACT(SECOND FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.MINUTE),
                                "EXTRACT(MINUTE FROM f0)",
                                22L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.MINUTE),
                                "EXTRACT(MINUTE FROM f1)",
                                56L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.MINUTE),
                                "EXTRACT(MINUTE FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.HOUR),
                                "EXTRACT(HOUR FROM f0)",
                                11L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.HOUR),
                                "EXTRACT(HOUR FROM f1)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.HOUR),
                                "EXTRACT(HOUR FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.DAY),
                                "EXTRACT(DAY FROM f0)",
                                31L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.DAY),
                                "EXTRACT(DAY FROM f1)",
                                29L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.DAY),
                                "EXTRACT(DAY FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f3").extract(TimeIntervalUnit.DAY),
                                "EXTRACT(DAY FROM f3)",
                                14L,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.WEEK),
                                "EXTRACT(WEEK FROM f0)",
                                5L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.WEEK),
                                "EXTRACT(WEEK FROM f1)",
                                9L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.WEEK),
                                "EXTRACT(WEEK FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.MONTH),
                                "EXTRACT(MONTH FROM f0)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.MONTH),
                                "EXTRACT(MONTH FROM f1)",
                                2L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.MONTH),
                                "EXTRACT(MONTH FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.QUARTER),
                                "EXTRACT(QUARTER FROM f0)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.QUARTER),
                                "EXTRACT(QUARTER FROM f1)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.QUARTER),
                                "EXTRACT(QUARTER FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.YEAR),
                                "EXTRACT(YEAR FROM f0)",
                                2000L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.YEAR),
                                "EXTRACT(YEAR FROM f1)",
                                2020L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.YEAR),
                                "EXTRACT(YEAR FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.DECADE),
                                "EXTRACT(DECADE FROM f0)",
                                200L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.DECADE),
                                "EXTRACT(DECADE FROM f1)",
                                202L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.DECADE),
                                "EXTRACT(DECADE FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.CENTURY),
                                "EXTRACT(CENTURY FROM f0)",
                                20L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.CENTURY),
                                "EXTRACT(CENTURY FROM f1)",
                                21L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.CENTURY),
                                "EXTRACT(CENTURY FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                $("f0").extract(TimeIntervalUnit.MILLENNIUM),
                                "EXTRACT(MILLENNIUM FROM f0)",
                                2L,
                                BIGINT().nullable())
                        .testResult(
                                $("f1").extract(TimeIntervalUnit.MILLENNIUM),
                                "EXTRACT(MILLENNIUM FROM f1)",
                                3L,
                                BIGINT().nullable())
                        .testResult(
                                $("f2").extract(TimeIntervalUnit.MILLENNIUM),
                                "EXTRACT(MILLENNIUM FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOW FROM f0)", 2L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOW FROM f1)", 7L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOW FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f0)", 1L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f1)", 6L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOY FROM f0)", 31L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOY FROM f1)", 60L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DOY FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f0)", 2000L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f1)", 2020L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f2)", null, BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.EPOCH, $("f0")),
                                "EXTRACT(EPOCH FROM f0)",
                                949317753L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.EPOCH, $("f1")),
                                "EXTRACT(EPOCH FROM f1)",
                                1582941419L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.EPOCH, $("f2")),
                                "EXTRACT(EPOCH FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testTableApiValidationError(
                                call("EXTRACT", TimeIntervalUnit.EPOCH, $("f5")),
                                "EXTRACT requires 2nd argument to be a temporal type, but type is BOOLEAN")
                        .testTableApiValidationError(
                                call("EXTRACT", JsonExistsOnError.ERROR, $("f2")),
                                "EXTRACT requires 1st argument to be a TimeIntervalUnit literal"));
    }

    private Stream<TestSetSpec> temporalOverlapsTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS)
                        .onFieldsWithData(
                                LocalTime.of(2, 55, 0),
                                Duration.ofHours(1),
                                LocalTime.of(3, 30, 0),
                                Duration.ofHours(2))
                        .andDataTypes(TIME(), INTERVAL(HOUR()), TIME(), INTERVAL(HOUR()))
                        .testResult(
                                temporalOverlaps($("f0"), $("f1"), $("f2"), $("f3")),
                                "(f0, f1) OVERLAPS (f2, f3)",
                                true,
                                BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS)
                        .onFieldsWithData(
                                LocalTime.of(9, 0, 0),
                                LocalTime.of(9, 30, 0),
                                LocalTime.of(9, 29, 0),
                                LocalTime.of(9, 31, 0),
                                LocalTime.of(10, 0, 0),
                                LocalTime.of(10, 15, 0),
                                Duration.ofHours(3))
                        .andDataTypes(
                                TIME(), TIME(), TIME(), TIME(), TIME(), TIME(), INTERVAL(HOUR()))
                        .testResult(
                                temporalOverlaps($("f0"), $("f1"), $("f2"), $("f3")),
                                "(f0, f1) OVERLAPS (f2, f3)",
                                true,
                                BOOLEAN())
                        .testResult(
                                temporalOverlaps($("f0"), $("f4"), $("f5"), $("f6")),
                                "(f0, f4) OVERLAPS (f5, f6)",
                                false,
                                BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS)
                        .onFieldsWithData(
                                LocalDate.of(2011, 3, 10),
                                Duration.ofDays(10),
                                LocalDate.of(2011, 3, 19),
                                Duration.ofDays(10))
                        .andDataTypes(DATE(), INTERVAL(DAY()), DATE(), INTERVAL(DAY()))
                        .testResult(
                                temporalOverlaps($("f0"), $("f1"), $("f2"), $("f3")),
                                "(f0, f1) OVERLAPS (f2, f3)",
                                true,
                                BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS)
                        .onFieldsWithData(
                                LocalDateTime.of(2011, 3, 10, 5, 2, 2),
                                Duration.ofSeconds(0),
                                LocalDateTime.of(2011, 3, 10, 5, 2, 2),
                                LocalDateTime.of(2011, 3, 10, 5, 2, 1),
                                LocalDateTime.of(2011, 3, 10, 5, 2, 2, 1000000),
                                LocalDateTime.of(2011, 3, 10, 5, 2, 2, 2000000))
                        .andDataTypes(
                                TIMESTAMP(),
                                INTERVAL(SECOND()),
                                TIMESTAMP(),
                                TIMESTAMP(),
                                TIMESTAMP(),
                                TIMESTAMP())
                        .testResult(
                                temporalOverlaps($("f0"), $("f1"), $("f2"), $("f3")),
                                "(f0, f1) OVERLAPS (f2, f3)",
                                true,
                                BOOLEAN())
                        .testResult(
                                temporalOverlaps($("f4"), $("f1"), $("f5"), $("f5")),
                                "(f4, f1) OVERLAPS (f5, f5)",
                                false,
                                BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TEMPORAL_OVERLAPS)
                        .onFieldsWithData(
                                1,
                                LocalDateTime.of(2011, 3, 10, 5, 2, 2),
                                LocalDate.of(2011, 3, 10))
                        .andDataTypes(INT(), TIMESTAMP(), DATE())
                        .testTableApiValidationError(
                                temporalOverlaps($("f0"), $("f1"), $("f1"), $("f1")),
                                "TEMPORAL_OVERLAPS requires 1st argument 'leftTimePoint' to be a DATETIME type, but is INT")
                        .testTableApiValidationError(
                                temporalOverlaps($("f1"), $("f1"), $("f0"), $("f1")),
                                "TEMPORAL_OVERLAPS requires 3rd argument 'rightTimePoint' to be a DATETIME type, but is INT")
                        .testTableApiValidationError(
                                temporalOverlaps($("f1"), $("f1"), $("f2"), $("f2")),
                                "TEMPORAL_OVERLAPS requires 'leftTimePoint' and 'rightTimePoint' arguments to be of the same type, but is TIMESTAMP(6) != DATE")
                        .testTableApiValidationError(
                                temporalOverlaps($("f2"), $("f1"), $("f2"), $("f2")),
                                "TEMPORAL_OVERLAPS requires 'leftTemporal' and 'leftTimePoint' arguments to be of the same type if 'leftTemporal' is a DATETIME, but is TIMESTAMP(6) != DATE")
                        .testTableApiValidationError(
                                temporalOverlaps($("f1"), $("f0"), $("f1"), $("f0")),
                                "TEMPORAL_OVERLAPS requires 2nd argument 'leftTemporal' to be DATETIME or INTERVAL type, but is INT"));
    }

    private Stream<TestSetSpec> ceilTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.FLOOR)
                        .onFieldsWithData(
                                // https://issues.apache.org/jira/browse/FLINK-17224
                                // Fractional seconds are lost
                                LocalTime.of(11, 22, 33),
                                LocalDate.of(1990, 10, 14),
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987654321))
                        .andDataTypes(TIME(), DATE(), TIMESTAMP())
                        .testResult(
                                $("f0").ceil(TimeIntervalUnit.MILLISECOND),
                                "CEIL(f0 TO MILLISECOND)",
                                LocalTime.of(11, 22, 33),
                                TIME().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.MILLISECOND),
                                "CEIL(f1 TO MILLISECOND)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.MILLISECOND),
                                "CEIL(f2 TO MILLISECOND)",
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 988_000_000),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").ceil(TimeIntervalUnit.SECOND),
                                "CEIL(f0 TO SECOND)",
                                LocalTime.of(11, 22, 33),
                                TIME().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.SECOND),
                                "CEIL(f1 TO SECOND)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.SECOND),
                                "CEIL(f2 TO SECOND)",
                                LocalDateTime.of(2020, 2, 29, 1, 57),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").ceil(TimeIntervalUnit.MINUTE),
                                "CEIL(f0 TO MINUTE)",
                                LocalTime.of(11, 23),
                                TIME().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.MINUTE),
                                "CEIL(f1 TO MINUTE)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.MINUTE),
                                "CEIL(f2 TO MINUTE)",
                                LocalDateTime.of(2020, 2, 29, 1, 57),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").ceil(TimeIntervalUnit.HOUR),
                                "CEIL(f0 TO HOUR)",
                                LocalTime.of(12, 0),
                                TIME().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.HOUR),
                                "CEIL(f1 TO HOUR)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.HOUR),
                                "CEIL(f2 TO HOUR)",
                                LocalDateTime.of(2020, 2, 29, 2, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.DAY),
                                "CEIL(f1 TO DAY)",
                                LocalDate.of(1990, 10, 15),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.DAY),
                                "CEIL(f2 TO DAY)",
                                LocalDateTime.of(2020, 3, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.WEEK),
                                "CEIL(f1 TO WEEK)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.WEEK),
                                "CEIL(f2 TO WEEK)",
                                LocalDateTime.of(2020, 3, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.MONTH),
                                "CEIL(f1 TO MONTH)",
                                LocalDate.of(1990, 11, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.MONTH),
                                "CEIL(f2 TO MONTH)",
                                LocalDateTime.of(2020, 3, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.QUARTER),
                                "CEIL(f1 TO QUARTER)",
                                LocalDate.of(1991, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.QUARTER),
                                "CEIL(f2 TO QUARTER)",
                                LocalDateTime.of(2020, 4, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.YEAR),
                                "CEIL(f1 TO YEAR)",
                                LocalDate.of(1991, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.YEAR),
                                "CEIL(f2 TO YEAR)",
                                LocalDateTime.of(2021, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.DECADE),
                                "CEIL(f1 TO DECADE)",
                                LocalDate.of(2000, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.DECADE),
                                "CEIL(f2 TO DECADE)",
                                LocalDateTime.of(2030, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.CENTURY),
                                "CEIL(f1 TO CENTURY)",
                                LocalDate.of(2001, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.CENTURY),
                                "CEIL(f2 TO CENTURY)",
                                LocalDateTime.of(2101, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").ceil(TimeIntervalUnit.MILLENNIUM),
                                "CEIL(f1 TO MILLENNIUM)",
                                LocalDate.of(2001, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").ceil(TimeIntervalUnit.MILLENNIUM),
                                "CEIL(f2 TO MILLENNIUM)",
                                LocalDateTime.of(3001, 1, 1, 0, 0),
                                TIMESTAMP().nullable()));
    }

    private Stream<TestSetSpec> floorTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.FLOOR)
                        .onFieldsWithData(
                                // https://issues.apache.org/jira/browse/FLINK-17224
                                // Fractional seconds are lost
                                LocalTime.of(11, 22, 33),
                                LocalDate.of(1990, 10, 14),
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987654321))
                        .andDataTypes(TIME(), DATE(), TIMESTAMP())
                        .testResult(
                                $("f0").floor(TimeIntervalUnit.MILLISECOND),
                                "FLOOR(f0 TO MILLISECOND)",
                                LocalTime.of(11, 22, 33),
                                TIME().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.MILLISECOND),
                                "FLOOR(f1 TO MILLISECOND)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.MILLISECOND),
                                "FLOOR(f2 TO MILLISECOND)",
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987_000_000),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").floor(TimeIntervalUnit.SECOND),
                                "FLOOR(f0 TO SECOND)",
                                LocalTime.of(11, 22, 33),
                                TIME().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.SECOND),
                                "FLOOR(f1 TO SECOND)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.SECOND),
                                "FLOOR(f2 TO SECOND)",
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").floor(TimeIntervalUnit.MINUTE),
                                "FLOOR(f0 TO MINUTE)",
                                LocalTime.of(11, 22),
                                TIME().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.MINUTE),
                                "FLOOR(f1 TO MINUTE)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.MINUTE),
                                "FLOOR(f2 TO MINUTE)",
                                LocalDateTime.of(2020, 2, 29, 1, 56),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f0").floor(TimeIntervalUnit.HOUR),
                                "FLOOR(f0 TO HOUR)",
                                LocalTime.of(11, 0),
                                TIME().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.HOUR),
                                "FLOOR(f1 TO HOUR)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.HOUR),
                                "FLOOR(f2 TO HOUR)",
                                LocalDateTime.of(2020, 2, 29, 1, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.DAY),
                                "FLOOR(f1 TO DAY)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.DAY),
                                "FLOOR(f2 TO DAY)",
                                LocalDateTime.of(2020, 2, 29, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.WEEK),
                                "FLOOR(f1 TO WEEK)",
                                LocalDate.of(1990, 10, 14),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.WEEK),
                                "FLOOR(f2 TO WEEK)",
                                LocalDateTime.of(2020, 2, 23, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.MONTH),
                                "FLOOR(f1 TO MONTH)",
                                LocalDate.of(1990, 10, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.MONTH),
                                "FLOOR(f2 TO MONTH)",
                                LocalDateTime.of(2020, 2, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.QUARTER),
                                "FLOOR(f1 TO QUARTER)",
                                LocalDate.of(1990, 10, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.QUARTER),
                                "FLOOR(f2 TO QUARTER)",
                                LocalDateTime.of(2020, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.YEAR),
                                "FLOOR(f1 TO YEAR)",
                                LocalDate.of(1990, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.YEAR),
                                "FLOOR(f2 TO YEAR)",
                                LocalDateTime.of(2020, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.DECADE),
                                "FLOOR(f1 TO DECADE)",
                                LocalDate.of(1990, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.DECADE),
                                "FLOOR(f2 TO DECADE)",
                                LocalDateTime.of(2020, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.CENTURY),
                                "FLOOR(f1 TO CENTURY)",
                                LocalDate.of(1901, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.CENTURY),
                                "FLOOR(f2 TO CENTURY)",
                                LocalDateTime.of(2001, 1, 1, 0, 0),
                                TIMESTAMP().nullable())
                        .testResult(
                                $("f1").floor(TimeIntervalUnit.MILLENNIUM),
                                "FLOOR(f1 TO MILLENNIUM)",
                                LocalDate.of(1001, 1, 1),
                                DATE().nullable())
                        .testResult(
                                $("f2").floor(TimeIntervalUnit.MILLENNIUM),
                                "FLOOR(f2 TO MILLENNIUM)",
                                LocalDateTime.of(2001, 1, 1, 0, 0),
                                TIMESTAMP().nullable()));
    }
}

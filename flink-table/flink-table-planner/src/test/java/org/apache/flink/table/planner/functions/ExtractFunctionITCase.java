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

import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Test {@link BuiltInFunctionDefinitions#EXTRACT} and its return type. */
class ExtractFunctionITCase extends BuiltInFunctionTestBase {
    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.EXTRACT)
                        .onFieldsWithData(
                                LocalDateTime.of(2000, 1, 31, 11, 22, 33, 123456789),
                                LocalDateTime.of(2020, 2, 29, 1, 56, 59, 987654321),
                                null)
                        .andDataTypes(
                                TIMESTAMP().nullable(),
                                TIMESTAMP().nullable(),
                                TIMESTAMP().nullable())
                        .testSqlResult(
                                "EXTRACT(NANOSECOND FROM f0)", 123456000L, BIGINT().nullable())
                        .testSqlResult(
                                "EXTRACT(NANOSECOND FROM f1)", 987654000L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(NANOSECOND FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MICROSECOND FROM f0)", 123456L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MICROSECOND FROM f1)", 987654L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MICROSECOND FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLISECOND FROM f0)", 123L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLISECOND FROM f1)", 987L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLISECOND FROM f2)", null, BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.SECOND, $("f0")),
                                "EXTRACT(SECOND FROM f0)",
                                33L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.SECOND, $("f1")),
                                "EXTRACT(SECOND FROM f1)",
                                59L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.SECOND, $("f2")),
                                "EXTRACT(SECOND FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MINUTE, $("f0")),
                                "EXTRACT(MINUTE FROM f0)",
                                22L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MINUTE, $("f1")),
                                "EXTRACT(MINUTE FROM f1)",
                                56L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MINUTE, $("f2")),
                                "EXTRACT(MINUTE FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.HOUR, $("f0")),
                                "EXTRACT(HOUR FROM f0)",
                                11L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.HOUR, $("f1")),
                                "EXTRACT(HOUR FROM f1)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.HOUR, $("f2")),
                                "EXTRACT(HOUR FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.DAY, $("f0")),
                                "EXTRACT(DAY FROM f0)",
                                31L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.DAY, $("f1")),
                                "EXTRACT(DAY FROM f1)",
                                29L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.DAY, $("f2")),
                                "EXTRACT(DAY FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.WEEK, $("f0")),
                                "EXTRACT(WEEK FROM f0)",
                                5L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.WEEK, $("f1")),
                                "EXTRACT(WEEK FROM f1)",
                                9L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.WEEK, $("f2")),
                                "EXTRACT(WEEK FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MONTH, $("f0")),
                                "EXTRACT(MONTH FROM f0)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MONTH, $("f1")),
                                "EXTRACT(MONTH FROM f1)",
                                2L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.MONTH, $("f2")),
                                "EXTRACT(MONTH FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.QUARTER, $("f0")),
                                "EXTRACT(QUARTER FROM f0)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.QUARTER, $("f1")),
                                "EXTRACT(QUARTER FROM f1)",
                                1L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.QUARTER, $("f2")),
                                "EXTRACT(QUARTER FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.YEAR, $("f0")),
                                "EXTRACT(YEAR FROM f0)",
                                2000L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.YEAR, $("f1")),
                                "EXTRACT(YEAR FROM f1)",
                                2020L,
                                BIGINT().nullable())
                        .testResult(
                                call("EXTRACT", TimeIntervalUnit.YEAR, $("f2")),
                                "EXTRACT(YEAR FROM f2)",
                                null,
                                BIGINT().nullable())
                        .testSqlResult("EXTRACT(DECADE FROM f0)", 200L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DECADE FROM f1)", 202L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(DECADE FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(CENTURY FROM f0)", 20L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(CENTURY FROM f1)", 21L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(CENTURY FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLENNIUM FROM f0)", 2L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLENNIUM FROM f1)", 3L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(MILLENNIUM FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f0)", 1L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f1)", 6L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISODOW FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f0)", 2000L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f1)", 2020L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(ISOYEAR FROM f2)", null, BIGINT().nullable())
                        .testSqlResult("EXTRACT(EPOCH FROM f0)", 949317753L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(EPOCH FROM f1)", 1582941419L, BIGINT().nullable())
                        .testSqlResult("EXTRACT(EPOCH FROM f2)", null, BIGINT().nullable()));
    }
}

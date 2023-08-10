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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import java.time.LocalDate;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.generateSeries;
import static org.apache.flink.table.api.Expressions.row;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(arrayContainsTestCases(), generateSeriesTestCases()).flatMap(s -> s);
    }

    private Stream<TestSetSpec> arrayContainsTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_CONTAINS)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                null,
                                new String[] {"Hello", "World"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {1, null, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()))
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayContains(2),
                                "ARRAY_CONTAINS(f0, 2)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f0").arrayContains(42),
                                "ARRAY_CONTAINS(f0, 42)",
                                false,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<INT> of null value
                        .testResult(
                                $("f1").arrayContains(12),
                                "ARRAY_CONTAINS(f1, 12)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f1").arrayContains(null),
                                "ARRAY_CONTAINS(f1, NULL)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<STRING> NOT NULL
                        .testResult(
                                $("f2").arrayContains("Hello"),
                                "ARRAY_CONTAINS(f2, 'Hello')",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f3").arrayContains(row(true, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (TRUE, DATE '1990-10-14'))",
                                true,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(row(false, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (FALSE, DATE '1990-10-14'))",
                                false,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(null),
                                "ARRAY_CONTAINS(f3, null)",
                                true,
                                DataTypes.BOOLEAN())
                        // ARRAY<INT> with null elements
                        .testResult(
                                $("f4").arrayContains(null),
                                "ARRAY_CONTAINS(f4, NULL)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_CONTAINS(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)")
                        .testTableApiValidationError(
                                $("f0").arrayContains(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)"));
    }

    private Stream<TestSetSpec> generateSeriesTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.GENERATE_SERIES)
                        .onFieldsWithData(1, 2, 5, 9, 10, 0, -3, -10, 3.0)
                        .andDataTypes(
                                DataTypes.BIGINT(),
                                DataTypes.BIGINT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.DOUBLE())
                        .testResult(
                                generateSeries($("f6"), $("f7")),
                                "GENERATE_SERIES(f6, f7)",
                                new Long[] {},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f6"), $("f7"), -2),
                                "GENERATE_SERIES(f6, f7, -2)",
                                new Long[] {-3L, -5L, -7L, -9L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f0"), $("f1")),
                                "GENERATE_SERIES(f0, f1)",
                                new Long[] {1L, 2L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f0"), $("f2")),
                                "GENERATE_SERIES(f0, f2)",
                                new Long[] {1L, 2L, 3L, 4L, 5L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f2"), $("f4")),
                                "GENERATE_SERIES(f2, f4)",
                                new Long[] {5L, 6L, 7L, 8L, 9L, 10L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f0"), $("f4"), 2),
                                "GENERATE_SERIES(f0, f4, 2)",
                                new Long[] {1L, 3L, 5L, 7L, 9L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f0"), $("f4"), -2),
                                "GENERATE_SERIES(f0, f4, -2)",
                                new Long[] {},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testResult(
                                generateSeries($("f4"), $("f2"), -2),
                                "GENERATE_SERIES(f4, f2, -2)",
                                new Long[] {10L, 8L, 6L},
                                DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testTableApiRuntimeError(
                                generateSeries($("f0"), $("f1"), $("f5")),
                                "Step argument in GENERATE_SERIES function cannot be zero.")
                        .testSqlRuntimeError(
                                "GENERATE_SERIES(f0, f1, f5)",
                                "Step argument in GENERATE_SERIES function cannot be zero.")
                        .testSqlValidationError(
                                "GENERATE_SERIES(f0)",
                                "No match found for function signature GENERATE_SERIES(<NUMERIC>)")
                        .testSqlValidationError(
                                "GENERATE_SERIES(f0, f1, f2, f3)",
                                "No match found for function signature GENERATE_SERIES(<NUMERIC>, <NUMERIC>, <NUMERIC>, <NUMERIC>)")
                        .testSqlValidationError(
                                "GENERATE_SERIES()",
                                "No match found for function signature GENERATE_SERIES()")
                        .testTableApiValidationError(
                                generateSeries($("f6"), $("f7"), $("f8")),
                                "Invalid function call:\n" + "GENERATE_SERIES(INT, INT, DOUBLE)")
                        .testSqlValidationError(
                                "GENERATE_SERIES(f6, f7, f8)",
                                " Invalid function call:\n" + "GENERATE_SERIES(INT, INT, DOUBLE)"));
    }
}

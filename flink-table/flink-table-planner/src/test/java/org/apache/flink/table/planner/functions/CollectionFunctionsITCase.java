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
import static org.apache.flink.table.api.Expressions.row;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(arrayContainsTestCases(), arraySliceTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> arraySliceTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_SLICE)
                        .onFieldsWithData(
                                new Integer[] {null, 1, 2, 3, 4, 5, 6, null},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new String[] {"a", "b", "c", "d", "e"},
                                new Integer[] {1, 2, 3, 4, 5})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-123),
                                "ARRAY_SLICE(f4, -123)",
                                new Integer[] {1, 2, 3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(0),
                                "ARRAY_SLICE(f4, 0)",
                                new Integer[] {1, 2, 3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-3),
                                "ARRAY_SLICE(f4, -3)",
                                new Integer[] {3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(9),
                                "ARRAY_SLICE(f4, 9)",
                                new Integer[] {},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-123, -231),
                                "ARRAY_SLICE(f4, -123, -231)",
                                new Integer[] {},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-5, -5),
                                "ARRAY_SLICE(f4, -5, -5)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-6, -5),
                                "ARRAY_SLICE(f4, -6, -5)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(5, 6),
                                "ARRAY_SLICE(f4, 5, 6)",
                                new Integer[] {5},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(20, 30),
                                "ARRAY_SLICE(f4, 20, 30)",
                                new Integer[] {},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f4").arraySlice(-123, 123),
                                "ARRAY_SLICE(f4, -123, 123)",
                                new Integer[] {1, 2, 3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(0, 8),
                                "ARRAY_SLICE(f0, 0, 8)",
                                new Integer[] {null, 1, 2, 3, 4, 5, 6, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(0, 9),
                                "ARRAY_SLICE(f0, 0, 9)",
                                new Integer[] {null, 1, 2, 3, 4, 5, 6, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(0, -1),
                                "ARRAY_SLICE(f0, 0, -1)",
                                new Integer[] {null, 1, 2, 3, 4, 5, 6, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(1, 0),
                                "ARRAY_SLICE(f0, 1, 0)",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(-1, 15),
                                "ARRAY_SLICE(f0, -1, 15)",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(8, 15),
                                "ARRAY_SLICE(f0, 8, 15)",
                                new Integer[] {null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(null, 15),
                                "ARRAY_SLICE(f0, null, 15)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(1, null),
                                "ARRAY_SLICE(f0, 1, null)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arraySlice(null, null),
                                "ARRAY_SLICE(f0, null, null)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f1").arraySlice(1, 3),
                                "ARRAY_SLICE(f1, 1, 3)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f2").arraySlice(1, 1),
                                "ARRAY_SLICE(f2, 1, 1)",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        .testSqlValidationError(
                                "ARRAY_SLICE(f3, TRUE, 2.5)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_SLICE(<ARRAY>, <INTEGER>, <INTEGER>)")
                        .testTableApiValidationError(
                                $("f3").arraySlice(true, 2.5),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_SLICE(<ARRAY>, <INTEGER>, <INTEGER>)")
                        .testSqlValidationError(
                                "ARRAY_SLICE()",
                                " No match found for function signature ARRAY_SLICE()")
                        .testSqlValidationError("ARRAY_SLICE(null)", "Illegal use of 'NULL'"));
    }
}

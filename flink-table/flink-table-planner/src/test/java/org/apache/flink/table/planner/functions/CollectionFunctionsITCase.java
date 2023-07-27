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
import org.apache.flink.util.CollectionUtil;

import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.util.CollectionUtil.entry;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(arrayContainsTestCases(), arrayExceptTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> arrayExceptTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_EXCEPT)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 2},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {null, null, 1},
                                new Integer[][] {
                                    new Integer[] {1, null, 3}, new Integer[] {0}, new Integer[] {1}
                                },
                                new Map[] {
                                    CollectionUtil.map(entry(1, "a"), entry(2, "b")),
                                    CollectionUtil.map(entry(3, "c"), entry(4, "d")),
                                    null
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayExcept(new Integer[] {1, null, 4}),
                                "ARRAY_EXCEPT(f0, ARRAY[1, NULL, 4])",
                                new Integer[] {2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f0").arrayExcept(new Integer[] {1}),
                                "ARRAY_EXCEPT(f0, ARRAY[1])",
                                new Integer[] {2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f0").arrayExcept(new Integer[] {42}),
                                "ARRAY_EXCEPT(f0, ARRAY[42])",
                                new Integer[] {1, 2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // arrayTwo is NULL
                        .testResult(
                                $("f0").arrayExcept(
                                                lit(null, DataTypes.ARRAY(DataTypes.INT()))
                                                        .cast(DataTypes.ARRAY(DataTypes.INT()))),
                                "ARRAY_EXCEPT(f0, CAST(NULL AS ARRAY<INT>))",
                                new Integer[] {1, 2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // arrayTwo contains null elements
                        .testResult(
                                $("f0").arrayExcept(new Integer[] {null, 2}),
                                "ARRAY_EXCEPT(f0, ARRAY[null, 2])",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // arrayOne is NULL
                        .testResult(
                                $("f1").arrayExcept(new Integer[] {1, 2, 3}),
                                "ARRAY_EXCEPT(f1, ARRAY[1,2,3])",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // arrayOne contains null elements
                        .testResult(
                                $("f3").arrayExcept(new Integer[] {null, 42}),
                                "ARRAY_EXCEPT(f3, ARRAY[null, 42])",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f2").arrayExcept(
                                                new Row[] {
                                                    Row.of(true, LocalDate.of(1990, 10, 14))
                                                }),
                                "ARRAY_EXCEPT(f2, ARRAY[(TRUE, DATE '1990-10-14')])",
                                new Row[] {Row.of(true, LocalDate.of(2022, 4, 20)), null},
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.BOOLEAN(), DataTypes.DATE()))
                                        .nullable())
                        .testResult(
                                $("f2").arrayExcept(
                                                lit(
                                                                null,
                                                                DataTypes.ARRAY(
                                                                        DataTypes.ROW(
                                                                                DataTypes.BOOLEAN(),
                                                                                DataTypes.DATE())))
                                                        .cast(
                                                                DataTypes.ARRAY(
                                                                        DataTypes.ROW(
                                                                                DataTypes.BOOLEAN(),
                                                                                DataTypes
                                                                                        .DATE())))),
                                "ARRAY_EXCEPT(f2, CAST(NULL AS ARRAY<ROW<col1 BOOLEAN, col2 DATE>>))",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null,
                                },
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.BOOLEAN(), DataTypes.DATE()))
                                        .nullable())
                        // ARRAY<ARRAY<INT>>
                        .testResult(
                                $("f4").arrayExcept(new Integer[][] {new Integer[] {0}}),
                                "ARRAY_EXCEPT(f4, ARRAY[ARRAY[0]])",
                                new Integer[][] {new Integer[] {1, null, 3}, new Integer[] {1}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).nullable()))
                        // ARRAY<MAP<INT, STRING>> with NULL elements
                        .testResult(
                                $("f5").arrayExcept(
                                                new Map[] {
                                                    CollectionUtil.map(entry(3, "c"), entry(4, "d"))
                                                }),
                                "ARRAY_EXCEPT(f5, ARRAY[MAP[3, 'c', 4, 'd']])",
                                new Map[] {CollectionUtil.map(entry(1, "a"), entry(2, "b")), null},
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                                        .nullable())
                        // Invalid signatures
                        .testSqlValidationError(
                                "ARRAY_EXCEPT(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_EXCEPT(<COMMON>, <COMMON>)")
                        .testTableApiValidationError(
                                $("f0").arrayExcept(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_EXCEPT(<COMMON>, <COMMON>)")
                        .testSqlValidationError(
                                "ARRAY_EXCEPT(f0, ARRAY['hi', 'there'])",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_EXCEPT(<COMMON>, <COMMON>)")
                        .testTableApiValidationError(
                                $("f0").arrayExcept(new String[] {"hi", "there"}),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_EXCEPT(<COMMON>, <COMMON>)"));
    }
}

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

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.util.CollectionUtil.entry;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(arrayContainsTestCases(), arrayMinTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> arrayMinTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_MIN)
                        .onFieldsWithData(
                                new Integer[] {1, 2, null},
                                null,
                                new Double[] {1.2, null, 3.4, 8.0},
                                new String[] {"a", null, "bc", "d", "def"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Map[] {
                                    CollectionUtil.map(entry(1, "a"), entry(2, "b")),
                                    CollectionUtil.map(entry(3, "c"), entry(4, "d")),
                                    null
                                },
                                new Integer[][] {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, null},
                                new Row[] {
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Boolean[] {true, false, true, false, true, null},
                                new Row[] {
                                    Row.of(true),
                                    Row.of(false),
                                    Row.of(true),
                                    Row.of(false),
                                    Row.of(true),
                                    Row.of(false),
                                    null
                                },
                                new Row[] {
                                    Row.of(1), Row.of(2), Row.of(8), Row.of(4), Row.of(5),
                                    Row.of(8), null
                                },
                                1,
                                new Integer[][] {{1, 2}, {2, 3}, null},
                                new LocalDate[] {
                                    LocalDate.of(2022, 1, 2),
                                    LocalDate.of(2023, 4, 21),
                                    LocalDate.of(2022, 12, 24),
                                    LocalDate.of(2026, 2, 10),
                                    LocalDate.of(2012, 5, 16),
                                    LocalDate.of(2092, 7, 19)
                                },
                                null)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.DOUBLE()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.BOOLEAN()),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.BOOLEAN())),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT())),
                                DataTypes.INT().notNull(),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.DATE()),
                                DataTypes.ARRAY(DataTypes.INT().notNull()))
                        .testResult($("f0").arrayMin(), "ARRAY_MIN(f0)", 1, DataTypes.INT())
                        .testResult($("f1").arrayMin(), "ARRAY_MIN(f1)", null, DataTypes.INT())
                        .testResult($("f2").arrayMin(), "ARRAY_MIN(f2)", 1.2, DataTypes.DOUBLE())
                        .testResult($("f3").arrayMin(), "ARRAY_MIN(f3)", "a", DataTypes.STRING())
                        .testResult($("f14").arrayMin(), "ARRAY_MIN(f14)", null, DataTypes.INT())
                        .testResult(
                                $("f13").arrayMin(),
                                "ARRAY_MIN(f13)",
                                LocalDate.of(2012, 5, 16),
                                DataTypes.DATE())
                        .testSqlValidationError(
                                "ARRAY_MIN(f4)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ROW<`f0` BOOLEAN, `f1` DATE>>")
                        .testTableApiValidationError(
                                $("f4").arrayMin(),
                                "Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ROW<`f0` BOOLEAN, `f1` DATE>>")
                        .testSqlValidationError(
                                "ARRAY_MIN(f5)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<MAP<INT, STRING>>")
                        .testTableApiValidationError(
                                $("f5").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<MAP<INT, STRING>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f6)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ARRAY<INT>>)")
                        .testTableApiValidationError(
                                $("f6").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<ARRAY<INT>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f7)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ROW<`f0` DATE>>)")
                        .testTableApiValidationError(
                                $("f7").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<ROW<`f0` DATE>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f8)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<BOOLEAN>)")
                        .testTableApiValidationError(
                                $("f8").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<BOOLEAN>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f9)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ROW<`f0` BOOLEAN>>)")
                        .testTableApiValidationError(
                                $("f9").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<ROW<`f0` BOOLEAN>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f10)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ROW<`f0` INT>>)")
                        .testTableApiValidationError(
                                $("f10").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<ROW<`f0` INT>>)")
                        .testTableApiValidationError(
                                $("f11").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(INT NOT NULL)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f11)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(INT NOT NULL)")
                        .testTableApiValidationError(
                                $("f12").arrayMin(),
                                "Invalid function call:\n" + "ARRAY_MIN(ARRAY<ARRAY<INT>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN(f12)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MIN(ARRAY<ARRAY<INT>>)")
                        .testSqlValidationError(
                                "ARRAY_MIN()", "No match found for function signature ARRAY_MIN()")
                        .testSqlValidationError(
                                "ARRAY_MIN(ARRAY[1], ARRAY[2])",
                                "No match found for function signature ARRAY_MIN(<INTEGER ARRAY>, <INTEGER ARRAY>)")
                        .withFunction(CreateEmptyArray.class)
                        .testResult(
                                call("CreateEmptyArray").arrayMin(),
                                "ARRAY_MIN(CreateEmptyArray())",
                                null,
                                DataTypes.INT()));
    }

    public static class CreateEmptyArray extends ScalarFunction {
        public @DataTypeHint("ARRAY<INT NOT NULL>") int[] eval() {
            return new int[] {};
        }
    }
}

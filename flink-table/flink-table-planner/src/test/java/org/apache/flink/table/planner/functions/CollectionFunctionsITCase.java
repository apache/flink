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
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.row;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(arrayContainsTestCases(), arrayJoinTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> arrayJoinTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_JOIN)
                        .onFieldsWithData(
                                new String[] {"abv", "bbb", "cb"},
                                new String[] {"a", "b", null},
                                new String[] {null, "1", null},
                                new String[] {null, null, "1", null, null},
                                null,
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {1, 2, 3, null},
                                new Boolean[] {null, false, true},
                                new Double[] {1.2, 34.0, 4.0, 4.5},
                                1)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.BOOLEAN()),
                                DataTypes.ARRAY(DataTypes.DOUBLE()).notNull(),
                                DataTypes.INT().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), null),
                                "ARRAY_JOIN(f0, null)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), "+", null),
                                "ARRAY_JOIN(f0, '+', null)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), null, null),
                                "ARRAY_JOIN(f0, null, null)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), "+"),
                                "ARRAY_JOIN(f0, '+')",
                                "abv+bbb+cb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), "+", "abc"),
                                "ARRAY_JOIN(f0, '+', 'abc')",
                                "abv+bbb+cb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), " "),
                                "ARRAY_JOIN(f0, ' ')",
                                "abv bbb cb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), ""),
                                "ARRAY_JOIN(f0, '')",
                                "abvbbbcb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), " ", ""),
                                "ARRAY_JOIN(f0, ' ', '')",
                                "abv bbb cb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), " ", " "),
                                "ARRAY_JOIN(f0, ' ', ' ')",
                                "abv bbb cb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), "", ""),
                                "ARRAY_JOIN(f0, '', '')",
                                "abvbbbcb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f0"), "", " "),
                                "ARRAY_JOIN(f0, '', ' ')",
                                "abvbbbcb",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "+", "abc"),
                                "ARRAY_JOIN(f1, '+', 'abc')",
                                "a+b+abc",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "+", ""),
                                "ARRAY_JOIN(f1, '+', '')",
                                "a+b+",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "+"),
                                "ARRAY_JOIN(f1, '+')",
                                "a+b",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "+", " "),
                                "ARRAY_JOIN(f1, '+', ' ')",
                                "a+b+ ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "", "+"),
                                "ARRAY_JOIN(f1, '', '+')",
                                "ab+",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), "", ""),
                                "ARRAY_JOIN(f1, '', '')",
                                "ab",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), " ", ""),
                                "ARRAY_JOIN(f1, ' ', '')",
                                "a b ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f1"), " ", " "),
                                "ARRAY_JOIN(f1, ' ', ' ')",
                                "a b  ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "+", "abc"),
                                "ARRAY_JOIN(f2, '+', 'abc')",
                                "abc+1+abc",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "+"),
                                "ARRAY_JOIN(f2, '+')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), ""),
                                "ARRAY_JOIN(f2, '')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), " "),
                                "ARRAY_JOIN(f2, ' ')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "+", ""),
                                "ARRAY_JOIN(f2, '+', '')",
                                "+1+",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "+", " "),
                                "ARRAY_JOIN(f2, '+', ' ')",
                                " +1+ ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "", ""),
                                "ARRAY_JOIN(f2, '', '')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), "", " "),
                                "ARRAY_JOIN(f2, '', ' ')",
                                " 1 ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), " ", " "),
                                "ARRAY_JOIN(f2, ' ', ' ')",
                                "  1  ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f2"), " ", ""),
                                "ARRAY_JOIN(f2, ' ', '')",
                                " 1 ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "+", "abc"),
                                "ARRAY_JOIN(f3, '+', 'abc')",
                                "abc+abc+1+abc+abc",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "+"),
                                "ARRAY_JOIN(f3, '+')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), ""),
                                "ARRAY_JOIN(f3, '')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), " "),
                                "ARRAY_JOIN(f3, ' ')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "+", ""),
                                "ARRAY_JOIN(f3, '+', '')",
                                "++1++",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "+", " "),
                                "ARRAY_JOIN(f3, '+', ' ')",
                                " + +1+ + ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "", ""),
                                "ARRAY_JOIN(f3, '', '')",
                                "1",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), "", " "),
                                "ARRAY_JOIN(f3, '', ' ')",
                                "  1  ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), " ", " "),
                                "ARRAY_JOIN(f3, ' ', ' ')",
                                "    1    ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f3"), " ", ""),
                                "ARRAY_JOIN(f3, ' ', '')",
                                "  1  ",
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("ARRAY_JOIN", $("f4"), " ", ""),
                                "ARRAY_JOIN(f4, ' ', '')",
                                null,
                                DataTypes.STRING().nullable())
                        .testSqlValidationError(
                                "ARRAY_JOIN(f0)",
                                "No match found for function "
                                        + "signature ARRAY_JOIN(<VARCHAR(2147483647) ARRAY>)")
                        .testSqlValidationError(
                                "ARRAY_JOIN()",
                                "No match found for function signature ARRAY_JOIN()")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f5, '+')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f5"), "+"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f6, '+')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f6"), "+", "abc"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f7, '+', 'abc')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f7"), "+"),
                                "Invalid function call:\n"
                                        + "ARRAY_JOIN(ARRAY<INT>, CHAR(1) NOT NULL)")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f8, '+', 'abc')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f8"), "+"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f9, '+', 'abc')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f9"), "+"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "ARRAY_JOIN(f10, '+', 'abc')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f10"), "+"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testTableApiValidationError(
                                call("ARRAY_JOIN", $("f0"), "+", "+", "+"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>)\n"
                                        + "ARRAY_JOIN(ARRAY<STRING>, <CHARACTER_STRING>, <CHARACTER_STRING>)"));
    }
}

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
        return Stream.of(arrayContainsTestCases(), splitTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> splitTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.SPLIT)
                        .onFieldsWithData(
                                "123,123,23",
                                null,
                                ",123,123",
                                ",123,123,",
                                123,
                                "12345",
                                ",123,,,123,")
                        .andDataTypes(
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull())
                        .testResult(
                                $("f0").split(","),
                                "SPLIT(f0, ',')",
                                new String[] {"123", "123", "23"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f0").split(null),
                                "SPLIT(f0, NULL)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f0").split(""),
                                "SPLIT(f0, '')",
                                new String[] {"1", "2", "3", ",", "1", "2", "3", ",", "2", "3"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").split(","),
                                "SPLIT(f1, ',')",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").split(null),
                                "SPLIT(f1, null)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f2").split(","),
                                "SPLIT(f2, ',')",
                                new String[] {"", "123", "123"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f3").split(","),
                                "SPLIT(f3, ',')",
                                new String[] {"", "123", "123", ""},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f5").split(","),
                                "SPLIT(f5, ',')",
                                new String[] {"12345"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f6").split(","),
                                "SPLIT(f6, ',')",
                                new String[] {"", "123", "", "", "123", ""},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testTableApiValidationError(
                                $("f4").split(","),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SPLIT(<CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "SPLIT(f4, ',')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SPLIT(<CHARACTER_STRING>, <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "SPLIT()", "No match found for function signature SPLIT()")
                        .testSqlValidationError(
                                "SPLIT(f1, '1', '2')",
                                "No match found for function signature SPLIT(<CHARACTER>, <CHARACTER>, <CHARACTER>)"));
    }
}

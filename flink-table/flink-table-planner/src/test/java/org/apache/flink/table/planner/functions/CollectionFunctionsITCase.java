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
        return Stream.of(arrayContainsTestCases(), fieldTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> fieldTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.FIELD)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 2, null},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                1,
                                2.1,
                                "3")
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.INT().notNull(),
                                DataTypes.DOUBLE().notNull(),
                                DataTypes.STRING().notNull())
                        .testResult($("f0").field(), "FIELD(f0)", 0, DataTypes.INT().nullable())
                        .testResult(
                                $("f0").field(new Integer[] {1, 2, 2, null}, 1, 2.0, "3"),
                                "FIELD(f0, ARRAY[1,2,2,null], 1, 2.0, '3')",
                                1,
                                DataTypes.INT().nullable())
                        .testResult($("f0").field(), "FIELD(f0)", 0, DataTypes.INT())
                        .testResult(
                                $("f0").field((Integer) null),
                                "FIELD(f0, null)",
                                0,
                                DataTypes.INT().nullable())
                        .testResult(
                                $("f1").field(new Integer[] {1, 2, 2, null}, 1, 2.0, "3"),
                                "FIELD(f1, ARRAY[1,2,2,null], 1, 2.0, '3')",
                                0,
                                DataTypes.INT().nullable())
                        .testResult(
                                $("f2").field(new Integer[] {1, 2, 2, null}, 1, 2.0, "3"),
                                "FIELD(f2, ARRAY[1,2,2,null], 1, 2.0, '3')",
                                0,
                                DataTypes.INT().nullable())
                        .testResult(
                                $("f2").field(
                                                new Integer[] {1, 2, 2, null},
                                                1,
                                                2.0,
                                                "3",
                                                new Row[] {
                                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                                    null
                                                }),
                                "FIELD(f2, ARRAY[1,2,2,null], 1, 2.0, '3', ARRAY[(TRUE, DATE '2022-4-20'), (TRUE, DATE '1990-10-14'), NULL])",
                                5,
                                DataTypes.INT())
                        .testResult(
                                $("f3").field(new Integer[] {1, 2, 2, null}, 1, 2.0, "3"),
                                "FIELD(f3, ARRAY[1,2,2,null], 1, 2.0, '3')",
                                2,
                                DataTypes.INT().notNull())
                        .testResult(
                                $("f4").field(new Integer[] {1, 2, 2, null}, 1, 2.1, "3"),
                                "FIELD(f4, ARRAY[1,2,2,null], 1, 2.1E0, '3')",
                                3,
                                DataTypes.INT().notNull())
                        .testResult(
                                $("f5").field(new Integer[] {1, 2, 2, null}, 1, 2.0, "3"),
                                "FIELD(f5, ARRAY[1,2,2,null], 1, 2.0, '3')",
                                4,
                                DataTypes.INT().notNull())
                        .testSqlValidationError(
                                "FIELD()", "No match found for function signature FIELD()"));
    }
}

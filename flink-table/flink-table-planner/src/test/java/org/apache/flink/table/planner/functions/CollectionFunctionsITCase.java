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
        return Stream.of(arrayContainsTestCases(), eltTestCases()).flatMap(s -> s);
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

    private Stream<TestSetSpec> eltTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ELT)
                        .onFieldsWithData(
                                Row.of(1, 2, "123", 56, 1.2, null, Row.of(1, 2, 3)),
                                null,
                                new Integer[] {1, 2, 3})
                        .andDataTypes(
                                DataTypes.ROW(
                                        DataTypes.INT(),
                                        DataTypes.INT(),
                                        DataTypes.STRING(),
                                        DataTypes.INT(),
                                        DataTypes.DOUBLE(),
                                        DataTypes.INT(),
                                        DataTypes.ROW(
                                                DataTypes.INT(), DataTypes.INT(), DataTypes.INT())),
                                DataTypes.ROW(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult($("f0").elt(null), "ELT(f0, null)", null, DataTypes.INT())
                        .testResult($("f0").elt(1), "ELT(f0, 1)", 1, DataTypes.INT())
                        .testResult($("f1").elt(1), "ELT(f1, 1)", null, DataTypes.INT())
                        .testResult($("f0").elt(2), "ELT(f0, 2)", 2, DataTypes.INT())
                        .testResult($("f0").elt(3), "ELT(f0, 3)", "123", DataTypes.STRING())
                        .testResult($("f0").elt(4), "ELT(f0, 4)", 56, DataTypes.INT())
                        .testResult($("f0").elt(5), "ELT(f0, 5)", 1.2, DataTypes.DOUBLE())
                        .testResult($("f0").elt(6), "ELT(f0, 6)", null, DataTypes.INT())
                        .testResult(
                                $("f0").elt(7),
                                "ELT(f0, 7)",
                                Row.of(1, 2, 3),
                                DataTypes.ROW(DataTypes.INT(), DataTypes.INT(), DataTypes.INT()))
                        .testSqlValidationError(
                                "elt(f0, 0)",
                                "the input should not smaller than 1 and larger than 7")
                        .testTableApiValidationError(
                                $("f0").elt(0),
                                "the input should not smaller than 1 and larger than 7")
                        .testSqlValidationError(
                                "elt(f0, 8)",
                                "the input should not smaller than 1 and larger than 7")
                        .testTableApiValidationError(
                                $("f0").elt(8),
                                "the input should not smaller than 1 and larger than 7")
                        .testSqlValidationError(
                                "ELT(f0, true)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ELT(ROW<`f0` INT, `f1` INT, `f2` STRING, `f3` INT, `f4` DOUBLE, `f5` INT, `f6` ROW<`f0` INT, `f1` INT, `f2` INT>>, BOOLEAN NOT NULL)")
                        .testTableApiValidationError(
                                $("f0").elt(true),
                                "Invalid function call:\n"
                                        + "ELT(ROW<`f0` INT, `f1` INT, `f2` STRING, `f3` INT, `f4` DOUBLE, `f5` INT, `f6` ROW<`f0` INT, `f1` INT, `f2` INT>>, BOOLEAN NOT NULL)")
                        .testSqlValidationError(
                                "ELT(f0, true, 'abc')",
                                "No match found for function signature ELT(<RecordType:peek_no_expand(INTEGER f0, INTEGER f1, VARCHAR(2147483647) f2, INTEGER f3, DOUBLE f4, INTEGER f5, RecordType:peek_no_expand(INTEGER f0, INTEGER f1, INTEGER f2) f6)>, <BOOLEAN>, <CHARACTER>)")
                        .testSqlValidationError(
                                "ELT(f0)",
                                "No match found for function signature ELT(<RecordType:peek_no_expand(INTEGER f0, INTEGER f1, VARCHAR(2147483647) f2, INTEGER f3, DOUBLE f4, INTEGER f5, RecordType:peek_no_expand(INTEGER f0, INTEGER f1, INTEGER f2) f6)>)")
                        .testSqlValidationError(
                                "ELT()", "No match found for function signature ELT()")
                        .testSqlValidationError("ELT(null, 'abc')", "Illegal use of 'NULL'"));
    }
}

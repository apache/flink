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

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Test String functions correct behaviour. */
class StringFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(regexpExtractTestCases(), substringIndexTestCases()).flatMap(s -> s);
    }

    private Stream<TestSetSpec> regexpExtractTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.REGEXP_EXTRACT, "Check return type")
                        .onFieldsWithData("22", "ABC")
                        .testResult(
                                call("regexpExtract", $("f0"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f0,'[A-Z]+')",
                                null,
                                DataTypes.STRING().nullable())
                        .testResult(
                                call("regexpExtract", $("f1"), "[A-Z]+"),
                                "REGEXP_EXTRACT(f1, '[A-Z]+')",
                                "ABC",
                                DataTypes.STRING().nullable()));
    }

    private Stream<TestSetSpec> substringIndexTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.SUBSTRING_INDEX)
                        .onFieldsWithData(
                                null,
                                "www.apache.org",
                                "",
                                null,
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                new byte[] {3},
                                new byte[] {3, 3},
                                new byte[] {},
                                new byte[] {6})
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES())
                        // StringData
                        // expr NULL
                        .testResult(
                                $("f0").substringIndex(".", 0),
                                "SUBSTRING_INDEX(f0, '.', 0)",
                                null,
                                DataTypes.STRING().nullable())
                        // delim NULL
                        .testResult(
                                $("f1").substringIndex(null, 0),
                                "SUBSTRING_INDEX(f1, NULL, 0)",
                                null,
                                DataTypes.STRING().nullable())
                        // count NULL
                        .testResult(
                                $("f1").substringIndex(".", null),
                                "SUBSTRING_INDEX(f1, '.', NULL)",
                                null,
                                DataTypes.STRING().nullable())
                        // expr empty
                        .testResult(
                                $("f2").substringIndex(".", 1),
                                "SUBSTRING_INDEX(f2, '.', 1)",
                                "",
                                DataTypes.STRING().nullable())
                        // delim empty
                        .testResult(
                                $("f1").substringIndex($("f2"), 1),
                                "SUBSTRING_INDEX(f1, '', 1)",
                                "",
                                DataTypes.STRING().nullable())
                        // count 0
                        .testResult(
                                $("f1").substringIndex(".", 0),
                                "SUBSTRING_INDEX(f1, '.', 0)",
                                "",
                                DataTypes.STRING().nullable())
                        // delim does not exist in expr
                        .testResult(
                                $("f1").substringIndex("..", 1),
                                "SUBSTRING_INDEX(f1, '..', 1)",
                                "www.apache.org",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex("x", 1),
                                "SUBSTRING_INDEX(f1, 'x', 1)",
                                "www.apache.org",
                                DataTypes.STRING().nullable())
                        // delim length > 1
                        .testResult(
                                $("f1").substringIndex("ww", 1),
                                "SUBSTRING_INDEX(f1, 'ww', 1)",
                                "",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex("ww", 2),
                                "SUBSTRING_INDEX(f1, 'ww', 2)",
                                "w",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex("ww", -1),
                                "SUBSTRING_INDEX(f1, 'ww', -1)",
                                ".apache.org",
                                DataTypes.STRING().nullable())
                        // normal cases
                        .testResult(
                                $("f1").substringIndex(".", -3),
                                "SUBSTRING_INDEX(f1, '.', -3)",
                                "www.apache.org",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex(".", -2),
                                "SUBSTRING_INDEX(f1, '.', -2)",
                                "apache.org",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex("g", -1),
                                "SUBSTRING_INDEX(f1, 'g', -1)",
                                "",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex(".", 3),
                                "SUBSTRING_INDEX(f1, '.', 3)",
                                "www.apache.org",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex(".", 2),
                                "SUBSTRING_INDEX(f1, '.', 2)",
                                "www.apache",
                                DataTypes.STRING().nullable())
                        .testResult(
                                $("f1").substringIndex("w", 1),
                                "SUBSTRING_INDEX(f1, 'w', 1)",
                                "",
                                DataTypes.STRING().nullable())
                        .testSqlValidationError(
                                "SUBSTRING_INDEX(f1, '.', '')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)")
                        .testTableApiValidationError(
                                $("f1").substringIndex(".", ""),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)")
                        // byte[]
                        // expr NULL
                        .testResult(
                                $("f3").substringIndex($("f4"), 0),
                                "SUBSTRING_INDEX(f3, f4, 0)",
                                null,
                                DataTypes.BYTES().nullable())
                        // delim NULL
                        .testResult(
                                $("f4").substringIndex(null, 0),
                                "SUBSTRING_INDEX(f4, NULL, 0)",
                                null,
                                DataTypes.BYTES().nullable())
                        // count NULL
                        .testResult(
                                $("f4").substringIndex($("f5"), null),
                                "SUBSTRING_INDEX(f4, f5, NULL)",
                                null,
                                DataTypes.BYTES().nullable())
                        // expr empty
                        .testResult(
                                $("f7").substringIndex($("f5"), 1),
                                "SUBSTRING_INDEX(f7, f5, 1)",
                                new byte[] {},
                                DataTypes.BYTES().nullable())
                        // delim empty
                        .testResult(
                                $("f4").substringIndex($("f7"), 1),
                                "SUBSTRING_INDEX(f4, f7, 1)",
                                new byte[] {},
                                DataTypes.BYTES().nullable())
                        // count 0
                        .testResult(
                                $("f4").substringIndex($("f5"), 0),
                                "SUBSTRING_INDEX(f4, f5, 0)",
                                new byte[] {},
                                DataTypes.BYTES().nullable())
                        // delim does not exist in expr
                        .testResult(
                                $("f4").substringIndex($("f8"), 1),
                                "SUBSTRING_INDEX(f4, f8, 1)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES().nullable())
                        // delim length > 1
                        .testResult(
                                $("f4").substringIndex($("f6"), 1),
                                "SUBSTRING_INDEX(f4, f6, 1)",
                                new byte[] {1, 2},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f4").substringIndex($("f6"), 2),
                                "SUBSTRING_INDEX(f4, f6, 2)",
                                new byte[] {1, 2, 3},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f4").substringIndex($("f6"), -1),
                                "SUBSTRING_INDEX(f4, f6, -1)",
                                new byte[] {4, 5, 5},
                                DataTypes.BYTES().nullable())
                        // normal cases
                        .testResult(
                                $("f4").substringIndex($("f5"), -4),
                                "SUBSTRING_INDEX(f4, f5, -4)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f4").substringIndex($("f5"), -2),
                                "SUBSTRING_INDEX(f4, f5, -2)",
                                new byte[] {3, 4, 5, 5},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f6").substringIndex($("f5"), -1),
                                "SUBSTRING_INDEX(f6, f5, -1)",
                                new byte[] {},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f4").substringIndex($("f5"), 3),
                                "SUBSTRING_INDEX(f4, f5, 3)",
                                new byte[] {1, 2, 3, 3},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f4").substringIndex($("f5"), 5),
                                "SUBSTRING_INDEX(f4, f5, 5)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES().nullable())
                        .testResult(
                                $("f6").substringIndex($("f5"), 1),
                                "SUBSTRING_INDEX(f6, f5, 1)",
                                new byte[] {},
                                DataTypes.BYTES().nullable())
                        .testSqlValidationError(
                                "SUBSTRING_INDEX(f4, f5, '')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)")
                        .testTableApiValidationError(
                                $("f4").substringIndex(".", 1),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)"));
    }
}

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
import static org.apache.flink.table.api.Expressions.lit;

/** Test String functions correct behaviour. */
class StringFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(regexpExtractTestCases(), substringIndexTestCases(), translateTestCases())
                .flatMap(s -> s);
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
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.SUBSTRING_INDEX, "StringData")
                        .onFieldsWithData(null, "www.apache.org", "")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").substringIndex(".", 0),
                                "SUBSTRING_INDEX(f0, '.', 0)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(null, 0),
                                "SUBSTRING_INDEX(f1, NULL, 0)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(".", null),
                                "SUBSTRING_INDEX(f1, '.', NULL)",
                                null,
                                DataTypes.STRING())
                        // empty input
                        .testResult(
                                $("f2").substringIndex(".", 1),
                                "SUBSTRING_INDEX(f2, '.', 1)",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex($("f2"), 1),
                                "SUBSTRING_INDEX(f1, '', 1)",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(".", 0),
                                "SUBSTRING_INDEX(f1, '.', 0)",
                                "",
                                DataTypes.STRING())
                        // delim does not exist in expr
                        .testResult(
                                $("f1").substringIndex("..", 1),
                                "SUBSTRING_INDEX(f1, '..', 1)",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex("x", 1),
                                "SUBSTRING_INDEX(f1, 'x', 1)",
                                "www.apache.org",
                                DataTypes.STRING())
                        // delim length > 1
                        .testResult(
                                $("f1").substringIndex("ww", 1),
                                "SUBSTRING_INDEX(f1, 'ww', 1)",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex("ww", 2),
                                "SUBSTRING_INDEX(f1, 'ww', 2)",
                                "w",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex("ww", -1),
                                "SUBSTRING_INDEX(f1, 'ww', -1)",
                                ".apache.org",
                                DataTypes.STRING())
                        // return type & length(expr) = length(delim)
                        .testResult(
                                lit("www.apache.org").substringIndex("www.apache.org", 1),
                                "SUBSTRING_INDEX('www.apache.org', 'www.apache.org', 1)",
                                "",
                                DataTypes.VARCHAR(14).notNull())
                        // normal cases
                        .testResult(
                                $("f1").substringIndex(".", -3),
                                "SUBSTRING_INDEX(f1, '.', -3)",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(".", -2),
                                "SUBSTRING_INDEX(f1, '.', -2)",
                                "apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex("g", -1),
                                "SUBSTRING_INDEX(f1, 'g', -1)",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(".", 3),
                                "SUBSTRING_INDEX(f1, '.', 3)",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex(".", 2),
                                "SUBSTRING_INDEX(f1, '.', 2)",
                                "www.apache",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").substringIndex("w", 1),
                                "SUBSTRING_INDEX(f1, 'w', 1)",
                                "",
                                DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.SUBSTRING_INDEX, "byte[]")
                        .onFieldsWithData(
                                null,
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                new byte[] {3},
                                new byte[] {3, 3},
                                new byte[] {},
                                new byte[] {6},
                                new byte[] {3, 3})
                        .andDataTypes(
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES().notNull())
                        // null input
                        .testResult(
                                $("f0").substringIndex($("f1"), 0),
                                "SUBSTRING_INDEX(f0, f1, 0)",
                                null,
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex(null, 0),
                                "SUBSTRING_INDEX(f1, NULL, 0)",
                                null,
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f2"), null),
                                "SUBSTRING_INDEX(f1, f2, NULL)",
                                null,
                                DataTypes.BYTES())
                        // empty input
                        .testResult(
                                $("f4").substringIndex($("f2"), 1),
                                "SUBSTRING_INDEX(f4, f2, 1)",
                                new byte[0],
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f4"), 1),
                                "SUBSTRING_INDEX(f1, f4, 1)",
                                new byte[0],
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f2"), 0),
                                "SUBSTRING_INDEX(f1, f2, 0)",
                                new byte[0],
                                DataTypes.BYTES())
                        // delim does not exist in expr
                        .testResult(
                                $("f1").substringIndex($("f5"), 1),
                                "SUBSTRING_INDEX(f1, f5, 1)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES())
                        // delim length > 1
                        .testResult(
                                $("f1").substringIndex($("f3"), 1),
                                "SUBSTRING_INDEX(f1, f3, 1)",
                                new byte[] {1, 2},
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f3"), 2),
                                "SUBSTRING_INDEX(f1, f3, 2)",
                                new byte[] {1, 2, 3},
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f3"), -1),
                                "SUBSTRING_INDEX(f1, f3, -1)",
                                new byte[] {4, 5, 5},
                                DataTypes.BYTES())
                        // return type & length(expr) = length(delim)
                        .testResult(
                                $("f6").substringIndex($("f6"), 1),
                                "SUBSTRING_INDEX(f6, f6, 1)",
                                new byte[0],
                                DataTypes.BYTES().notNull())
                        // normal cases
                        .testResult(
                                $("f1").substringIndex($("f2"), -4),
                                "SUBSTRING_INDEX(f1, f2, -4)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f2"), -2),
                                "SUBSTRING_INDEX(f1, f2, -2)",
                                new byte[] {3, 4, 5, 5},
                                DataTypes.BYTES())
                        .testResult(
                                $("f3").substringIndex($("f2"), -1),
                                "SUBSTRING_INDEX(f3, f2, -1)",
                                new byte[0],
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f2"), 3),
                                "SUBSTRING_INDEX(f1, f2, 3)",
                                new byte[] {1, 2, 3, 3},
                                DataTypes.BYTES())
                        .testResult(
                                $("f1").substringIndex($("f2"), 5),
                                "SUBSTRING_INDEX(f1, f2, 5)",
                                new byte[] {1, 2, 3, 3, 3, 4, 5, 5},
                                DataTypes.BYTES())
                        .testResult(
                                $("f3").substringIndex($("f2"), 1),
                                "SUBSTRING_INDEX(f3, f2, 1)",
                                new byte[0],
                                DataTypes.BYTES()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.SUBSTRING_INDEX, "Validation Error")
                        .onFieldsWithData("test", new byte[] {1, 2, 3})
                        .andDataTypes(DataTypes.STRING(), DataTypes.BYTES())
                        .testTableApiValidationError(
                                $("f0").substringIndex($("f1"), 1),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)")
                        .testSqlValidationError(
                                "SUBSTRING_INDEX(f0, f1, 1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "SUBSTRING_INDEX(expr <CHARACTER_STRING>, delim <CHARACTER_STRING>, count <INTEGER>)\n"
                                        + "SUBSTRING_INDEX(expr <BINARY_STRING>, delim <BINARY_STRING>, count <INTEGER>)"));
    }

    private Stream<TestSetSpec> translateTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRANSLATE)
                        .onFieldsWithData(
                                null, "www.apache.org", "", "翻译test，测试", "www.\uD83D\uDE00.org")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").translate("abc", "123"),
                                "TRANSLATE(f0, 'abc', '123')",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate($("f0"), "123"),
                                "TRANSLATE(f1, f0, '123')",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", $("f0")),
                                "TRANSLATE(f1, 'abc', f0)",
                                "www.phe.org",
                                DataTypes.STRING())
                        // empty input
                        .testResult(
                                $("f2").translate("abc", "123"),
                                "TRANSLATE(f2, 'abc', '123')",
                                "",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate($("f2"), "123"),
                                "TRANSLATE(f1, f2, '123')",
                                "www.apache.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", $("f2")),
                                "TRANSLATE(f1, 'abc', f2)",
                                "www.phe.org",
                                DataTypes.STRING())
                        // from longer than to
                        .testResult(
                                $("f1").translate("abcde", "123"),
                                "TRANSLATE(f1, 'abcde', '123')",
                                "www.1p13h.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abcde.", "123"),
                                "TRANSLATE(f1, 'abcde.', '123')",
                                "www1p13horg",
                                DataTypes.STRING())
                        // to longer than from
                        .testResult(
                                $("f1").translate("abc", "12345"),
                                "TRANSLATE(f1, 'abc', '12345')",
                                "www.1p13he.org",
                                DataTypes.STRING())
                        // duplicate chars in from
                        .testResult(
                                $("f1").translate("abcae", "12345"),
                                "TRANSLATE(f1, 'abcae', '12345')",
                                "www.1p13h5.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("...", "123"),
                                "TRANSLATE(f1, '...', '123')",
                                "www1apache1org",
                                DataTypes.STRING())
                        // case sensitive
                        .testResult(
                                $("f1").translate("ABCDE", "12345"),
                                "TRANSLATE(f1, 'ABCDE', '12345')",
                                "www.apache.org",
                                DataTypes.STRING())
                        // Unicode
                        .testResult(
                                $("f3").translate("翻译测试test，", "测试翻译tset。"),
                                "TRANSLATE(f3, '翻译测试test，', '测试翻译tset。')",
                                "测试tset。翻译",
                                DataTypes.STRING())
                        .testResult(
                                $("f3").translate("翻译测试test，", "test翻译  "),
                                "TRANSLATE(f3, '翻译测试test，', 'test翻译  ')",
                                "te翻译 翻st",
                                DataTypes.STRING())
                        .testResult(
                                $("f4").translate(".\uD83D\uDE00", "\uD83D\uDE00."),
                                "TRANSLATE(f4, '.\uD83D\uDE00', '\uD83D\uDE00.')",
                                "www\uD83D\uDE00.\uD83D\uDE00org",
                                DataTypes.STRING())
                        .testResult(
                                $("f4").translate("\uD83D\uDE00w", "笑α"),
                                "TRANSLATE(f4, '\uD83D\uDE00w', '笑α')",
                                "ααα.笑.org",
                                DataTypes.STRING())
                        // return type
                        .testResult(
                                lit("www.apache.org").translate("abc", "123"),
                                "TRANSLATE('www.apache.org', 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING().notNull())
                        // dict reuse (coverage)
                        .testResult(
                                lit("www.apache.org")
                                        .translate("abc", "123")
                                        .translate("abc", "123"),
                                "TRANSLATE(TRANSLATE('www.apache.org', 'abc', '123'), 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING().notNull())
                        // normal cases
                        .testResult(
                                $("f1").translate("abc", "123"),
                                "TRANSLATE(f1, 'abc', '123')",
                                "www.1p13he.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abc", "ABC"),
                                "TRANSLATE(f1, 'abc', 'ABC')",
                                "www.ApAChe.org",
                                DataTypes.STRING())
                        .testResult(
                                $("f1").translate("abcworg", "123 "),
                                "TRANSLATE(f1, 'abcworg', '123 ')",
                                "   .1p13he.",
                                DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRANSLATE, "Validation Error")
                        .onFieldsWithData(12345)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").translate("3", "5"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TRANSLATE3(expr <CHARACTER_STRING>, fromStr <CHARACTER_STRING>, toStr <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "TRANSLATE(f0, '3', '5')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "TRANSLATE3(expr <CHARACTER_STRING>, fromStr <CHARACTER_STRING>, toStr <CHARACTER_STRING>)"));
    }
}

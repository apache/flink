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
        return Stream.of(regexpExtractTestCases(), startsWithTestCases(), translateTestCases())
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

    private Stream<TestSetSpec> startsWithTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.STARTS_WITH, "StringData")
                        .onFieldsWithData(null, "www.apache.org", "", "in中文", "\uD83D\uDE00")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").startsWith("abc"),
                                "STARTSWITH(f0, 'abc')",
                                null,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith($("f0")),
                                "STARTSWITH(f1, f0)",
                                null,
                                DataTypes.BOOLEAN())
                        // empty input
                        .testResult(
                                $("f2").startsWith("abc"),
                                "STARTSWITH(f2, 'abc')",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith($("f2")),
                                "STARTSWITH(f1, f2)",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                lit("").startsWith(""),
                                "STARTSWITH('', '')",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN().notNull())
                        // normal cases
                        .testResult(
                                $("f1").startsWith("ww"),
                                "STARTSWITH(f1, 'ww')",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith("."),
                                "STARTSWITH(f1, '.')",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").startsWith("in中"),
                                "STARTSWITH(f3, 'in中')",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").startsWith("中"),
                                "STARTSWITH(f3, '中')",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f4").startsWith($("f4")),
                                "STARTSWITH(f4, f4)",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f4").startsWith("\uD83D"),
                                "STARTSWITH(f4, '\uD83D')",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.STARTS_WITH, "byte[]")
                        .onFieldsWithData(
                                null,
                                new byte[] {1, 2, 3},
                                new byte[0],
                                new byte[0],
                                new byte[] {1, 2},
                                new byte[] {3})
                        .andDataTypes(
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES().notNull(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES())
                        // null input
                        .testResult(
                                $("f0").startsWith($("f1")),
                                "STARTSWITH(f0, f1)",
                                null,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith($("f0")),
                                "STARTSWITH(f1, f0)",
                                null,
                                DataTypes.BOOLEAN())
                        // empty input
                        .testResult(
                                $("f2").startsWith($("f1")),
                                "STARTSWITH(f2, f1)",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith($("f2")),
                                "STARTSWITH(f1, f2)",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").startsWith($("f3")),
                                "STARTSWITH(f3, f3)",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN().notNull())
                        // normal cases
                        .testResult(
                                $("f1").startsWith($("f4")),
                                "STARTSWITH(f1, f4)",
                                Boolean.TRUE,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f1").startsWith($("f5")),
                                "STARTSWITH(f1, f5)",
                                Boolean.FALSE,
                                DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.STARTS_WITH, "Validation Error")
                        .onFieldsWithData("12345", "123".getBytes())
                        .andDataTypes(DataTypes.STRING(), DataTypes.BYTES())
                        .testTableApiValidationError(
                                $("f0").startsWith($("f1")),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "STARTSWITH(expr <CHARACTER_STRING>, startExpr <CHARACTER_STRING>)\n"
                                        + "STARTSWITH(expr <BINARY_STRING>, startExpr <BINARY_STRING>)")
                        .testSqlValidationError(
                                "STARTSWITH(f0, f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "STARTSWITH(expr <CHARACTER_STRING>, startExpr <CHARACTER_STRING>)\n"
                                        + "STARTSWITH(expr <BINARY_STRING>, startExpr <BINARY_STRING>)"));
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

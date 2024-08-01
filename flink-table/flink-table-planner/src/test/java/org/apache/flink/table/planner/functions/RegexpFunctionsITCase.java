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

/** Test Regexp functions correct behaviour. */
class RegexpFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        regexpCountTestCases(),
                        regexpExtractTestCases(),
                        regexpExtractAllTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> regexpCountTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_COUNT)
                        .onFieldsWithData(null, "abcdeabde")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").regexpCount($("f1")),
                                "REGEXP_COUNT(f0, f1)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                $("f1").regexpCount($("f0")),
                                "REGEXP_COUNT(f1, f0)",
                                null,
                                DataTypes.INT())
                        // invalid regexp
                        .testResult(
                                $("f1").regexpCount("("),
                                "REGEXP_COUNT(f1, '(')",
                                null,
                                DataTypes.INT())
                        // normal cases
                        .testResult(
                                lit("hello world! Hello everyone!").regexpCount("Hello"),
                                "REGEXP_COUNT('hello world! Hello everyone!', 'Hello')",
                                1,
                                DataTypes.INT())
                        .testResult(
                                lit("abcabcabc").regexpCount("abcab"),
                                "REGEXP_COUNT('abcabcabc', 'abcab')",
                                1,
                                DataTypes.INT())
                        .testResult(
                                lit("abcd").regexpCount("z"),
                                "REGEXP_COUNT('abcd', 'z')",
                                0,
                                DataTypes.INT())
                        .testResult(
                                lit("^abc").regexpCount("\\^abc"),
                                "REGEXP_COUNT('^abc', '\\^abc')",
                                1,
                                DataTypes.INT())
                        .testResult(
                                lit("a.b.c.d").regexpCount("\\."),
                                "REGEXP_COUNT('a.b.c.d', '\\.')",
                                3,
                                DataTypes.INT())
                        .testResult(
                                lit("a*b*c*d").regexpCount("\\*"),
                                "REGEXP_COUNT('a*b*c*d', '\\*')",
                                3,
                                DataTypes.INT())
                        .testResult(
                                lit("abc123xyz456").regexpCount("\\d"),
                                "REGEXP_COUNT('abc123xyz456', '\\d')",
                                6,
                                DataTypes.INT())
                        .testResult(
                                lit("Helloworld! Hello everyone!").regexpCount("\\bHello\\b"),
                                "REGEXP_COUNT('Helloworld! Hello everyone!', '\\bHello\\b')",
                                1,
                                DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_COUNT, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").regexpCount("1024"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_COUNT(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "REGEXP_COUNT(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_COUNT(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)"));
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

    private Stream<TestSetSpec> regexpExtractAllTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_EXTRACT_ALL)
                        .onFieldsWithData(null, "abcdeabde", "100-200, 300-400")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").regexpExtractAll($("f1")),
                                "REGEXP_EXTRACT_ALL(f0, f1)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll($("f0")),
                                "REGEXP_EXTRACT_ALL(f1, f0)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll($("f1"), null),
                                "REGEXP_EXTRACT_ALL(f1, f1, NULL)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        // invalid regexp
                        .testResult(
                                $("f1").regexpExtractAll("("),
                                "REGEXP_EXTRACT_ALL(f1, '(')",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        // invalid extractIndex
                        .testResult(
                                $("f1").regexpExtractAll("(abcdeabde)", -1),
                                "REGEXP_EXTRACT_ALL(f1, '(abcdeabde)', -1)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll("abcdeabde"),
                                "REGEXP_EXTRACT_ALL(f1, 'abcdeabde')",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll("(abcdeabde)", 2),
                                "REGEXP_EXTRACT_ALL(f1, '(abcdeabde)', 2)",
                                null,
                                DataTypes.ARRAY(DataTypes.STRING()))
                        // not found
                        .testResult(
                                $("f2").regexpExtractAll("[a-z]", 0),
                                "REGEXP_EXTRACT_ALL(f2, '[a-z]', 0)",
                                new String[] {},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        // optional rule
                        .testResult(
                                $("f1").regexpExtractAll("(abcdeabde)|([a-z]*)", 2),
                                "REGEXP_EXTRACT_ALL(f1, '(abcdeabde)|([a-z]*)', 2)",
                                new String[] {null, ""},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll("ab((c)|(.?))de", 2),
                                "REGEXP_EXTRACT_ALL(f1, 'ab((c)|(.?))de', 2)",
                                new String[] {"c", null},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        // normal cases
                        .testResult(
                                $("f1").regexpExtractAll("(ab)([a-z]+)(e)", 2),
                                "REGEXP_EXTRACT_ALL(f1, '(ab)([a-z]+)(e)', 2)",
                                new String[] {"cdeabd"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f1").regexpExtractAll("", 0),
                                "REGEXP_EXTRACT_ALL(f1, '', 0)",
                                new String[] {"", "", "", "", "", "", "", "", "", ""},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f2").regexpExtractAll("(\\d+)-(\\d+)", 0),
                                "REGEXP_EXTRACT_ALL(f2, '(\\d+)-(\\d+)', 0)",
                                new String[] {"100-200", "300-400"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f2").regexpExtractAll("(\\d+)-(\\d+)", 1),
                                "REGEXP_EXTRACT_ALL(f2, '(\\d+)-(\\d+)', 1)",
                                new String[] {"100", "300"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f2").regexpExtractAll("(\\d+)-(\\d+)", 2),
                                "REGEXP_EXTRACT_ALL(f2, '(\\d+)-(\\d+)', 2)",
                                new String[] {"200", "400"},
                                DataTypes.ARRAY(DataTypes.STRING()))
                        .testResult(
                                $("f2").regexpExtractAll("(\\d+).*", 1),
                                "REGEXP_EXTRACT_ALL(f2, '(\\d+).*', 1)",
                                new String[] {"100"},
                                DataTypes.ARRAY(DataTypes.STRING())),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.REGEXP_EXTRACT_ALL, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").regexpExtractAll("1024"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>, extractIndex <INTEGER_NUMERIC>)")
                        .testSqlValidationError(
                                "REGEXP_EXTRACT_ALL(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>, extractIndex <INTEGER_NUMERIC>)"));
    }
}

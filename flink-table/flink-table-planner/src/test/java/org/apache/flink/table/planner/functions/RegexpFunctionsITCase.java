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
                        regexpExtractAllTestCases(),
                        regexpInstrTestCases(),
                        regexpSubstrTestCases())
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

    private Stream<TestSetSpec> regexpInstrTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_INSTR)
                        .onFieldsWithData(null, "abcdeabde", "100-200, 300-400")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").regexpInstr($("f1")),
                                "REGEXP_INSTR(f0, f1)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                $("f1").regexpInstr($("f0")),
                                "REGEXP_INSTR(f1, f0)",
                                null,
                                DataTypes.INT())
                        // invalid regexp
                        .testResult(
                                $("f1").regexpInstr("("),
                                "REGEXP_INSTR(f1, '(')",
                                null,
                                DataTypes.INT())
                        // not found
                        .testResult(
                                $("f2").regexpInstr("[a-z]"),
                                "REGEXP_INSTR(f2, '[a-z]')",
                                0,
                                DataTypes.INT())
                        // border chars
                        .testResult(
                                lit("Helloworld! Hello everyone!").regexpInstr("\\bHello\\b"),
                                "REGEXP_INSTR('Helloworld! Hello everyone!', '\\bHello\\b')",
                                13,
                                DataTypes.INT())
                        .testResult(
                                lit("Helloworld!  Hello everyone!").regexpInstr("\\bHello\\b"),
                                "REGEXP_INSTR('Helloworld!  Hello everyone!', '\\bHello\\b')",
                                14,
                                DataTypes.INT())
                        // normal cases
                        .testResult(
                                lit("hello world! Hello everyone!").regexpInstr("Hello"),
                                "REGEXP_INSTR('hello world! Hello everyone!', 'Hello')",
                                14,
                                DataTypes.INT())
                        .testResult(
                                lit("a.b.c.d").regexpInstr("\\."),
                                "REGEXP_INSTR('a.b.c.d', '\\.')",
                                2,
                                DataTypes.INT())
                        .testResult(
                                lit("abc123xyz456").regexpInstr("\\d"),
                                "REGEXP_INSTR('abc123xyz456', '\\d')",
                                4,
                                DataTypes.INT())
                        .testResult(
                                $("f2").regexpInstr("(\\d+)-(\\d+)"),
                                "REGEXP_INSTR(f2, '(\\d+)-(\\d+)')",
                                1,
                                DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_INSTR, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").regexpInstr("1024"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_INSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "REGEXP_INSTR(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_INSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)"));
    }

    private Stream<TestSetSpec> regexpSubstrTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_SUBSTR)
                        .onFieldsWithData(null, "abcdeabde", "100-200, 300-400")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").regexpSubstr($("f1")),
                                "REGEXP_SUBSTR(f0, f1)",
                                null,
                                DataTypes.STRING())
                        .testResult(
                                $("f1").regexpSubstr($("f0")),
                                "REGEXP_SUBSTR(f1, f0)",
                                null,
                                DataTypes.STRING())
                        // invalid regexp
                        .testResult(
                                $("f1").regexpSubstr("("),
                                "REGEXP_SUBSTR(f1, '(')",
                                null,
                                DataTypes.STRING())
                        // not found
                        .testResult(
                                $("f2").regexpSubstr("[a-z]"),
                                "REGEXP_SUBSTR(f2, '[a-z]')",
                                null,
                                DataTypes.STRING())
                        // border chars
                        .testResult(
                                lit("Helloworld! Hello everyone!").regexpSubstr("\\bHello\\b"),
                                "REGEXP_SUBSTR('Helloworld! Hello everyone!', '\\bHello\\b')",
                                "Hello",
                                DataTypes.STRING())
                        .testResult(
                                $("f2").regexpSubstr("(\\d+)-(\\d+)$"),
                                "REGEXP_SUBSTR(f2, '(\\d+)-(\\d+)$')",
                                "300-400",
                                DataTypes.STRING())
                        // normal cases
                        .testResult(
                                lit("hello world! Hello everyone!").regexpSubstr("Hello"),
                                "REGEXP_SUBSTR('hello world! Hello everyone!', 'Hello')",
                                "Hello",
                                DataTypes.STRING())
                        .testResult(
                                lit("a.b.c.d").regexpSubstr("\\."),
                                "REGEXP_SUBSTR('a.b.c.d', '\\.')",
                                ".",
                                DataTypes.STRING())
                        .testResult(
                                lit("abc123xyz456").regexpSubstr("\\d"),
                                "REGEXP_SUBSTR('abc123xyz456', '\\d')",
                                "1",
                                DataTypes.STRING())
                        .testResult(
                                $("f2").regexpSubstr("(\\d+)-(\\d+)"),
                                "REGEXP_SUBSTR(f2, '(\\d+)-(\\d+)')",
                                "100-200",
                                DataTypes.STRING()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.REGEXP_SUBSTR, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").regexpSubstr("1024"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_SUBSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "REGEXP_SUBSTR(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_SUBSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)"));
    }
}

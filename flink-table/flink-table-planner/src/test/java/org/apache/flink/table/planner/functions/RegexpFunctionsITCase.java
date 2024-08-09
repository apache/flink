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
import org.apache.flink.util.FlinkRuntimeException;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** Test Regexp functions correct behaviour. */
class RegexpFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(regexpExtractAllTestCases()).flatMap(s -> s);
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
                        .testTableApiRuntimeError(
                                $("f1").regexpExtractAll("("), FlinkRuntimeException.class)
                        .testSqlRuntimeError(
                                "REGEXP_EXTRACT_ALL(f1, '(')", FlinkRuntimeException.class)
                        // invalid extractIndex
                        .testTableApiRuntimeError(
                                $("f1").regexpExtractAll("abcdeabde"),
                                IndexOutOfBoundsException.class,
                                "Extract group index 1 is out of range [0, 0].")
                        .testSqlRuntimeError(
                                "REGEXP_EXTRACT_ALL(f1, 'abcdeabde')",
                                IndexOutOfBoundsException.class,
                                "Extract group index 1 is out of range [0, 0].")
                        .testTableApiRuntimeError(
                                $("f1").regexpExtractAll("(abcdeabde)", -1),
                                IndexOutOfBoundsException.class,
                                "Extract group index -1 is out of range [0, 1].")
                        .testSqlRuntimeError(
                                "REGEXP_EXTRACT_ALL(f1, '(abcdeabde)', -1)",
                                IndexOutOfBoundsException.class,
                                "Extract group index -1 is out of range [0, 1].")
                        .testTableApiRuntimeError(
                                $("f1").regexpExtractAll("(abcdeabde)", 2),
                                IndexOutOfBoundsException.class,
                                "Extract group index 2 is out of range [0, 1].")
                        .testSqlRuntimeError(
                                "REGEXP_EXTRACT_ALL(f1, '(abcdeabde)', 2)",
                                IndexOutOfBoundsException.class,
                                "Extract group index 2 is out of range [0, 1].")
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
                        // return type
                        .testResult(
                                lit("abcdeabde").regexpExtractAll("abcdeabde", 0),
                                "REGEXP_EXTRACT_ALL('abcdeabde', 'abcdeabde', 0)",
                                new String[] {"abcdeabde"},
                                DataTypes.ARRAY(DataTypes.STRING()).notNull())
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
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>, extractIndex <INTEGER>)")
                        .testSqlValidationError(
                                "REGEXP_EXTRACT_ALL(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)\n"
                                        + "REGEXP_EXTRACT_ALL(str <CHARACTER_STRING>, regex <CHARACTER_STRING>, extractIndex <INTEGER>)"));
    }
}

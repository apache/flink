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
        return regexpSubstrTestCases();
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
                        .testTableApiRuntimeError(
                                $("f1").regexpSubstr("("), FlinkRuntimeException.class)
                        .testSqlRuntimeError("REGEXP_SUBSTR(f1, '(')", FlinkRuntimeException.class)
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

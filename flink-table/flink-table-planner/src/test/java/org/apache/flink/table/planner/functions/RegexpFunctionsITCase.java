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
        return regexpInStrTestCases();
    }

    private Stream<TestSetSpec> regexpInStrTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_INSTR)
                        .onFieldsWithData(null, "abcdeabde", "100-200, 300-400")
                        .andDataTypes(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING())
                        // null input
                        .testResult(
                                $("f0").regexpInStr($("f1")),
                                "REGEXP_INSTR(f0, f1)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                $("f1").regexpInStr($("f0")),
                                "REGEXP_INSTR(f1, f0)",
                                null,
                                DataTypes.INT())
                        // invalid regexp
                        .testTableApiRuntimeError(
                                $("f1").regexpInStr("("), FlinkRuntimeException.class)
                        .testSqlRuntimeError("REGEXP_INSTR(f1, '(')", FlinkRuntimeException.class)
                        // not found
                        .testResult(
                                $("f2").regexpInStr("[a-z]"),
                                "REGEXP_INSTR(f2, '[a-z]')",
                                0,
                                DataTypes.INT())
                        // border chars
                        .testResult(
                                lit("Helloworld! Hello everyone!").regexpInStr("\\bHello\\b"),
                                "REGEXP_INSTR('Helloworld! Hello everyone!', '\\bHello\\b')",
                                13,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("Helloworld!  Hello everyone!").regexpInStr("\\bHello\\b"),
                                "REGEXP_INSTR('Helloworld!  Hello everyone!', '\\bHello\\b')",
                                14,
                                DataTypes.INT().notNull())
                        // normal cases
                        .testResult(
                                lit("hello world! Hello everyone!").regexpInStr("Hello"),
                                "REGEXP_INSTR('hello world! Hello everyone!', 'Hello')",
                                14,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("a.b.c.d").regexpInStr("\\."),
                                "REGEXP_INSTR('a.b.c.d', '\\.')",
                                2,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("abc123xyz456").regexpInStr("\\d"),
                                "REGEXP_INSTR('abc123xyz456', '\\d')",
                                4,
                                DataTypes.INT().notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.REGEXP_INSTR, "Validation Error")
                        .onFieldsWithData(1024)
                        .andDataTypes(DataTypes.INT())
                        .testTableApiValidationError(
                                $("f0").regexpInStr("1024"),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_INSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)")
                        .testSqlValidationError(
                                "REGEXP_INSTR(f0, '1024')",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "REGEXP_INSTR(str <CHARACTER_STRING>, regex <CHARACTER_STRING>)"));
    }
}

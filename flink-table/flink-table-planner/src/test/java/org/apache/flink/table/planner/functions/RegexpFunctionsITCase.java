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
        return Stream.of(regexpCountTestCases()).flatMap(s -> s);
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
                        .testTableApiRuntimeError(
                                $("f1").regexpCount("("), FlinkRuntimeException.class)
                        .testSqlRuntimeError("REGEXP_COUNT(f1, '(')", FlinkRuntimeException.class)
                        // normal cases
                        .testResult(
                                lit("hello world! Hello everyone!").regexpCount("Hello"),
                                "REGEXP_COUNT('hello world! Hello everyone!', 'Hello')",
                                1,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("abcabcabc").regexpCount("abcab"),
                                "REGEXP_COUNT('abcabcabc', 'abcab')",
                                1,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("abcd").regexpCount("z"),
                                "REGEXP_COUNT('abcd', 'z')",
                                0,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("^abc").regexpCount("\\^abc"),
                                "REGEXP_COUNT('^abc', '\\^abc')",
                                1,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("a.b.c.d").regexpCount("\\."),
                                "REGEXP_COUNT('a.b.c.d', '\\.')",
                                3,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("a*b*c*d").regexpCount("\\*"),
                                "REGEXP_COUNT('a*b*c*d', '\\*')",
                                3,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("abc123xyz456").regexpCount("\\d"),
                                "REGEXP_COUNT('abc123xyz456', '\\d')",
                                6,
                                DataTypes.INT().notNull())
                        .testResult(
                                lit("Helloworld! Hello everyone!").regexpCount("\\bHello\\b"),
                                "REGEXP_COUNT('Helloworld! Hello everyone!', '\\bHello\\b')",
                                1,
                                DataTypes.INT().notNull()),
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
}

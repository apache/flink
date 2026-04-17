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

/** Integration tests for {@code LIKE <pattern> [ESCAPE <escape>]} pattern matching operations. */
class LikeFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(withEscape(), withoutEscape()).flatMap(s -> s);
    }

    private Stream<TestSetSpec> withoutEscape() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LIKE)
                        .onFieldsWithData("test", "t\"est", "tes\"t", "t\"es\"t")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())

                        // Multiple % with quote in middle segment
                        .testSqlResult("f0 LIKE 'a%b\"c%d'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE 't%es%t'", true, DataTypes.BOOLEAN())

                        // Quote in first segment
                        .testSqlResult("f0 LIKE 'a\"b%c%d'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE 't\"e%s%t'", true, DataTypes.BOOLEAN())

                        // Quote in last segment
                        .testSqlResult("f0 LIKE 'a%b%c\"d'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 't%e%s\"t'", true, DataTypes.BOOLEAN())

                        // Multiple quotes
                        .testSqlResult("f0 LIKE 'a\"%b\"%c'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f3 LIKE 't\"%s\"%t'", true, DataTypes.BOOLEAN())

                        // Pattern with underscore and quote
                        .testSqlResult("f0 LIKE 'te_t\"'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 'te_\"t'", true, DataTypes.BOOLEAN())

                        // Multiple underscores with quotes
                        .testSqlResult("f0 LIKE '_\"_test_\"_'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f3 LIKE '_\"__\"_'", true, DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LIKE)
                        .onFieldsWithData("test", "abc%def", "test_123", "hello'world")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())

                        // Normal exact match - no special chars
                        .testSqlResult("f0 LIKE 'test'", true, DataTypes.BOOLEAN())

                        // Normal exact match - in case of empty strings
                        .testSqlResult("'' LIKE ''", true, DataTypes.BOOLEAN().notNull())
                        .testSqlResult("'' LIKE '%'", true, DataTypes.BOOLEAN().notNull())
                        .testSqlResult("f0 LIKE ''", false, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '%%'", true, DataTypes.BOOLEAN())

                        // Starts with pattern
                        .testSqlResult("f0 LIKE 'te%'", true, DataTypes.BOOLEAN())

                        // Ends with pattern
                        .testSqlResult("f0 LIKE '%st'", true, DataTypes.BOOLEAN())

                        // Contains pattern
                        .testSqlResult("f0 LIKE '%es%'", true, DataTypes.BOOLEAN())

                        // Single quote in data (not pattern)
                        // SQL escapes single quote as ''
                        .testSqlResult("f3 LIKE '%''%'", true, DataTypes.BOOLEAN())

                        // Pattern with % in data matches literal
                        .testSqlResult("f1 LIKE 'abc%def'", true, DataTypes.BOOLEAN())

                        // Pattern doesn't match
                        .testSqlResult("f0 LIKE 'orange'", false, DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LIKE)
                        .onFieldsWithData("test")
                        .andDataTypes(DataTypes.STRING())

                        // With backslash and double quote in the middle
                        .testSqlResult("f0 LIKE 'test\\\"more'", false, DataTypes.BOOLEAN())

                        // With backslash at the end
                        .testSqlResult("f0 LIKE 'test\\\\'", false, DataTypes.BOOLEAN()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LIKE)
                        .onFieldsWithData("test", "\"test", "te\"st", "test\"", "test\\")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())

                        // Quick path
                        .testSqlResult("f0 LIKE 'test\"quote'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 'te\"st'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '\"test'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE '\"test'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE 'test\"'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f3 LIKE 'test\"'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE 'start\"test%'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 'te\"s%'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '%test\"end'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE '%te\"st'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '%mid\"dle%'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE '%te\"st%'", true, DataTypes.BOOLEAN())

                        // Trailing backslash
                        .testSqlResult("f0 LIKE 'test\\'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f4 LIKE 'test\\'", true, DataTypes.BOOLEAN()));
    }

    private Stream<TestSetSpec> withEscape() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LIKE)
                        .onFieldsWithData("test", "test%", "te_st", "te\"st", "test\\", "✅test✅")
                        .andDataTypes(
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING(),
                                DataTypes.STRING())
                        // Empty strings in pattern or escape
                        .testSqlResult("f0 LIKE 'test\"end' ESCAPE ''", false, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '' ESCAPE ''", false, DataTypes.BOOLEAN())
                        // Escaping with emoji
                        .testSqlResult("f0 LIKE 'test' ESCAPE '✅'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE 'test✅%' ESCAPE '✅'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE 'test!%' ESCAPE '!'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '✅test' ESCAPE '!'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f0 LIKE '✅test' ESCAPE '\\'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f5 LIKE '✅test✅' ESCAPE '\\'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f5 LIKE '✅%✅' ESCAPE '\\'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f5 LIKE '✅%' ESCAPE '\\'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f5 LIKE '%st✅' ESCAPE '\\'", true, DataTypes.BOOLEAN())
                        // Mixed escaped symbols
                        .testSqlResult("f2 LIKE 'te_st' ESCAPE '!'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 'te__st' ESCAPE '_'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE 'test_%' ESCAPE '_'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f2 LIKE 'te%_st' ESCAPE '%'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f1 LIKE 'test%%' ESCAPE '%'", true, DataTypes.BOOLEAN())
                        .testSqlValidationError(
                                "f2 LIKE 'te_st' ESCAPE '_'", "Invalid escape sequence 'te_st', 2")
                        .testSqlValidationError(
                                "f1 LIKE 'test_' ESCAPE '_'", "Invalid escape sequence 'test_', 4")
                        .testSqlValidationError(
                                "f2 LIKE 'te%st' ESCAPE '%'", "Invalid escape sequence 'te%st', 2")
                        .testSqlValidationError(
                                "f1 LIKE 'test%' ESCAPE '%'", "Invalid escape sequence 'test%', 4")
                        .testSqlValidationError(
                                "f0 LIKE 'test\\\"end' ESCAPE '\\'",
                                "Invalid escape sequence 'test\\\"end', 4")
                        .testSqlValidationError(
                                "f0 LIKE '%e_t%' ESCAPE 'ab'", "Invalid escape character 'ab'")
                        // Mixed
                        .testSqlResult("f0 LIKE 'test\"end' ESCAPE '!'", false, DataTypes.BOOLEAN())
                        .testSqlResult("f3 LIKE 'te\"st' ESCAPE '!'", true, DataTypes.BOOLEAN())
                        .testSqlResult("f4 LIKE 'test\\' ESCAPE '!'", true, DataTypes.BOOLEAN())
                        .testSqlResult(
                                "'a1bc' LIKE CAST('a%\"+1+\"b%c' AS STRING) ESCAPE '!'",
                                false, DataTypes.BOOLEAN().notNull())
                        .testSqlResult(
                                "'a1\"+1+\"bc' LIKE CAST('a%\"+1+\"b%c' AS STRING) ESCAPE '!'",
                                true, DataTypes.BOOLEAN().notNull())
                        // Unicode like sequence
                        .testSqlResult(
                                "f0 LIKE 'test" + "\\u" + "000Aend' ESCAPE '!'",
                                false,
                                DataTypes.BOOLEAN())
                        .testSqlResult(
                                "'test\\u000Aend' LIKE 'test" + "\\u" + "000Aend' ESCAPE '!'",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // Special characters
                        .testSqlResult(
                                "f0 LIKE '\btest\ne\\nd\f' ESCAPE '!'", false, DataTypes.BOOLEAN())
                        .testSqlResult(
                                "'\btest\ne\\nd\f' LIKE '\btest\ne\\nd\f' ESCAPE '!'",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // Invalid escape character
                        .testSqlValidationError(
                                "f0 LIKE 'test' ESCAPE '\u0000'",
                                "Invalid escape character '\u0000'"));
    }
}

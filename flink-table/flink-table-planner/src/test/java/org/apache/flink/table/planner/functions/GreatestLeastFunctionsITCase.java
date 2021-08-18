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

import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Tests for GREATEST, LEAST functions {@link BuiltInFunctionDefinitions}. */
public class GreatestLeastFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.GREATEST)
                        .onFieldsWithData(
                                null,
                                1,
                                2,
                                3.14,
                                "hello",
                                "world",
                                LocalDateTime.parse("1970-01-01T00:00:03.001"),
                                LocalDateTime.parse("1970-01-01T00:00:02.001"))
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.INT().notNull(),
                                DataTypes.DECIMAL(3, 2).notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.TIMESTAMP(3).notNull(),
                                DataTypes.TIMESTAMP(3).notNull())
                        .testSqlValidationError(
                                "GREATEST(f1, f4)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "GREATEST(INT NOT NULL, STRING NOT NULL)")
                        .testSqlResult(
                                "CAST(GREATEST(f1, f3, f2) AS DECIMAL(3, 2))",
                                BigDecimal.valueOf(3.14),
                                DataTypes.DECIMAL(3, 2).notNull())
                        .testResult(
                                call("GREATEST", $("f0"), $("f1"), $("f2")),
                                "GREATEST(f0, f1, f2)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                call("GREATEST", $("f4"), $("f5")),
                                "GREATEST(f4, f5)",
                                "world",
                                DataTypes.STRING().notNull())
                        .testSqlResult(
                                "GREATEST(f6, f7)",
                                LocalDateTime.parse("1970-01-01T00:00:03.001"),
                                DataTypes.TIMESTAMP(3).notNull())
                        .testSqlValidationError(
                                "GREATEST(f5, f6)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "GREATEST(STRING NOT NULL, TIMESTAMP(3) NOT NULL)"),
                TestSpec.forFunction(BuiltInFunctionDefinitions.LEAST)
                        .onFieldsWithData(null, 1, 2, 3.14, "hello", "world")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.INT().notNull(),
                                DataTypes.DECIMAL(3, 2).notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull())
                        .testSqlValidationError(
                                "LEAST(f1, f4)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "LEAST(INT NOT NULL, STRING NOT NULL)")
                        .testSqlResult(
                                "CAST(LEAST(f1, f3, f2) AS DECIMAL(3, 2))",
                                BigDecimal.valueOf(100, 2),
                                DataTypes.DECIMAL(3, 2).notNull())
                        .testTableApiResult(
                                call("LEAST", $("f1"), $("f3"), $("f2"))
                                        .cast(DataTypes.DECIMAL(3, 2)),
                                BigDecimal.valueOf(100, 2),
                                DataTypes.DECIMAL(3, 2).notNull())
                        .testResult(
                                call("LEAST", $("f0"), $("f1")),
                                "LEAST(f0, f1)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                call("LEAST", $("f4"), $("f5")),
                                "LEAST(f4, f5)",
                                "hello",
                                DataTypes.STRING().notNull()));
    }
}

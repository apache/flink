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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/** Tests for GREATEST, LEAST functions {@link BuiltInFunctionDefinitions}. */
class GreatestLeastFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.GREATEST)
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
                        .testResult(
                                call("GREATEST", $("f1"), $("f3"), $("f2"))
                                        .cast(DataTypes.DECIMAL(3, 2)),
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
                        .testResult(
                                call(
                                        "GREATEST",
                                        call("IFNULL", $("f4"), $("f5")),
                                        call("IFNULL", $("f5"), $("f4"))),
                                "GREATEST(IFNULL(f4, f5), IFNULL(f5, f4))",
                                "world",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("GREATEST", $("f6"), $("f7")),
                                "GREATEST(f6, f7)",
                                LocalDateTime.parse("1970-01-01T00:00:03.001"),
                                DataTypes.TIMESTAMP(3).notNull())
                        // assert that primitive types are returned and used in the equality
                        // operator applied on top of the GREATEST functions
                        .testResult(
                                call(
                                        "EQUALS",
                                        call("GREATEST", $("f1"), $("f2")),
                                        call("GREATEST", $("f1"), $("f2"))),
                                "GREATEST(f1, f2) = GREATEST(f1, f2)",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                call(
                                        "EQUALS",
                                        call("GREATEST", $("f0"), $("f1")),
                                        call("GREATEST", $("f0"), $("f1"))),
                                "GREATEST(f0, f1) = GREATEST(f0, f1)",
                                null,
                                DataTypes.BOOLEAN())
                        .testSqlValidationError(
                                "GREATEST(f5, f6)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "GREATEST(STRING NOT NULL, TIMESTAMP(3) NOT NULL)"),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.LEAST)
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
                                "LEAST(f1, f4)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "LEAST(INT NOT NULL, STRING NOT NULL)")
                        .testResult(
                                call("LEAST", $("f1"), $("f3"), $("f2"))
                                        .cast(DataTypes.DECIMAL(3, 2)),
                                "CAST(LEAST(f1, f3, f2) AS DECIMAL(3, 2))",
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
                                DataTypes.STRING().notNull())
                        // assert that primitive types are returned and used in the equality
                        // operator applied on top of the GREATEST functions
                        .testResult(
                                call(
                                        "EQUALS",
                                        call("LEAST", $("f1"), $("f2")),
                                        call("LEAST", $("f1"), $("f2"))),
                                "LEAST(f1, f2) = LEAST(f1, f2)",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                call(
                                        "EQUALS",
                                        call("LEAST", $("f0"), $("f1")),
                                        call("LEAST", $("f0"), $("f1"))),
                                "LEAST(f0, f1) = LEAST(f0, f1)",
                                null,
                                DataTypes.BOOLEAN())
                        .testResult(
                                call(
                                        "LEAST",
                                        call("IFNULL", $("f4"), $("f5")),
                                        call("IFNULL", $("f5"), $("f4"))),
                                "LEAST(IFNULL(f4, f5), IFNULL(f5, f4))",
                                "hello",
                                DataTypes.STRING().notNull())
                        .testSqlValidationError(
                                "LEAST(f5, f6)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "LEAST(STRING NOT NULL, TIMESTAMP(3) NOT NULL)"));
    }
}

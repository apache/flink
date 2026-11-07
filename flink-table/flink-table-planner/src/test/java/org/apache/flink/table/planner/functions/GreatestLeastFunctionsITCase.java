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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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
                                        + "LEAST(STRING NOT NULL, TIMESTAMP(3) NOT NULL)"),
                // raw </> comparison path: byte/short/long and the int-backed DATE/TIME
                TestSetSpec.forExpression("GREATEST/LEAST over TINYINT/SMALLINT/BIGINT/DATE/TIME")
                        .onFieldsWithData(
                                (byte) 1,
                                (byte) 2,
                                (short) 10,
                                (short) 20,
                                100L,
                                200L,
                                LocalDate.of(2020, 1, 1),
                                LocalDate.of(2021, 6, 15),
                                LocalTime.of(1, 2, 3),
                                LocalTime.of(10, 20, 30))
                        .andDataTypes(
                                DataTypes.TINYINT().notNull(),
                                DataTypes.TINYINT().notNull(),
                                DataTypes.SMALLINT().notNull(),
                                DataTypes.SMALLINT().notNull(),
                                DataTypes.BIGINT().notNull(),
                                DataTypes.BIGINT().notNull(),
                                DataTypes.DATE().notNull(),
                                DataTypes.DATE().notNull(),
                                DataTypes.TIME().notNull(),
                                DataTypes.TIME().notNull())
                        .testResult(
                                call("GREATEST", $("f0"), $("f1")),
                                "GREATEST(f0, f1)",
                                (byte) 2,
                                DataTypes.TINYINT().notNull())
                        .testResult(
                                call("LEAST", $("f0"), $("f1")),
                                "LEAST(f0, f1)",
                                (byte) 1,
                                DataTypes.TINYINT().notNull())
                        .testResult(
                                call("GREATEST", $("f2"), $("f3")),
                                "GREATEST(f2, f3)",
                                (short) 20,
                                DataTypes.SMALLINT().notNull())
                        .testResult(
                                call("LEAST", $("f2"), $("f3")),
                                "LEAST(f2, f3)",
                                (short) 10,
                                DataTypes.SMALLINT().notNull())
                        .testResult(
                                call("GREATEST", $("f4"), $("f5")),
                                "GREATEST(f4, f5)",
                                200L,
                                DataTypes.BIGINT().notNull())
                        .testResult(
                                call("LEAST", $("f4"), $("f5")),
                                "LEAST(f4, f5)",
                                100L,
                                DataTypes.BIGINT().notNull())
                        .testResult(
                                call("GREATEST", $("f6"), $("f7")),
                                "GREATEST(f6, f7)",
                                LocalDate.of(2021, 6, 15),
                                DataTypes.DATE().notNull())
                        .testResult(
                                call("LEAST", $("f6"), $("f7")),
                                "LEAST(f6, f7)",
                                LocalDate.of(2020, 1, 1),
                                DataTypes.DATE().notNull())
                        .testResult(
                                call("GREATEST", $("f8"), $("f9")),
                                "GREATEST(f8, f9)",
                                LocalTime.of(10, 20, 30),
                                DataTypes.TIME().notNull())
                        .testResult(
                                call("LEAST", $("f8"), $("f9")),
                                "LEAST(f8, f9)",
                                LocalTime.of(1, 2, 3),
                                DataTypes.TIME().notNull()),
                // Float/Double.compare path: NaN sorts greatest, -0.0 sorts below 0.0.
                // NaN/-0.0 are fed as strings and cast at runtime because fromValues stores numeric
                // literals as BigDecimal, which cannot represent NaN and collapses -0.0 to 0.0.
                TestSetSpec.forExpression(
                                "GREATEST/LEAST over FLOAT/DOUBLE incl NaN and signed zero")
                        .onFieldsWithData(1.0f, 2.0d, "NaN", "-0.0", "0.0")
                        .andDataTypes(
                                DataTypes.FLOAT().notNull(),
                                DataTypes.DOUBLE().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull(),
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("GREATEST", $("f0"), $("f2").cast(DataTypes.FLOAT())),
                                "GREATEST(f0, CAST(f2 AS FLOAT))",
                                Float.NaN,
                                DataTypes.FLOAT().notNull())
                        .testResult(
                                call("LEAST", $("f0"), $("f2").cast(DataTypes.FLOAT())),
                                "LEAST(f0, CAST(f2 AS FLOAT))",
                                1.0f,
                                DataTypes.FLOAT().notNull())
                        .testResult(
                                call("GREATEST", $("f1"), $("f2").cast(DataTypes.DOUBLE())),
                                "GREATEST(f1, CAST(f2 AS DOUBLE))",
                                Double.NaN,
                                DataTypes.DOUBLE().notNull())
                        .testResult(
                                call("LEAST", $("f1"), $("f2").cast(DataTypes.DOUBLE())),
                                "LEAST(f1, CAST(f2 AS DOUBLE))",
                                2.0d,
                                DataTypes.DOUBLE().notNull())
                        .testResult(
                                call(
                                        "GREATEST",
                                        $("f3").cast(DataTypes.FLOAT()),
                                        $("f4").cast(DataTypes.FLOAT())),
                                "GREATEST(CAST(f3 AS FLOAT), CAST(f4 AS FLOAT))",
                                0.0f,
                                DataTypes.FLOAT().notNull())
                        .testResult(
                                call(
                                        "LEAST",
                                        $("f3").cast(DataTypes.FLOAT()),
                                        $("f4").cast(DataTypes.FLOAT())),
                                "LEAST(CAST(f3 AS FLOAT), CAST(f4 AS FLOAT))",
                                -0.0f,
                                DataTypes.FLOAT().notNull()),
                // Boolean.compare path: false < true
                TestSetSpec.forExpression("GREATEST/LEAST over BOOLEAN")
                        .onFieldsWithData(false, true)
                        .andDataTypes(DataTypes.BOOLEAN().notNull(), DataTypes.BOOLEAN().notNull())
                        .testResult(
                                call("GREATEST", $("f0"), $("f1")),
                                "GREATEST(f0, f1)",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                call("LEAST", $("f0"), $("f1")),
                                "LEAST(f0, f1)",
                                false,
                                DataTypes.BOOLEAN().notNull()),
                // short-circuit: a NULL arg must stop evaluation of the remaining args. MOD(f1, 0)
                // would throw if evaluated; repeating it forces a shared RexLocalRef so the body
                // must
                // stay guarded (not hoisted) under table.optimizer.reuse-sub-expression-enabled.
                TestSetSpec.forExpression("GREATEST/LEAST short-circuit on NULL arg")
                        .onFieldsWithData(null, 6, 0)
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.INT().notNull())
                        .testResult(
                                call("GREATEST", $("f0"), $("f1").mod($("f2"))),
                                "GREATEST(f0, MOD(f1, f2))",
                                null,
                                DataTypes.INT())
                        .testResult(
                                call("LEAST", $("f0"), $("f1").mod($("f2"))),
                                "LEAST(f0, MOD(f1, f2))",
                                null,
                                DataTypes.INT())
                        .testResult(
                                call(
                                        "GREATEST",
                                        $("f0"),
                                        $("f1").mod($("f2")),
                                        $("f1").mod($("f2"))),
                                "GREATEST(f0, MOD(f1, f2), MOD(f1, f2))",
                                null,
                                DataTypes.INT()));
    }
}

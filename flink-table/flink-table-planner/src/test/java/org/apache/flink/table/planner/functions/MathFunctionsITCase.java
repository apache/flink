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
import java.time.Duration;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** Tests for math {@link BuiltInFunctionDefinitions} that fully use the new type system. */
class MathFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.PLUS)
                        .onFieldsWithData(new BigDecimal("1514356320000"))
                        .andDataTypes(DataTypes.DECIMAL(19, 0).notNull())
                        // DECIMAL(19, 0) + INT(10, 0) => DECIMAL(20, 0)
                        .testResult(
                                $("f0").plus(6),
                                "f0 + 6",
                                new BigDecimal("1514356320006"),
                                DataTypes.DECIMAL(20, 0).notNull())
                        // DECIMAL(19, 0) + DECIMAL(19, 0) => DECIMAL(20, 0)
                        .testResult(
                                $("f0").plus($("f0")),
                                "f0 + f0",
                                new BigDecimal("3028712640000"),
                                DataTypes.DECIMAL(20, 0).notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MINUS)
                        .onFieldsWithData(new BigDecimal("1514356320000"))
                        .andDataTypes(DataTypes.DECIMAL(19, 0))
                        // DECIMAL(19, 0) - INT(10, 0) => DECIMAL(20, 0)
                        .testResult(
                                $("f0").minus(6),
                                "f0 - 6",
                                new BigDecimal("1514356319994"),
                                DataTypes.DECIMAL(20, 0))
                        // DECIMAL(19, 0) - DECIMAL(19, 0) => DECIMAL(20, 0)
                        .testResult(
                                $("f0").minus($("f0")),
                                "f0 - f0",
                                new BigDecimal("0"),
                                DataTypes.DECIMAL(20, 0)),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.DIVIDE)
                        .onFieldsWithData(new BigDecimal("1514356320000"))
                        .andDataTypes(DataTypes.DECIMAL(19, 0).notNull())
                        // DECIMAL(19, 0) / INT(10, 0) => DECIMAL(30, 11)
                        .testResult(
                                $("f0").dividedBy(6),
                                "f0 / 6",
                                new BigDecimal("252392720000.00000000000"),
                                DataTypes.DECIMAL(30, 11).notNull())
                        // DECIMAL(19, 0) / DECIMAL(19, 0) => DECIMAL(39, 20) => DECIMAL(38, 19)
                        .testResult(
                                $("f0").dividedBy($("f0")),
                                "f0 / f0",
                                new BigDecimal("1.0000000000000000000"),
                                DataTypes.DECIMAL(38, 19).notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TIMES)
                        .onFieldsWithData(new BigDecimal("1514356320000"), Duration.ofSeconds(2), 2)
                        .andDataTypes(
                                DataTypes.DECIMAL(19, 0),
                                DataTypes.INTERVAL(DataTypes.SECOND(3)),
                                DataTypes.INT())
                        // DECIMAL(19, 0) * INT(10, 0) => DECIMAL(29, 0)
                        .testResult(
                                $("f0").times(6),
                                "f0 * 6",
                                new BigDecimal("9086137920000"),
                                DataTypes.DECIMAL(30, 0))
                        // DECIMAL(19, 0) * DECIMAL(19, 0) => DECIMAL(38, 0)
                        .testResult(
                                $("f0").times($("f0")),
                                "f0 * f0",
                                new BigDecimal("2293275063923942400000000"),
                                DataTypes.DECIMAL(38, 0))
                        .testResult(
                                $("f1").times(3),
                                "f1 * 3",
                                Duration.ofSeconds(6),
                                DataTypes.INTERVAL(DataTypes.SECOND(3)))
                        .testResult(
                                $("f2").times(
                                                lit(3).seconds()
                                                        .cast(
                                                                DataTypes.INTERVAL(
                                                                        DataTypes.SECOND(3)))),
                                "f2 * interval '3' second(3)",
                                Duration.ofSeconds(6),
                                DataTypes.INTERVAL(DataTypes.SECOND(3))),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.MOD)
                        .onFieldsWithData(new BigDecimal("1514356320000"), 44L, 3)
                        .andDataTypes(DataTypes.DECIMAL(19, 0), DataTypes.BIGINT(), DataTypes.INT())
                        // DECIMAL(19, 0) % DECIMAL(19, 0) => DECIMAL(19, 0)
                        .testResult(
                                $("f0").mod($("f0")),
                                "MOD(f0, f0)",
                                new BigDecimal(0),
                                DataTypes.DECIMAL(19, 0))
                        // DECIMAL(19, 0) % INT(10, 0) => INT(10, 0)
                        .testResult($("f0").mod(6), "MOD(f0, 6)", 0, DataTypes.INT())
                        // BIGINT(19, 0) % INT(10, 0) => INT(10, 0)
                        .testResult($("f1").mod($("f2")), "MOD(f1, f2)", 2, DataTypes.INT()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ROUND)
                        .onFieldsWithData(new BigDecimal("12345.12345"))
                        // ROUND(DECIMAL(10, 5) NOT NULL, 2) => DECIMAL(8, 2) NOT NULL
                        .testResult(
                                $("f0").round(2),
                                "ROUND(f0, 2)",
                                new BigDecimal("12345.12"),
                                DataTypes.DECIMAL(8, 2).notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TRUNCATE)
                        .onFieldsWithData(new BigDecimal("123.456"))
                        // TRUNCATE(DECIMAL(6, 3) NOT NULL, 2) => DECIMAL(6, 2) NOT NULL
                        .testResult(
                                $("f0").truncate(2),
                                "TRUNCATE(f0, 2)",
                                new BigDecimal("123.45"),
                                DataTypes.DECIMAL(6, 2).notNull()));
    }
}

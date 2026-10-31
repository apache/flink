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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** End-to-end tests for Spark-compatible BIT_COUNT in SQL and the Table API. */
class BitCountFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.concat(
                Stream.of(
                        integerCases(
                                DataTypes.TINYINT(),
                                new Number[] {
                                    (byte) 0, (byte) 3, (byte) -1, Byte.MIN_VALUE, Byte.MAX_VALUE
                                },
                                57,
                                7),
                        integerCases(
                                DataTypes.SMALLINT(),
                                new Number[] {
                                    (short) 0,
                                    (short) 3,
                                    (short) -1,
                                    Short.MIN_VALUE,
                                    Short.MAX_VALUE
                                },
                                49,
                                15),
                        integerCases(
                                DataTypes.INT(),
                                new Number[] {0, 3, -1, Integer.MIN_VALUE, Integer.MAX_VALUE},
                                33,
                                31),
                        integerCases(
                                DataTypes.BIGINT(),
                                new Number[] {0L, 3L, -1L, Long.MIN_VALUE, Long.MAX_VALUE},
                                1,
                                63),
                        TestSetSpec.forFunction(BuiltInFunctionDefinitions.BIT_COUNT, "booleans")
                                .onFieldsWithData(true, false, null)
                                .andDataTypes(
                                        DataTypes.BOOLEAN().notNull(),
                                        DataTypes.BOOLEAN(),
                                        DataTypes.BOOLEAN())
                                .testResult(
                                        resultSpec(
                                                $("f0").bitCount(),
                                                "BIT_COUNT(f0)",
                                                1,
                                                DataTypes.INT().notNull()),
                                        resultSpec(
                                                $("f1").bitCount(),
                                                "BIT_COUNT(f1)",
                                                0,
                                                DataTypes.INT()),
                                        resultSpec(
                                                $("f2").bitCount(),
                                                "BIT_COUNT(f2)",
                                                null,
                                                DataTypes.INT())),
                        TestSetSpec.forFunction(
                                        BuiltInFunctionDefinitions.BIT_COUNT, "constant folding")
                                .onFieldsWithData(0)
                                .withConstantFoldingEnabled()
                                .testResult(
                                        resultSpec(
                                                lit(3).bitCount(),
                                                "BIT_COUNT(3)",
                                                2,
                                                DataTypes.INT().notNull()),
                                        resultSpec(
                                                lit(-1).bitCount(),
                                                "BIT_COUNT(-1)",
                                                64,
                                                DataTypes.INT().notNull()),
                                        resultSpec(
                                                lit(true).bitCount(),
                                                "BIT_COUNT(TRUE)",
                                                1,
                                                DataTypes.INT().notNull()),
                                        resultSpec(
                                                lit(null, DataTypes.BIGINT()).bitCount(),
                                                "BIT_COUNT(CAST(NULL AS BIGINT))",
                                                null,
                                                DataTypes.INT()))
                                .testSqlResult("BIT_COUNT(NULL)", null, DataTypes.INT()),
                        TestSetSpec.forFunction(
                                        BuiltInFunctionDefinitions.BIT_COUNT, "argument count")
                                .onFieldsWithData(0)
                                .testSqlValidationError(
                                        "BIT_COUNT()",
                                        "No match found for function signature BIT_COUNT().")
                                .testSqlValidationError(
                                        "BIT_COUNT(1, 2)",
                                        "No match found for function signature "
                                                + "BIT_COUNT(<NUMERIC>, <NUMERIC>).")),
                Stream.of(
                                DataTypes.FLOAT(),
                                DataTypes.DOUBLE(),
                                DataTypes.DECIMAL(10, 2),
                                DataTypes.STRING(),
                                DataTypes.BYTES(),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .map(
                                type ->
                                        TestSetSpec.forFunction(
                                                        BuiltInFunctionDefinitions.BIT_COUNT,
                                                        "reject " + type)
                                                .onFieldsWithData(0)
                                                .testTableApiValidationError(
                                                        lit(null, type).bitCount(),
                                                        "Invalid input arguments")
                                                .testSqlValidationError(
                                                        "BIT_COUNT(CAST(NULL AS " + type + "))",
                                                        "Invalid input arguments")));
    }

    private static TestSetSpec integerCases(
            DataType type, Number[] values, int minimumCount, int maximumCount) {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.BIT_COUNT, type.toString())
                .onFieldsWithData(values[0], values[1], values[2], values[3], values[4], null)
                .andDataTypes(type.notNull(), type, type, type, type, type)
                .testResult(
                        resultSpec(
                                $("f0").bitCount(), "BIT_COUNT(f0)", 0, DataTypes.INT().notNull()),
                        resultSpec($("f1").bitCount(), "BIT_COUNT(f1)", 2, DataTypes.INT()),
                        resultSpec($("f2").bitCount(), "BIT_COUNT(f2)", 64, DataTypes.INT()),
                        resultSpec(
                                $("f3").bitCount(), "BIT_COUNT(f3)", minimumCount, DataTypes.INT()),
                        resultSpec(
                                $("f4").bitCount(), "BIT_COUNT(f4)", maximumCount, DataTypes.INT()),
                        resultSpec($("f5").bitCount(), "BIT_COUNT(f5)", null, DataTypes.INT()));
    }
}

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

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

/** SQL, Table API, coercion, and constant-folding tests for CBRT. */
class CbrtFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.concat(
                Stream.of(
                        numericTypes(),
                        boundaries(),
                        TestSetSpec.forFunction(BuiltInFunctionDefinitions.CBRT, "constant folding")
                                .onFieldsWithData(0)
                                .withConstantFoldingEnabled()
                                .testResult(
                                        resultSpec(
                                                lit(27).cbrt(),
                                                "CBRT(27)",
                                                3.0,
                                                DataTypes.DOUBLE().notNull()),
                                        resultSpec(
                                                lit(null, DataTypes.DOUBLE()).cbrt(),
                                                "CBRT(CAST(NULL AS DOUBLE))",
                                                null,
                                                DataTypes.DOUBLE()))
                                .testSqlResult("CBRT(NULL)", null, DataTypes.DOUBLE()),
                        TestSetSpec.forFunction(BuiltInFunctionDefinitions.CBRT, "argument count")
                                .onFieldsWithData(0)
                                .testSqlValidationError(
                                        "CBRT()", "No match found for function signature CBRT().")
                                .testSqlValidationError(
                                        "CBRT(1, 2)",
                                        "No match found for function signature CBRT(<NUMERIC>, <NUMERIC>).")),
                Stream.of(
                                DataTypes.BOOLEAN(),
                                DataTypes.STRING(),
                                DataTypes.BYTES(),
                                DataTypes.ARRAY(DataTypes.INT()))
                        .map(
                                type ->
                                        TestSetSpec.forFunction(
                                                        BuiltInFunctionDefinitions.CBRT,
                                                        "reject " + type)
                                                .onFieldsWithData(0)
                                                .testTableApiValidationError(
                                                        lit(null, type).cbrt(),
                                                        "Invalid input arguments")
                                                .testSqlValidationError(
                                                        "CBRT(CAST(NULL AS " + type + "))",
                                                        "Invalid input arguments")));
    }

    private static TestSetSpec numericTypes() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.CBRT, "numeric coercion")
                .onFieldsWithData((byte) 1, (short) 1, 1, 1L, 1.0f, 1.0, new BigDecimal("1.00"))
                .andDataTypes(
                        DataTypes.TINYINT().notNull(),
                        DataTypes.SMALLINT(),
                        DataTypes.INT(),
                        DataTypes.BIGINT(),
                        DataTypes.FLOAT(),
                        DataTypes.DOUBLE(),
                        DataTypes.DECIMAL(10, 2))
                .testResult(
                        resultSpec($("f0").cbrt(), "CBRT(f0)", 1.0, DataTypes.DOUBLE().notNull()),
                        resultSpec($("f1").cbrt(), "CBRT(f1)", 1.0, DataTypes.DOUBLE()),
                        resultSpec($("f2").cbrt(), "CBRT(f2)", 1.0, DataTypes.DOUBLE()),
                        resultSpec($("f3").cbrt(), "CBRT(f3)", 1.0, DataTypes.DOUBLE()),
                        resultSpec($("f4").cbrt(), "CBRT(f4)", 1.0, DataTypes.DOUBLE()),
                        resultSpec($("f5").cbrt(), "CBRT(f5)", 1.0, DataTypes.DOUBLE()),
                        resultSpec($("f6").cbrt(), "CBRT(f6)", 1.0, DataTypes.DOUBLE()));
    }

    private static TestSetSpec boundaries() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.CBRT, "boundaries and nulls")
                .onFieldsWithData("-27.0", "8.0", "-0.0", "Infinity", "-Infinity", "NaN", null)
                .andDataTypes(
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING())
                .testResult(
                        resultSpec(
                                $("f0").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f0 AS DOUBLE))",
                                -3.0,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f1").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f1 AS DOUBLE))",
                                2.0,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f2").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f2 AS DOUBLE))",
                                -0.0,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f3").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f3 AS DOUBLE))",
                                Double.POSITIVE_INFINITY,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f4").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f4 AS DOUBLE))",
                                Double.NEGATIVE_INFINITY,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f5").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f5 AS DOUBLE))",
                                Double.NaN,
                                DataTypes.DOUBLE()),
                        resultSpec(
                                $("f6").cast(DataTypes.DOUBLE()).cbrt(),
                                "CBRT(CAST(f6 AS DOUBLE))",
                                null,
                                DataTypes.DOUBLE()));
    }
}

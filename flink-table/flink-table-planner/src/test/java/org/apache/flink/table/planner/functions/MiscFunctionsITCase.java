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
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.callSql;

/** Tests for miscellaneous {@link BuiltInFunctionDefinitions}. */
public class MiscFunctionsITCase extends BuiltInFunctionTestBase {

    @Parameterized.Parameters(name = "{index}: {0}")
    public static List<TestSpec> testData() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.TYPE_OF)
                        .onFieldsWithData(12, "Hello world", false)
                        .testResult(
                                call("TYPEOF", $("f0")),
                                "TYPEOF(f0)",
                                "INT NOT NULL",
                                DataTypes.STRING())
                        .testTableApiValidationError(
                                call("TYPEOF", $("f0"), $("f2")), "Invalid input arguments.")
                        .testSqlValidationError(
                                "TYPEOF(f0, f2)",
                                "SQL validation failed. Invalid function call:\nTYPEOF(INT NOT NULL, BOOLEAN NOT NULL)")
                        .testTableApiResult(
                                call("TYPEOF", $("f1"), true),
                                "CHAR(11) NOT NULL",
                                DataTypes.STRING())
                        .testSqlResult("TYPEOF(NULL)", "NULL", DataTypes.STRING()),
                TestSpec.forFunction(BuiltInFunctionDefinitions.IF_NULL)
                        .onFieldsWithData(null, new BigDecimal("123.45"))
                        .andDataTypes(DataTypes.INT().nullable(), DataTypes.DECIMAL(5, 2).notNull())
                        .withFunction(TakesNotNull.class)
                        .testResult(
                                $("f0").ifNull($("f0")),
                                "IFNULL(f0, f0)",
                                null,
                                DataTypes.INT().nullable())
                        .testResult(
                                $("f0").ifNull($("f1")),
                                "IFNULL(f0, f1)",
                                new BigDecimal("123.45"),
                                DataTypes.DECIMAL(12, 2).notNull())
                        .testResult(
                                $("f1").ifNull($("f0")),
                                "IFNULL(f1, f0)",
                                new BigDecimal("123.45"),
                                DataTypes.DECIMAL(12, 2).notNull())
                        .testSqlValidationError(
                                "IFNULL(SUBSTR(''), f0)",
                                "Invalid number of arguments to function 'SUBSTR'.")
                        .testResult(
                                $("f1").ifNull($("f0")),
                                "IFNULL(f1, f0)",
                                new BigDecimal("123.45"),
                                DataTypes.DECIMAL(12, 2).notNull())
                        .testResult(
                                call("TakesNotNull", $("f0").ifNull(12)),
                                "TakesNotNull(IFNULL(f0, 12))",
                                12,
                                DataTypes.INT().notNull()),
                TestSpec.forExpression("SQL call")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiResult(
                                callSql("f2 || '!'"), "Hello World!", DataTypes.STRING().notNull())
                        .testTableApiResult(callSql("ABS(f0)"), null, DataTypes.INT().nullable())
                        .testTableApiResult(
                                callSql("UPPER(f2)").plus(callSql("LOWER(f2)")).substring(2, 20),
                                "ELLO WORLDhello worl",
                                DataTypes.STRING().notNull())
                        .testTableApiValidationError(
                                callSql("UPPER(f1)"), "Invalid SQL expression: UPPER(f1)"));
    }

    // --------------------------------------------------------------------------------------------

    /** Function that takes a NOT NULL argument. */
    public static class TakesNotNull extends ScalarFunction {
        public int eval(int i) {
            return i;
        }
    }
}

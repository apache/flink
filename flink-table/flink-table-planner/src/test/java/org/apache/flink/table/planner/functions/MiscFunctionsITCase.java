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

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.nullOf;

/** Tests for miscellaneous {@link BuiltInFunctionDefinitions}. */
class MiscFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.TYPE_OF)
                        .onFieldsWithData(12, "Hello world", false)
                        .testResult(
                                call("TYPEOF", $("f0")),
                                "TYPEOF(f0)",
                                "INT NOT NULL",
                                DataTypes.STRING())
                        .testTableApiValidationError(
                                call("TYPEOF", $("f0"), $("f2")),
                                "Invalid function call:\n"
                                        + "TYPEOF(INT NOT NULL, BOOLEAN NOT NULL)")
                        .testSqlValidationError(
                                "TYPEOF(f0, f2)",
                                "SQL validation failed. Invalid function call:\nTYPEOF(INT NOT NULL, BOOLEAN NOT NULL)")
                        .testTableApiResult(
                                call("TYPEOF", $("f1"), true),
                                "CHAR(11) NOT NULL",
                                DataTypes.STRING())
                        .testSqlResult("TYPEOF(NULL)", "NULL", DataTypes.STRING()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.IF_NULL)
                        .onFieldsWithData(null, new BigDecimal("123.45"), "Hello world")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.DECIMAL(5, 2).notNull(),
                                DataTypes.STRING())
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
                                $("f2").ifNull("0"),
                                "IFNULL(f2, '0')",
                                "Hello world",
                                DataTypes.STRING().notNull())
                        .testResult(
                                call("TakesNotNull", $("f0").ifNull(12)),
                                "TakesNotNull(IFNULL(f0, 12))",
                                12,
                                DataTypes.INT().notNull()),
                TestSetSpec.forExpression("SQL call")
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
                                callSql("UPPER(f1)"), "Invalid SQL expression: UPPER(f1)"),
                TestSetSpec.forExpression("assignment with argument reordering")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiResult(
                                call(
                                        OptionalArgInMiddleFunction.class,
                                        nullOf(DataTypes.BOOLEAN()).asArgument("b"),
                                        $("f2").asArgument("s"),
                                        lit("value").asArgument("optional"),
                                        lit(42L).asArgument("l"),
                                        $("f1").asArgument("i")),
                                "i=12,optional=value,l=42,b=null,s=Hello World",
                                DataTypes.STRING()),
                TestSetSpec.forExpression("assignment with optional arguments")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiResult(
                                call(
                                        OptionalArgInMiddleFunction.class,
                                        nullOf(DataTypes.BOOLEAN()).asArgument("b"),
                                        $("f2").asArgument("s"),
                                        lit(42L).asArgument("l"),
                                        $("f1").asArgument("i")),
                                "i=12,optional=null,l=42,b=null,s=Hello World",
                                DataTypes.STRING()),
                TestSetSpec.forExpression("missing assignment")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiValidationError(
                                call(
                                        OptionalArgInMiddleFunction.class,
                                        nullOf(DataTypes.BOOLEAN()).asArgument("b"),
                                        $("f2").asArgument("s")),
                                "If the call uses named arguments, a valid name has to be provided for all passed arguments. "
                                        + "Missing required arguments: [i => INT NOT NULL, l => BIGINT]"),
                TestSetSpec.forExpression("assignment with invalid arguments")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiValidationError(
                                call(
                                        OptionalArgInMiddleFunction.class,
                                        $("f1").asArgument("INVALID"),
                                        lit(42L).asArgument("l"),
                                        nullOf(DataTypes.BOOLEAN()).asArgument("b"),
                                        $("f2").asArgument("s")),
                                "If the call uses named arguments, a valid name has to be provided for all passed arguments. "
                                        + "Unknown argument names: [INVALID]"),
                TestSetSpec.forExpression("invalid assignment")
                        .onFieldsWithData("Hello World")
                        .testTableApiValidationError(
                                $("f0").asArgument("other"),
                                "Named arguments via asArgument() can only be used within function calls."),
                TestSetSpec.forExpression("invalid function for assignment")
                        .onFieldsWithData("Hello World")
                        .testTableApiValidationError(
                                Expressions.coalesce(
                                        $("f0").asArgument("col1"), $("f0").asArgument("col2")),
                                "The function does not support named arguments. "
                                        + "Please pass the arguments based on positions (i.e. without asArgument())."),
                TestSetSpec.forExpression("assignment with duplicate arguments")
                        .onFieldsWithData(null, 12, "Hello World")
                        .andDataTypes(
                                DataTypes.INT().nullable(),
                                DataTypes.INT().notNull(),
                                DataTypes.STRING().notNull())
                        .testTableApiValidationError(
                                call(
                                        OptionalArgInMiddleFunction.class,
                                        $("f1").asArgument("i"),
                                        lit(42L).asArgument("l"),
                                        nullOf(DataTypes.BOOLEAN()).asArgument("b"),
                                        $("f2").asArgument("s"),
                                        $("f2").asArgument("s")),
                                "Duplicate named argument found: s"),
                TestSetSpec.forExpression("position-based optional argument")
                        .onFieldsWithData(12)
                        .andDataTypes(DataTypes.INT().notNull())
                        .testTableApiResult(
                                call(OptionalArgAtEndFunction.class, $("f0")),
                                "i=12,optional=null",
                                DataTypes.STRING()));
    }

    // --------------------------------------------------------------------------------------------

    /** Function that takes a NOT NULL argument. */
    public static class TakesNotNull extends ScalarFunction {
        public int eval(int i) {
            return i;
        }
    }

    /** Function that uses a static signature. */
    public static class OptionalArgInMiddleFunction extends ScalarFunction {
        public String eval(
                int i,
                @ArgumentHint(isOptional = true) String optional,
                Long l,
                Boolean b,
                String s) {
            return String.format("i=%s,optional=%s,l=%s,b=%s,s=%s", i, optional, l, b, s);
        }
    }

    /** Function that uses a static signature with optionals at the end. */
    public static class OptionalArgAtEndFunction extends ScalarFunction {
        public String eval(int i, @ArgumentHint(isOptional = true) String optional) {
            return String.format("i=%s,optional=%s", i, optional);
        }
    }
}

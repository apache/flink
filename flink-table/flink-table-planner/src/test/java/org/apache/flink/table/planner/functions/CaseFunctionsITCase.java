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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Tests for CASE WHEN expression. */
class CaseFunctionsITCase extends BuiltInFunctionTestBase {

    private static final String CASE_STATEMENT =
            longCase(idx -> String.format("when f0 = %d then %d", idx, idx), "9999");

    // every WHEN from f0 on is true
    private static final String OVERLAPPING_CASE_STATEMENT =
            longCase(1000, idx -> String.format("when f0 <= %d then %d", idx, idx), "9999");

    // the last WHEN divides by zero if it runs; it starts the last chunk (21 WHENs, chunks of 5)
    private static final String FAILING_LATER_CASE_STATEMENT =
            "case when f0 = 110 then 0 "
                    + IntStream.range(1, 20)
                            .mapToObj(idx -> String.format("when f0 = -%d then %d", idx, idx))
                            .collect(Collectors.joining(" "))
                    + " when 10 / (f0 - 110) = 1 then 1 else 9999 end";

    private static final String NULL_RESULT_CASE_STATEMENT =
            longCase(
                    idx ->
                            String.format(
                                    "when f0 = %d then %s",
                                    idx, idx == 110 ? "CAST(NULL AS INT)" : idx),
                    "1");

    private static final String NO_ELSE_CASE_STATEMENT =
            longCase(idx -> String.format("when f0 = %d then %d", idx, idx), null);

    // WHEN 110 is a nullable BOOLEAN column
    private static final String NULLABLE_CONDITION_CASE_STATEMENT =
            longCase(
                    idx ->
                            idx == 110
                                    ? "when f1 then -1"
                                    : String.format("when f0 = %d then %d", idx, idx),
                    "9999");

    private static final String STRING_CASE_STATEMENT =
            longCase(
                    idx -> String.format("when f0 = %d then CAST(%d AS STRING)", idx, idx),
                    "CAST(NULL AS STRING)");

    private static final String DECIMAL_CASE_STATEMENT =
            longCase(
                    idx -> String.format("when f0 = %d then CAST(%d AS DECIMAL(10, 2))", idx, idx),
                    "CAST(NULL AS DECIMAL(10, 2))");

    private static final String TIMESTAMP_CASE_STATEMENT =
            longCase(
                    idx ->
                            String.format(
                                    "when f0 = %d then "
                                            + "TIMESTAMPADD(SECOND, %d, TIMESTAMP '2020-01-01 00:00:00')",
                                    idx, idx),
                    "CAST(NULL AS TIMESTAMP(0))");

    private static final String ARRAY_CASE_STATEMENT =
            longCase(
                    idx -> String.format("when f0 = %d then ARRAY[%d]", idx, idx),
                    "CAST(NULL AS ARRAY<INT NOT NULL>)");

    private static final String INNER_CASE_STATEMENT =
            longCase(idx -> String.format("when f1 = %d then %d", idx, idx), "9999");

    // a long CASE in a WHEN and in a THEN of another long CASE
    private static final String NESTED_CASE_STATEMENT =
            longCase(
                    idx -> {
                        if (idx == 100) {
                            return "when (" + INNER_CASE_STATEMENT + ") = -1 then -100";
                        }
                        if (idx == 110) {
                            return "when f0 = 110 then (" + INNER_CASE_STATEMENT + ")";
                        }
                        return String.format("when f0 = %d then %d", idx, idx);
                    },
                    "9999");

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.concat(specs(), chunkBoundarySpecs());
    }

    // matches the last WHEN at chunk-size boundaries (chunks of ceil(sqrt(n)))
    private static Stream<TestSetSpec> chunkBoundarySpecs() {
        return IntStream.of(9, 10, 16, 17)
                .mapToObj(
                        n ->
                                TestSetSpec.forExpression("CASE WHEN with " + n + " WHENs")
                                        .onFieldsWithData(n - 1)
                                        .testSqlResult(
                                                longCase(
                                                        n,
                                                        idx ->
                                                                String.format(
                                                                        "when f0 = %d then %d",
                                                                        idx, idx),
                                                        "9999"),
                                                n - 1,
                                                DataTypes.INT().notNull()));
    }

    private static Stream<TestSetSpec> specs() {
        return Stream.of(
                TestSetSpec.forExpression("CASE WHEN")
                        .onFieldsWithData(110)
                        .testSqlResult(CASE_STATEMENT, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN ELSE")
                        .onFieldsWithData(450)
                        .testSqlResult(CASE_STATEMENT, 9999, DataTypes.INT().notNull()),
                // a later match must not overwrite the first one
                TestSetSpec.forExpression("CASE WHEN first match wins across chunks")
                        .onFieldsWithData(110)
                        .testSqlResult(OVERLAPPING_CASE_STATEMENT, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN skips later conditions")
                        .onFieldsWithData(110)
                        .testSqlResult(FAILING_LATER_CASE_STATEMENT, 0, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN nested")
                        .onFieldsWithData(110, 120)
                        .testSqlResult(NESTED_CASE_STATEMENT, 120, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN NULL result in a later chunk")
                        .onFieldsWithData(110)
                        .testSqlResult(NULL_RESULT_CASE_STATEMENT, null, DataTypes.INT()),
                TestSetSpec.forExpression("CASE WHEN without ELSE and no match")
                        .onFieldsWithData(450)
                        .testSqlResult(NO_ELSE_CASE_STATEMENT, null, DataTypes.INT()),
                TestSetSpec.forExpression("CASE WHEN nullable condition in a later chunk")
                        .onFieldsWithData(120, null)
                        .andDataTypes(DataTypes.INT(), DataTypes.BOOLEAN())
                        .testSqlResult(
                                NULLABLE_CONDITION_CASE_STATEMENT, 120, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN reference result types")
                        .onFieldsWithData(110)
                        .testSqlResult(STRING_CASE_STATEMENT, "110", DataTypes.STRING())
                        .testSqlResult(
                                DECIMAL_CASE_STATEMENT,
                                new BigDecimal("110.00"),
                                DataTypes.DECIMAL(10, 2))
                        .testSqlResult(
                                TIMESTAMP_CASE_STATEMENT,
                                LocalDateTime.of(2020, 1, 1, 0, 1, 50),
                                DataTypes.TIMESTAMP(0))
                        .testSqlResult(
                                ARRAY_CASE_STATEMENT,
                                new Integer[] {110},
                                DataTypes.ARRAY(DataTypes.INT().notNull())),
                TestSetSpec.forExpression("CASE WHEN nullable")
                        .onFieldsWithData(110)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult(CASE_STATEMENT, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN ELSE on NULL")
                        .onFieldsWithData((Integer) null)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult(CASE_STATEMENT, 9999, DataTypes.INT().notNull()),
                // results computed without an if block
                TestSetSpec.forExpression("operators on NULL operands")
                        .onFieldsWithData(null, 1, 0, null, null, false, null)
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.INT().notNull(),
                                DataTypes.INT().notNull(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE(),
                                DataTypes.BOOLEAN().notNull(),
                                DataTypes.TINYINT())
                        // must not divide by zero for a NULL operand
                        .testSqlResult("f0 / f2", null, DataTypes.INT())
                        .testSqlResult("MOD(f0, f2)", null, DataTypes.INT())
                        // primitive results other than BOOLEAN
                        .testSqlResult("f0 + 1", null, DataTypes.INT())
                        .testSqlResult("f3 * 2", null, DataTypes.BIGINT())
                        .testSqlResult("f4 - 1", null, DataTypes.DOUBLE())
                        .testSqlResult("f6 + CAST(1 AS TINYINT)", null, DataTypes.TINYINT())
                        // BOOLEAN results
                        .testSqlResult("f0 = 1", null, DataTypes.BOOLEAN())
                        .testSqlResult("(f0 = 1) IS NULL", true, DataTypes.BOOLEAN().notNull())
                        .testSqlResult("NOT (f0 = 1)", null, DataTypes.BOOLEAN())
                        .testSqlResult("f0 < 5", null, DataTypes.BOOLEAN())
                        .testSqlResult(
                                "CASE WHEN f0 = 1 THEN 'a' ELSE 'b' END",
                                "b",
                                DataTypes.CHAR(1).notNull())
                        // operands that are never null
                        .testSqlResult("f5 = FALSE", true, DataTypes.BOOLEAN().notNull())
                        .testSqlResult("f1 + 1", 2, DataTypes.INT().notNull())
                        .testSqlResult("f1 * 5", 5, DataTypes.INT().notNull())
                        .testSqlResult("f1 < f2", false, DataTypes.BOOLEAN().notNull()));
    }

    /** 250 WHENs; no ELSE if {@code elseValue} is null. */
    private static String longCase(IntFunction<String> when, String elseValue) {
        return longCase(250, when, elseValue);
    }

    private static String longCase(int whens, IntFunction<String> when, String elseValue) {
        return "case "
                + IntStream.range(0, whens).mapToObj(when).collect(Collectors.joining(" "))
                + (elseValue == null ? " end" : " else " + elseValue + " end");
    }
}

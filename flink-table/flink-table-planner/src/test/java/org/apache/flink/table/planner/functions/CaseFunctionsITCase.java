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

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** Tests for CASE WHEN expression. */
class CaseFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {

        String caseStatement =
                "case "
                        + IntStream.range(0, 250)
                                .mapToObj(idx -> String.format("when f0 = %d then %d", idx, idx))
                                .collect(Collectors.joining(" "))
                        + "else 9999 end";

        // overlapping conditions: every WHEN from f0 on is true
        String overlappingCaseStatement =
                "case "
                        + IntStream.range(0, 250)
                                .mapToObj(idx -> String.format("when f0 <= %d then %d", idx, idx))
                                .collect(Collectors.joining(" "))
                        + " else 9999 end";

        // the last WHEN divides by zero for f0 = 110 if it runs
        String failingLaterCaseStatement =
                "case when f0 = 110 then 0 "
                        + IntStream.range(1, 25)
                                .mapToObj(idx -> String.format("when f0 = -%d then %d", idx, idx))
                                .collect(Collectors.joining(" "))
                        + " when 10 / (f0 - 110) = 1 then 1 else 9999 end";

        // Verify a long case when statement which produces deeply nested if else statements
        // works correctly.
        return Stream.of(
                TestSetSpec.forExpression("CASE WHEN")
                        .onFieldsWithData(110)
                        .testSqlResult(caseStatement, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN ELSE")
                        .onFieldsWithData(450)
                        .testSqlResult(caseStatement, 9999, DataTypes.INT().notNull()),
                // overlapping conditions: a later WHEN in another chunk must not overwrite the
                // first match
                TestSetSpec.forExpression("CASE WHEN first match wins across chunks")
                        .onFieldsWithData(110)
                        .testSqlResult(overlappingCaseStatement, 110, DataTypes.INT().notNull()),
                // a WHEN after the match, in a later chunk, would divide by zero if it ran
                TestSetSpec.forExpression("CASE WHEN skips later conditions")
                        .onFieldsWithData(110)
                        .testSqlResult(failingLaterCaseStatement, 0, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN nullable")
                        .onFieldsWithData(110)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult(caseStatement, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN ELSE on NULL")
                        .onFieldsWithData((Integer) null)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult(caseStatement, 9999, DataTypes.INT().notNull()),
                // conditions and other operators on NULL operands, whose result is computed
                // without an if block
                TestSetSpec.forExpression("operators on NULL operands")
                        .onFieldsWithData(null, 1, 0, null, null, false, null)
                        .andDataTypes(
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.INT(),
                                DataTypes.BIGINT(),
                                DataTypes.DOUBLE(),
                                DataTypes.BOOLEAN().notNull(),
                                DataTypes.TINYINT())
                        // the operator must not run for a NULL operand: no division by zero
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
                        .testSqlResult("f1 + 1", 2, DataTypes.INT())
                        .testSqlResult("f1 * 5", 5, DataTypes.INT()));
    }
}

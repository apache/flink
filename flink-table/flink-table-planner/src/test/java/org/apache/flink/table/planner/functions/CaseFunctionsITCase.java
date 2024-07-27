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

        // Verify a long case when statement which produces deeply nested if else statements
        // works correctly.
        return Stream.of(
                TestSetSpec.forExpression("CASE WHEN")
                        .onFieldsWithData(110)
                        .testSqlResult(caseStatement, 110, DataTypes.INT().notNull()),
                TestSetSpec.forExpression("CASE WHEN ELSE")
                        .onFieldsWithData(450)
                        .testSqlResult(caseStatement, 9999, DataTypes.INT().notNull()));
    }
}

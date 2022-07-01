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

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.types.RowKind.INSERT;

/** Tests for built-in FIRST_VALUE/LAST_VALUE aggregation functions. */
public class FirstLastValueAggregationFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                // FIRST_VALUE/LAST_VALUE, which is not ignoring null by default
                TestSpec.forFunction(BuiltInFunctionDefinitions.FIRST_VALUE)
                        .withDescription(
                                "FIRST_VALUE/LAST_VALUE function with not ignoring null by default.")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", null),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "B", 3)))
                        .testResult(
                                source ->
                                        "SELECT f0, FIRST_VALUE(f1), LAST_VALUE(f1) FROM "
                                                + source
                                                + " group by f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        $("f1").firstValue(),
                                                        $("f1").lastValue()),
                                ROW(STRING(), INT(), INT()),
                                Arrays.asList(Row.of("A", 1, null), Row.of("B", null, 3))),

                // FIRST_VALUE/LAST_VALUE with ignoring null as parameter
                TestSpec.forFunction(BuiltInFunctionDefinitions.FIRST_VALUE)
                        .withDescription(
                                "FIRST_VALUE/LAST_VALUE function with ignoring null as parameter.")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", null),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "B", 3)))
                        .testResult(
                                source ->
                                        "SELECT f0, FIRST_VALUE(f1, true), LAST_VALUE(f1, true) FROM "
                                                + source
                                                + " group by f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        $("f1").firstValue(true),
                                                        $("f1").lastValue(true)),
                                ROW(STRING(), INT(), INT()),
                                Arrays.asList(Row.of("A", 1, 1), Row.of("B", 3, 3))),

                // FIRST_VALUE/LAST_VALUE with not ignoring null as parameter
                TestSpec.forFunction(BuiltInFunctionDefinitions.FIRST_VALUE)
                        .withDescription(
                                "FIRST_VALUE/LAST_VALUE function with not ignoring null as parameter.")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", null),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(INSERT, "B", 3)))
                        .testResult(
                                source ->
                                        "SELECT f0, FIRST_VALUE(f1, false), LAST_VALUE(f1, false) FROM "
                                                + source
                                                + " group by f0",
                                source ->
                                        source.groupBy($("f0"))
                                                .select(
                                                        $("f0"),
                                                        $("f1").firstValue(false),
                                                        $("f1").lastValue(false)),
                                ROW(STRING(), INT(), INT()),
                                Arrays.asList(Row.of("A", 1, null), Row.of("B", null, 3))));
    }
}

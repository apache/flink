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
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** Tests for built-in ARRAY_AGG aggregation functions. */
class ArrayAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(
                TestSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_AGG)
                        .withDescription("ARRAY changelog stream aggregation")
                        .withSource(
                                ROW(STRING(), INT()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 1),
                                        Row.ofKind(INSERT, "A", 2),
                                        Row.ofKind(INSERT, "B", 2),
                                        Row.ofKind(INSERT, "B", 2),
                                        Row.ofKind(INSERT, "B", 3),
                                        Row.ofKind(INSERT, "C", 3),
                                        Row.ofKind(INSERT, "C", null),
                                        Row.ofKind(DELETE, "C", null),
                                        Row.ofKind(INSERT, "D", null),
                                        Row.ofKind(INSERT, "E", 4),
                                        Row.ofKind(INSERT, "E", 5),
                                        Row.ofKind(DELETE, "E", 5),
                                        Row.ofKind(UPDATE_BEFORE, "E", 4),
                                        Row.ofKind(UPDATE_AFTER, "E", 6)))
                        .testResult(
                                source ->
                                        "SELECT f0, array_agg(f1) FROM " + source + " GROUP BY f0",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f0")),
                                        $("f0"),
                                        $("f1").arrayAgg()),
                                ROW(STRING(), ARRAY(INT())),
                                ROW(STRING(), ARRAY(INT())),
                                Arrays.asList(
                                        Row.of("A", new Integer[] {1, 2}),
                                        Row.of("B", new Integer[] {2, 2, 3}),
                                        Row.of("C", new Integer[] {3}),
                                        Row.of("D", new Integer[] {null}),
                                        Row.of("E", new Integer[] {6})))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, array_agg(DISTINCT f1 IGNORE NULLS) FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), ARRAY(INT())),
                                Arrays.asList(
                                        Row.of("A", new Integer[] {1, 2}),
                                        Row.of("B", new Integer[] {2, 3}),
                                        Row.of("C", new Integer[] {3}),
                                        Row.of("D", null),
                                        Row.of("E", new Integer[] {6}))));
    }
}

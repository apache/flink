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
import org.apache.flink.types.bitmap.Bitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BITMAP;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** Tests for built-in bitmap aggregation functions. */
class BitmapAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        final List<TestSpec> specs = new ArrayList<>();
        specs.addAll(bitmapBuildAggTestCases());
        return specs.stream();
    }

    private List<TestSpec> bitmapBuildAggTestCases() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD_AGG)
                        .withDescription("without retraction")
                        .withSource(
                                ROW(INT(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1, "A"),
                                        Row.ofKind(INSERT, 2, "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(INSERT, 4, "A"),
                                        Row.ofKind(INSERT, 3, "A"),
                                        Row.ofKind(INSERT, 2, "B"),
                                        Row.ofKind(INSERT, -1, "A"),
                                        Row.ofKind(INSERT, 1, "B"),
                                        Row.ofKind(INSERT, -1, "B"),
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(INSERT, null, "C")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_BUILD_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapBuildAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", Bitmap.fromArray(new int[] {-1, 1, 2, 3, 4})),
                                        Row.of("B", Bitmap.fromArray(new int[] {-1, 1, 2})),
                                        Row.of("C", null))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD_AGG)
                        .withDescription("with retraction")
                        .withSource(
                                ROW(INT(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1, "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(DELETE, 1, "A"),
                                        Row.ofKind(INSERT, 1, "B"),
                                        Row.ofKind(DELETE, 1, "B"),
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(INSERT, 3, "B"),
                                        Row.ofKind(DELETE, 2, "B"), // count < 0
                                        Row.ofKind(INSERT, -1, "B"),
                                        Row.ofKind(INSERT, 2, "B"),
                                        Row.ofKind(INSERT, 2, "B"),
                                        Row.ofKind(INSERT, 1, "B"),
                                        Row.ofKind(DELETE, 1, "B"),
                                        Row.ofKind(UPDATE_BEFORE, 3, "B"),
                                        Row.ofKind(UPDATE_AFTER, 1, "B"),
                                        Row.ofKind(INSERT, 2, "C"),
                                        Row.ofKind(INSERT, 1, "C"),
                                        Row.ofKind(INSERT, -1, "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(DELETE, 1, "C"),
                                        Row.ofKind(DELETE, -1, "C"),
                                        Row.ofKind(DELETE, null, "C"),
                                        Row.ofKind(UPDATE_BEFORE, 2, "C"),
                                        Row.ofKind(UPDATE_AFTER, 1, "C")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_BUILD_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapBuildAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", null),
                                        Row.of("B", Bitmap.fromArray(new int[] {-1, 1, 2})),
                                        Row.of("C", Bitmap.fromArray(new int[] {1})))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD_AGG)
                        .withDescription("Validation Error")
                        .withSource(
                                ROW(BIGINT(), STRING()),
                                Collections.singletonList(Row.ofKind(INSERT, 1L, "A")))
                        .testValidationError(
                                source ->
                                        "SELECT f1, BITMAP_BUILD_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapBuildAgg()),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_BUILD_AGG(value <INTEGER>)"));
    }
}

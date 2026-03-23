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

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BITMAP;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** Tests for built-in bitmap aggregation functions. */
class BitmapAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        final List<TestSpec> specs = new ArrayList<>();
        specs.addAll(bitmapAndAggTestCases());
        specs.addAll(bitmapBuildAggTestCases());
        specs.addAll(bitmapOrAggTestCases());
        specs.addAll(bitmapXorAggTestCases());
        return specs.stream();
    }

    private List<TestSpec> bitmapAndAggTestCases() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_AND_AGG)
                        .withDescription("without retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 4), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(4, 6, 8, 12, 16), "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 1), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -2), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(INSERT, null, "D")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_AND_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapAndAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", fromArray(3)),
                                        Row.of("B", fromArray(4, 6)),
                                        Row.of("C", fromArray(-1)),
                                        Row.of("D", null))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_AND_AGG)
                        .withDescription("with retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(DELETE, fromArray(2, 4, 6), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 2, 3, 4), "B"),
                                        Row.ofKind(DELETE, fromArray(2, 4, 6), "B"), // count < 0
                                        Row.ofKind(INSERT, fromArray(2, 3, 4, 5, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(3, 4, 5), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(INSERT, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(DELETE, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(DELETE, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(DELETE, null, "C"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(1, 2), "C")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_AND_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapAndAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", null),
                                        Row.of("B", fromArray(3, 4)),
                                        Row.of("C", fromArray(1, 2)))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_AND_AGG)
                        .withDescription("Validation Error")
                        .withSource(
                                ROW(INT(), ARRAY(INT()), STRING()),
                                Collections.singletonList(Row.ofKind(INSERT, 1, array(1, 2), "A")))
                        .testValidationError(
                                source ->
                                        "SELECT f2, BITMAP_AND_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f2"),
                                        $("f1").bitmapAndAgg()),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_AND_AGG(bitmap <BITMAP>)"));
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
                                        Row.of("A", fromArray(-1, 1, 2, 3, 4)),
                                        Row.of("B", fromArray(-1, 1, 2)),
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
                                        Row.of("B", fromArray(-1, 1, 2)),
                                        Row.of("C", fromArray(1)))),
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

    private List<TestSpec> bitmapOrAggTestCases() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_OR_AGG)
                        .withDescription("without retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 4), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(4, 6, 8, 12, 16), "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 1), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -2), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(INSERT, null, "D")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_OR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapOrAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", fromArray(1, 2, 3, 4, 5)),
                                        Row.of(
                                                "B",
                                                Bitmap.fromArray(
                                                        new int[] {1, 2, 4, 6, 8, 12, 16})),
                                        Row.of("C", fromArray(0, 1, -2, -1)),
                                        Row.of("D", null))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_OR_AGG)
                        .withDescription("with retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(DELETE, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 2, 3, 4), "B"),
                                        Row.ofKind(DELETE, fromArray(2, 4, 6), "B"), // count < 0
                                        Row.ofKind(INSERT, fromArray(2, 3, 4, 5, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(3, 4, 5), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(INSERT, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(DELETE, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(DELETE, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(DELETE, null, "C"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(1, 2), "C")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_OR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapOrAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", null),
                                        Row.of("B", fromArray(0, 2, 3, 4, 5, 6, -1)),
                                        Row.of("C", fromArray(1, 2)))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_OR_AGG)
                        .withDescription("Validation Error")
                        .withSource(
                                ROW(INT(), ARRAY(INT()), STRING()),
                                Collections.singletonList(Row.ofKind(INSERT, 1, array(1, 2), "A")))
                        .testValidationError(
                                source ->
                                        "SELECT f2, BITMAP_OR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f2"),
                                        $("f1").bitmapOrAgg()),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_OR_AGG(bitmap <BITMAP>)"));
    }

    private List<TestSpec> bitmapXorAggTestCases() {
        return Arrays.asList(
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_XOR_AGG)
                        .withDescription("without retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 4), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(4, 6, 8, 12, 16), "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 1), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -2), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(INSERT, null, "D")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_XOR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapXorAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", fromArray(3, 4, 5)),
                                        Row.of("B", fromArray(1, 4, 6, 8, 12, 16)),
                                        Row.of("C", fromArray(0, 1, -2)),
                                        Row.of("D", null))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_XOR_AGG)
                        .withDescription("with retraction")
                        .withSource(
                                ROW(BITMAP(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, null, "A"),
                                        Row.ofKind(DELETE, fromArray(1, 2, 3), "A"),
                                        Row.ofKind(INSERT, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 3, 5), "B"),
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(INSERT, fromArray(-1, 0, 2, 3, 4), "B"),
                                        Row.ofKind(DELETE, fromArray(2, 4, 6), "B"), // count < 0
                                        Row.ofKind(INSERT, fromArray(2, 3, 4, 5, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(INSERT, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(DELETE, fromArray(1, 4, 7), "B"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 4, 6), "B"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(3, 4, 5), "B"),
                                        Row.ofKind(INSERT, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(INSERT, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(INSERT, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(INSERT, null, "C"),
                                        Row.ofKind(DELETE, fromArray(-1, -3, 0), "C"),
                                        Row.ofKind(DELETE, fromArray(1, 5, 13), "C"),
                                        Row.ofKind(DELETE, null, "C"),
                                        Row.ofKind(UPDATE_BEFORE, fromArray(2, 3, 11), "C"),
                                        Row.ofKind(UPDATE_AFTER, fromArray(1, 2), "C")))
                        .testResult(
                                source ->
                                        "SELECT f1, BITMAP_XOR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f1"),
                                        $("f0").bitmapXorAgg()),
                                ROW(STRING(), BITMAP()),
                                ROW(STRING(), BITMAP()),
                                Arrays.asList(
                                        Row.of("A", null),
                                        Row.of("B", fromArray(0, 3, 4, 6, -1)),
                                        Row.of("C", fromArray(1, 2)))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_XOR_AGG)
                        .withDescription("Validation Error")
                        .withSource(
                                ROW(INT(), ARRAY(INT()), STRING()),
                                Collections.singletonList(Row.ofKind(INSERT, 1, array(1, 2), "A")))
                        .testValidationError(
                                source ->
                                        "SELECT f2, BITMAP_XOR_AGG(f0) FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f2"),
                                        $("f1").bitmapXorAgg()),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_XOR_AGG(bitmap <BITMAP>)"));
    }

    // ~ Utils --------------------------------------------------------------------

    private Bitmap fromArray(int... values) {
        return Bitmap.fromArray(values);
    }
}

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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** Tests for built-in ARRAY_AGG aggregation functions. */
class PercentileAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(percentileTestCases()).flatMap(s -> s);
    }

    private Stream<TestSpec> percentileTestCases() {
        return Stream.of(
                TestSpec.forFunction(BuiltInFunctionDefinitions.PERCENTILE)
                        .withDescription("Double value")
                        .withSource(
                                ROW(DOUBLE(), INT(), STRING()),
                                Arrays.asList(
                                        // normal
                                        Row.ofKind(INSERT, 1.0, 1, "A"),
                                        Row.ofKind(INSERT, 5.0, 2, "A"),
                                        Row.ofKind(INSERT, 4.0, 1, "A"),
                                        Row.ofKind(INSERT, 2.0, 4, "A"),
                                        Row.ofKind(INSERT, 9.0, 3, "A"),
                                        Row.ofKind(INSERT, 1.0, 2, "A"),
                                        Row.ofKind(INSERT, 4.0, 1, "A"),
                                        // retract
                                        Row.ofKind(INSERT, null, 1, "B"),
                                        Row.ofKind(DELETE, null, 1, "B"),
                                        Row.ofKind(INSERT, 3.0, null, "B"),
                                        Row.ofKind(INSERT, 2.0, 3, "B"),
                                        Row.ofKind(INSERT, 2.0, -1, "B"),
                                        Row.ofKind(INSERT, 6.0, 2, "B"),
                                        Row.ofKind(INSERT, 9.0, 1, "B"),
                                        Row.ofKind(DELETE, 9.0, 1, "B"),
                                        Row.ofKind(UPDATE_BEFORE, 2.0, 3, "B"),
                                        Row.ofKind(UPDATE_AFTER, 4.0, 1, "B"),
                                        // retract value with special frequency
                                        Row.ofKind(INSERT, 1.0, 2, "C"),
                                        Row.ofKind(INSERT, 2.0, 1, "C"),
                                        Row.ofKind(INSERT, 2.0, -1, "C"),
                                        Row.ofKind(INSERT, 2.0, null, "C"),
                                        Row.ofKind(DELETE, 2.0, 1, "C"),
                                        Row.ofKind(DELETE, 2.0, -1, "C"),
                                        Row.ofKind(DELETE, 2.0, null, "C"),
                                        Row.ofKind(UPDATE_BEFORE, 1.0, 2, "C"),
                                        Row.ofKind(UPDATE_AFTER, 2.0, 1, "C")))
                        // SinglePercentile without frequency
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, 0.5), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(0.5),
                                        $("f2")),
                                ROW(DOUBLE(), STRING()),
                                ROW(DOUBLE(), STRING()),
                                Arrays.asList(Row.of(4.0, "A"), Row.of(3.5, "B"), Row.of(2.0, "C")))
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, 0.5), PERCENTILE(f0, 0.3), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(0.5),
                                        $("f0").percentile(0.3),
                                        $("f2")),
                                ROW(DOUBLE(), DOUBLE(), STRING()),
                                ROW(DOUBLE(), DOUBLE(), STRING()),
                                Arrays.asList(
                                        Row.of(4.0, 1.7999999999999998, "A"),
                                        Row.of(3.5, 2.9, "B"),
                                        Row.of(2.0, 2.0, "C")))
                        // SinglePercentile with frequency
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, 0.5, f1), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(0.5, $("f1")),
                                        $("f2")),
                                ROW(DOUBLE(), STRING()),
                                ROW(DOUBLE(), STRING()),
                                Arrays.asList(Row.of(3.0, "A"), Row.of(6.0, "B"), Row.of(2.0, "C")))
                        // MultiPercentile without frequency
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, ARRAY[0.9, 0.7, 0.3, 1.0]), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(new double[] {0.9, 0.7, 0.3, 1.0}),
                                        $("f2")),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                Arrays.asList(
                                        Row.of(
                                                new Double[] {
                                                    6.600000000000001,
                                                    4.199999999999999,
                                                    1.7999999999999998,
                                                    9.0
                                                },
                                                "A"),
                                        Row.of(
                                                new Double[] {5.4, 4.199999999999999, 2.9, 6.0},
                                                "B"),
                                        Row.of(new Double[] {2.0, 2.0, 2.0, 2.0}, "C")))
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, ARRAY_REMOVE(ARRAY[0.0], 0.0)), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(array(0.0).arrayRemove(0.0)),
                                        $("f2")),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                Arrays.asList(
                                        Row.of(null, "A"), Row.of(null, "B"), Row.of(null, "C")))
                        // MultiPercentile with frequency
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, ARRAY[0.9, 0.7, 0.3, 1.0], f1), f2 FROM "
                                                + source
                                                + " GROUP BY f2",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f2")),
                                        $("f0").percentile(
                                                        new double[] {0.9, 0.7, 0.3, 1.0}, $("f1")),
                                        $("f2")),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                ROW(ARRAY(DOUBLE()), STRING()),
                                Arrays.asList(
                                        Row.of(new Double[] {9.0, 5.0, 2.0, 9.0}, "A"),
                                        Row.of(new Double[] {6.0, 6.0, 5.2, 6.0}, "B"),
                                        Row.of(new Double[] {2.0, 2.0, 2.0, 2.0}, "C"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.PERCENTILE)
                        .withDescription("DecimalData value")
                        .withSource(
                                ROW(DECIMAL(2, 1), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, null, "B"),
                                        Row.ofKind(DELETE, null, "B"),
                                        Row.ofKind(INSERT, BigDecimal.valueOf(3.0), "B"),
                                        Row.ofKind(INSERT, BigDecimal.valueOf(2.0), "B"),
                                        Row.ofKind(INSERT, BigDecimal.valueOf(2.0), "B"),
                                        Row.ofKind(INSERT, BigDecimal.valueOf(9.0), "B"),
                                        Row.ofKind(DELETE, BigDecimal.valueOf(2.0), "B"),
                                        Row.ofKind(UPDATE_BEFORE, BigDecimal.valueOf(2.0), "B"),
                                        Row.ofKind(UPDATE_AFTER, BigDecimal.valueOf(5.0), "B")))
                        .testResult(
                                source ->
                                        "SELECT PERCENTILE(f0, 0.1), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f0").percentile(0.1),
                                        $("f1")),
                                ROW(DOUBLE(), STRING()),
                                ROW(DOUBLE(), STRING()),
                                Arrays.asList(Row.of(3.4000000000000004, "B"))),
                TestSpec.forFunction(BuiltInFunctionDefinitions.PERCENTILE)
                        .withDescription("Validation Error")
                        .withSource(
                                ROW(DOUBLE(), STRING()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, 1.0, "A"),
                                        Row.ofKind(INSERT, 2.0, "A"),
                                        Row.ofKind(INSERT, 3.0, "A")))
                        .testValidationError(
                                source ->
                                        "SELECT PERCENTILE(f0, 1.5), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f0").percentile(1.5),
                                        $("f1")),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "PERCENTILE(expr <NUMERIC>, percentage [<NUMERIC NOT NULL> | ARRAY<NUMERIC NOT NULL> NOT NULL])\n"
                                        + "PERCENTILE(expr <NUMERIC>, percentage [<NUMERIC NOT NULL> | ARRAY<NUMERIC NOT NULL> NOT NULL], frequency <INTEGER_NUMERIC>)")
                        .testValidationError(
                                source ->
                                        "SELECT PERCENTILE(f0, -1), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                TableApiAggSpec.groupBySelect(
                                        Collections.singletonList($("f1")),
                                        $("f0").percentile(-1),
                                        $("f1")),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "PERCENTILE(expr <NUMERIC>, percentage [<NUMERIC NOT NULL> | ARRAY<NUMERIC NOT NULL> NOT NULL])\n"
                                        + "PERCENTILE(expr <NUMERIC>, percentage [<NUMERIC NOT NULL> | ARRAY<NUMERIC NOT NULL> NOT NULL], frequency <INTEGER_NUMERIC>)"),
                TestSpec.forFunction(BuiltInFunctionDefinitions.PERCENTILE)
                        .withDescription("Runtime Error")
                        .withSource(
                                ROW(DOUBLE(), STRING()),
                                Arrays.asList(Row.ofKind(INSERT, 1.0, "A")))
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT PERCENTILE(f0, 1.0 + 2.0), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                null,
                                IllegalArgumentException.class,
                                "Percentage of PERCENTILE should be between [0.0, 1.0], but was '3.0'.")
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT PERCENTILE(f0, 1.0 - 2.0, 2), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                null,
                                IllegalArgumentException.class,
                                "Percentage of PERCENTILE should be between [0.0, 1.0], but was '-1.0'.")
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT PERCENTILE(f0, ARRAY[0.0, 1.5]), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                null,
                                IllegalArgumentException.class,
                                "Percentage of PERCENTILE should be between [0.0, 1.0], but was '1.5'.")
                        .testSqlRuntimeError(
                                source ->
                                        "SELECT PERCENTILE(f0, ARRAY[0.5, -2.0], 2), f1 FROM "
                                                + source
                                                + " GROUP BY f1",
                                null,
                                IllegalArgumentException.class,
                                "Percentage of PERCENTILE should be between [0.0, 1.0], but was '-2.0'."));
    }
}

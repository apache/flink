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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.types.RowKind.DELETE;
import static org.apache.flink.types.RowKind.INSERT;
import static org.apache.flink.types.RowKind.UPDATE_AFTER;
import static org.apache.flink.types.RowKind.UPDATE_BEFORE;

/** Tests for built-in math aggregation functions. */
class MathAggFunctionITCase extends BuiltInAggregateFunctionTestBase {

    @Override
    Stream<TestSpec> getTestCaseSpecs() {
        return Stream.of(varianceRelatedTestCases()).flatMap(s -> s);
    }

    private Stream<TestSpec> varianceRelatedTestCases() {
        List<Row> batchData = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            batchData.add(Row.ofKind(INSERT, "A", (double) i));
            batchData.add(Row.ofKind(INSERT, "B", null));
            batchData.add(Row.ofKind(INSERT, "B", null));
            batchData.add(Row.ofKind(INSERT, "B", (double) i));
            batchData.add(Row.ofKind(DELETE, "B", (double) i));
            batchData.add(Row.ofKind(DELETE, "B", (double) i + 50));
        }

        return Stream.of(
                TestSpec.forExpression("failed case of FLINK-38740")
                        .withSource(
                                ROW(STRING(), DOUBLE()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", 0.27),
                                        Row.ofKind(INSERT, "A", 0.27),
                                        Row.ofKind(INSERT, "A", 0.27)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "STDDEV_POP(f1), STDDEV_SAMP(f1), VAR_POP(f1) < 0, VAR_SAMP(f1) < 0 "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), DOUBLE(), DOUBLE(), BOOLEAN(), BOOLEAN()),
                                List.of(Row.of("A", 0.0, 0.0, false, false))),
                TestSpec.forExpression("integer numeric")
                        .withSource(
                                ROW(STRING(), TINYINT(), SMALLINT(), INT(), BIGINT()),
                                Arrays.asList(
                                        Row.of("A", (byte) 0x1, (short) 0x10, 0x100, 0x1000L),
                                        Row.of("A", (byte) 0x2, (short) 0x20, 0x200, 0x2000L),
                                        Row.of("A", (byte) 0x3, (short) 0x30, 0x300, 0x3000L)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "STDDEV_POP(f1), STDDEV_SAMP(f1), VAR_POP(f1), VAR_SAMP(f1)  "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), TINYINT(), TINYINT(), TINYINT(), TINYINT()),
                                List.of(Row.of("A", (byte) 0, (byte) 1, (byte) 0, (byte) 1)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "STDDEV_POP(f2), STDDEV_SAMP(f2), VAR_POP(f2), VAR_SAMP(f2)  "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), SMALLINT(), SMALLINT(), SMALLINT(), SMALLINT()),
                                List.of(
                                        Row.of(
                                                "A",
                                                (short) 13,
                                                (short) 16,
                                                (short) 170,
                                                (short) 256)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "STDDEV_POP(f3), STDDEV_SAMP(f3), VAR_POP(f3), VAR_SAMP(f3)  "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), INT(), INT(), INT(), INT()),
                                List.of(Row.of("A", 209, 256, 43690, 65536)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "STDDEV_POP(f4), STDDEV_SAMP(f4), VAR_POP(f4), VAR_SAMP(f4)  "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), BIGINT(), BIGINT(), BIGINT(), BIGINT()),
                                List.of(Row.of("A", 3344L, 4096L, 11184810L, 16777216L))),
                TestSpec.forExpression("approximate numeric")
                        .withSource(
                                ROW(STRING(), FLOAT(), DOUBLE()),
                                Arrays.asList(
                                        Row.of("A", 0.1f, 1.0d),
                                        Row.of("A", 0.2f, 1.5d),
                                        Row.of("A", 0.3f, 2.0d)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "ROUND(STDDEV_POP(f1), 6), ROUND(STDDEV_SAMP(f1), 6), "
                                                + "ROUND(VAR_POP(f1), 6), ROUND(VAR_SAMP(f1), 6) "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), FLOAT(), FLOAT(), FLOAT(), FLOAT()),
                                List.of(Row.of("A", 0.081650f, 0.100000f, 0.006667f, 0.010000f)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "ROUND(STDDEV_POP(f2), 6), ROUND(STDDEV_SAMP(f2), 6), "
                                                + "ROUND(VAR_POP(f2), 6), ROUND(VAR_SAMP(f2), 6) "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()),
                                List.of(Row.of("A", 0.408248, 0.500000, 0.166667, 0.250000))),
                TestSpec.forExpression("decimal")
                        .withSource(
                                ROW(STRING(), DECIMAL(10, 2)),
                                Arrays.asList(
                                        Row.of("A", BigDecimal.valueOf(0.27)),
                                        Row.of("A", BigDecimal.valueOf(0.28)),
                                        Row.of("A", BigDecimal.valueOf(0.29))))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "ROUND(STDDEV_POP(f1), 6), ROUND(STDDEV_SAMP(f1), 6), "
                                                + "ROUND(VAR_POP(f1), 6), ROUND(VAR_SAMP(f1), 6) "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(
                                        STRING(),
                                        DECIMAL(38, 6),
                                        DECIMAL(38, 6),
                                        DECIMAL(38, 6),
                                        DECIMAL(38, 6)),
                                List.of(
                                        Row.of(
                                                "A",
                                                BigDecimal.valueOf(8165L, 6),
                                                BigDecimal.valueOf(10000L, 6),
                                                BigDecimal.valueOf(67L, 6),
                                                BigDecimal.valueOf(100L, 6)))),
                TestSpec.forExpression("retract")
                        .withSource(
                                ROW(STRING(), DOUBLE()),
                                Arrays.asList(
                                        Row.ofKind(INSERT, "A", null),
                                        Row.ofKind(INSERT, "B", 0.27),
                                        Row.ofKind(INSERT, "B", null),
                                        Row.ofKind(DELETE, "B", 0.27),
                                        Row.ofKind(INSERT, "C", 0.27),
                                        Row.ofKind(INSERT, "C", 0.28),
                                        Row.ofKind(INSERT, "C", null),
                                        Row.ofKind(INSERT, "C", null),
                                        Row.ofKind(INSERT, "C", null),
                                        Row.ofKind(DELETE, "C", null),
                                        Row.ofKind(UPDATE_BEFORE, "C", 0.27),
                                        Row.ofKind(UPDATE_AFTER, "C", 0.30),
                                        Row.ofKind(DELETE, "C", 0.27),
                                        Row.ofKind(DELETE, "C", 0.30),
                                        Row.ofKind(DELETE, "C", 0.28),
                                        Row.ofKind(INSERT, "C", 0.27),
                                        Row.ofKind(INSERT, "C", 0.50),
                                        Row.ofKind(INSERT, "D", 0.27),
                                        Row.ofKind(INSERT, "D", 0.28),
                                        Row.ofKind(INSERT, "D", 0.29),
                                        Row.ofKind(DELETE, "D", 0.28)))
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "ROUND(STDDEV_POP(f1), 6), ROUND(STDDEV_SAMP(f1), 6), "
                                                + "ROUND(VAR_POP(f1), 6), ROUND(VAR_SAMP(f1), 6) "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()),
                                List.of(
                                        Row.of("A", null, null, null, null),
                                        Row.of("B", null, null, null, null),
                                        Row.of("C", 0.000000, null, 0.000000, null),
                                        Row.of("D", 0.010000, 0.014142, 0.000100, 0.000200))),
                TestSpec.forExpression("merge")
                        .withConfiguration(
                                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, Boolean.TRUE)
                        .withConfiguration(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 10L)
                        .withConfiguration(
                                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                                Duration.ofMillis(100))
                        .withSource(ROW(STRING(), DOUBLE()), batchData)
                        .testSqlResult(
                                source ->
                                        "SELECT f0, "
                                                + "ROUND(STDDEV_POP(f1), 6), ROUND(STDDEV_SAMP(f1), 6), "
                                                + "ROUND(VAR_POP(f1), 6), ROUND(VAR_SAMP(f1), 6) "
                                                + "FROM "
                                                + source
                                                + " GROUP BY f0",
                                ROW(STRING(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()),
                                List.of(
                                        Row.of("A", 14.430870, 14.577380, 208.250000, 212.500000),
                                        Row.of("B", null, null, null, null))));
    }
}

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

package org.apache.flink.table.planner.runtime.common.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.utils.TableITCaseBase;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.assertj.core.api.Assertions.assertThat;

/** End-to-end tests for floating-point IN predicates on runtime data. */
class FloatingPointInITCase extends TableITCaseBase {

    @ParameterizedTest
    @CsvSource({"BATCH, FLOAT", "BATCH, DOUBLE", "STREAMING, FLOAT", "STREAMING, DOUBLE"})
    void testSignedZero(RuntimeExecutionMode mode, String type) throws Exception {
        final List<Row> input =
                Arrays.asList(
                        Row.of(1, -0.0f, -0.0d),
                        Row.of(1, -0.0f, -0.0d),
                        Row.of(2, 0.0f, 0.0d),
                        Row.of(3, 1.0f, 1.0d),
                        Row.of(4, null, null),
                        Row.of(5, 22.0f, 22.0d),
                        Row.of(6, Float.NaN, Double.NaN),
                        Row.of(7, Float.POSITIVE_INFINITY, Double.POSITIVE_INFINITY),
                        Row.of(8, Float.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY));
        assertThat(Float.floatToRawIntBits((Float) input.get(0).getField(1)))
                .isEqualTo(Integer.MIN_VALUE);
        assertThat(Double.doubleToRawLongBits((Double) input.get(0).getField(2)))
                .isEqualTo(Long.MIN_VALUE);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(mode);
        env.setParallelism(1);
        final StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(
                        env,
                        mode == RuntimeExecutionMode.BATCH
                                ? EnvironmentSettings.inBatchMode()
                                : EnvironmentSettings.inStreamingMode());
        tableEnv.createTemporaryView(
                "T",
                tableEnv.fromDataStream(
                        env.fromCollection(
                                input,
                                Types.ROW_NAMED(
                                        new String[] {"id", "f", "d"},
                                        Types.INT,
                                        Types.FLOAT,
                                        Types.DOUBLE))));
        assertThat(tableEnv.from("T").getResolvedSchema().getColumnDataTypes())
                .containsSequence(FLOAT(), DOUBLE());
        final List<Row> zeros = collect(tableEnv, "SELECT f, d FROM T WHERE id = 1");
        assertThat(zeros).hasSize(2);
        for (Row row : zeros) {
            assertThat(Float.floatToRawIntBits((Float) row.getField(0)))
                    .isEqualTo(Integer.MIN_VALUE);
            assertThat(Double.doubleToRawLongBits((Double) row.getField(1)))
                    .isEqualTo(Long.MIN_VALUE);
        }

        final String field = type.equals("FLOAT") ? "f" : "d";
        final String zero = "CAST(0 AS " + type + ")";
        final String pair = zero + ", CAST(2 AS " + type + ")";
        final String negativePair = "CAST('-0.0' AS " + type + "), CAST(2 AS " + type + ")";
        final String longList =
                IntStream.rangeClosed(2, 21)
                        .mapToObj(value -> "CAST(" + value + " AS " + type + ")")
                        .collect(Collectors.joining(", ", zero + ", ", ""));

        final String equality = "SELECT id FROM T WHERE " + field + " = " + zero;
        assertThat(generatedCode(tableEnv, equality)).doesNotContain("HashSet");
        assertThat(collect(tableEnv, equality))
                .containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(2));

        for (String values :
                Arrays.asList(zero, pair, longList, negativePair, zero + ", " + negativePair)) {
            final String predicate = field + " IN (" + values + ")";
            final String query = "SELECT id FROM T WHERE " + predicate;
            if (values.equals(zero)) {
                assertThat(generatedCode(tableEnv, query)).doesNotContain("HashSet");
            } else {
                assertThat(generatedCode(tableEnv, query))
                        .contains(type.equals("FLOAT") ? "FloatHashSet" : "DoubleHashSet")
                        .contains(".contains(");
            }
            assertThat(collect(tableEnv, query))
                    .containsExactlyInAnyOrder(Row.of(1), Row.of(1), Row.of(2));
            assertThat(
                            collect(
                                    tableEnv,
                                    "SELECT id, "
                                            + predicate
                                            + ", NOT ("
                                            + predicate
                                            + "), "
                                            + field
                                            + " IN ("
                                            + values
                                            + ", CAST(NULL AS "
                                            + type
                                            + ")), "
                                            + field
                                            + " NOT IN ("
                                            + values
                                            + ", CAST(NULL AS "
                                            + type
                                            + ")) FROM T"))
                    .containsExactlyInAnyOrder(
                            Row.of(1, true, false, true, false),
                            Row.of(1, true, false, true, false),
                            Row.of(2, true, false, true, false),
                            Row.of(3, false, true, null, null),
                            Row.of(4, null, null, null, null),
                            Row.of(5, false, true, null, null),
                            Row.of(6, false, true, null, null),
                            Row.of(7, false, true, null, null),
                            Row.of(8, false, true, null, null));
        }
    }

    private static List<Row> collect(StreamTableEnvironment tableEnv, String query)
            throws Exception {
        final List<Row> result = new ArrayList<>();
        try (CloseableIterator<Row> rows = tableEnv.executeSql(query).collect()) {
            rows.forEachRemaining(result::add);
        }
        return result;
    }

    private static String generatedCode(StreamTableEnvironment tableEnv, String query) {
        final Table table = tableEnv.sqlQuery(query);
        final Transformation<?> root = tableEnv.toDataStream(table).getTransformation();
        final StringBuilder code = new StringBuilder();
        for (Transformation<?> transformation : root.getTransitivePredecessors()) {
            if (transformation instanceof OneInputTransformation) {
                final OneInputTransformation<?, ?> oneInput =
                        (OneInputTransformation<?, ?>) transformation;
                if (oneInput.getOperatorFactory() instanceof CodeGenOperatorFactory) {
                    code.append(
                            ((CodeGenOperatorFactory<?>) oneInput.getOperatorFactory())
                                    .getGeneratedClass()
                                    .getCode());
                }
            }
        }
        assertThat(code.toString()).isNotEmpty();
        return code.toString();
    }
}

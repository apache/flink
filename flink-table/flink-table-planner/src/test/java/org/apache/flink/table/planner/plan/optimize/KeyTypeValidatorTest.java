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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.api.common.RuntimeExecutionMode.BATCH;
import static org.apache.flink.api.common.RuntimeExecutionMode.STREAMING;
import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.UNBOUNDED_ROW;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KeyTypeValidator}. */
class KeyTypeValidatorTest {

    private static final String GROUPING_KEY =
            "Column 'v' of type VARIANT cannot be used as a grouping key";

    private static final String DISTINCT_ARGUMENT =
            "of type VARIANT cannot be used as an argument of a DISTINCT aggregate";

    private static final String COMPARISON =
            "An expression of type VARIANT cannot be used in a comparison with";

    private static final String EQUALS = COMPARISON + " '='";

    private static Stream<Arguments> rejectedQueries() {
        final Stream<Arguments> bothModes =
                inBothModes(
                        Arguments.of(
                                "SELECT v, COUNT(*) FROM t GROUP BY v",
                                "Column 'v' of type VARIANT cannot be used as a grouping key, "
                                        + "because the type has no equality. "
                                        + "Cast the value to a comparable type first."),
                        Arguments.of(
                                "SELECT COUNT(*) FROM t GROUP BY r",
                                "Column 'r' of type ROW<`x` VARIANT> cannot be used as a grouping key"),
                        Arguments.of(
                                "SELECT COUNT(*) FROM t GROUP BY a",
                                "Column 'a' of type ARRAY<VARIANT> cannot be used as a grouping key"),
                        Arguments.of(
                                "SELECT COUNT(*) FROM t GROUP BY v['a']",
                                "An expression of type VARIANT cannot be used as a grouping key"),
                        Arguments.of(
                                "SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((v), (id))",
                                GROUPING_KEY),
                        Arguments.of(
                                "SELECT COUNT(*) FROM t GROUP BY ROLLUP (v, id)", GROUPING_KEY),
                        Arguments.of("SELECT DISTINCT v FROM t", GROUPING_KEY),
                        Arguments.of(
                                "SELECT COUNT(DISTINCT v) FROM t",
                                "Column 'v' " + DISTINCT_ARGUMENT),
                        Arguments.of(
                                "SELECT COUNT(DISTINCT v) OVER (PARTITION BY id ORDER BY ts) FROM t",
                                "An expression " + DISTINCT_ARGUMENT),
                        Arguments.of(
                                "SELECT window_start, v, COUNT(*) FROM TABLE("
                                        + "TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                        + "GROUP BY window_start, window_end, v",
                                GROUPING_KEY),
                        Arguments.of(
                                "SELECT window_start, COUNT(*) FROM TABLE("
                                        + "SESSION(TABLE t PARTITION BY v, DESCRIPTOR(ts), INTERVAL '1' MINUTE)) "
                                        + "GROUP BY window_start, window_end",
                                "Column 'v' of type VARIANT cannot be used as a PARTITION BY key"),
                        Arguments.of(
                                "SELECT v FROM t UNION SELECT v FROM u",
                                "Column 'v' of type VARIANT cannot be used in UNION"),
                        Arguments.of(
                                "SELECT v FROM t INTERSECT SELECT v FROM u",
                                "Column 'v' of type VARIANT cannot be used in INTERSECT"),
                        Arguments.of(
                                "SELECT v FROM t INTERSECT ALL SELECT v FROM u",
                                "Column 'v' of type VARIANT cannot be used in INTERSECT ALL"),
                        Arguments.of(
                                "SELECT v FROM t EXCEPT SELECT v FROM u",
                                "Column 'v' of type VARIANT cannot be used in EXCEPT"),
                        Arguments.of(
                                "SELECT v FROM t EXCEPT ALL SELECT v FROM u",
                                "Column 'v' of type VARIANT cannot be used in EXCEPT ALL"),
                        Arguments.of(
                                "SELECT v FROM t ORDER BY v",
                                "Column 'v' of type VARIANT cannot be used as an ORDER BY key, "
                                        + "because the type has no order."),
                        Arguments.of(
                                "SELECT ts, v FROM t ORDER BY ts, v",
                                "Column 'v' of type VARIANT cannot be used as an ORDER BY key"),
                        Arguments.of(
                                "SELECT COUNT(*) OVER (PARTITION BY v ORDER BY ts) FROM t",
                                "An expression of type VARIANT cannot be used as a PARTITION BY key "
                                        + "of an OVER window"),
                        Arguments.of(
                                "SELECT * FROM (SELECT id, ROW_NUMBER() OVER "
                                        + "(PARTITION BY v ORDER BY ts) AS rn FROM t) WHERE rn = 1",
                                "An expression of type VARIANT cannot be used as a PARTITION BY key "
                                        + "of an OVER window"),
                        Arguments.of(
                                "SELECT * FROM (SELECT id, ROW_NUMBER() OVER "
                                        + "(PARTITION BY id ORDER BY v) AS rn FROM t) WHERE rn <= 3",
                                "An expression of type VARIANT cannot be used as an ORDER BY key "
                                        + "of an OVER window"),
                        Arguments.of("SELECT t.id FROM t JOIN u ON t.v = u.v", EQUALS),
                        Arguments.of("SELECT t.id FROM t JOIN u USING (v)", EQUALS),
                        Arguments.of("SELECT id FROM t WHERE v = PARSE_JSON('1')", EQUALS),
                        Arguments.of("SELECT id FROM t WHERE v <> v", COMPARISON + " '<>'"),
                        Arguments.of(
                                "SELECT id FROM t WHERE v < v",
                                COMPARISON + " '<', because the type has no order."),
                        Arguments.of(
                                "SELECT id FROM t WHERE v BETWEEN PARSE_JSON('1') AND PARSE_JSON('2')",
                                COMPARISON),
                        // the SQL converter expands IS [NOT] DISTINCT FROM into =
                        Arguments.of(
                                "SELECT id FROM t WHERE v IS DISTINCT FROM PARSE_JSON('1')",
                                EQUALS),
                        Arguments.of(
                                "SELECT id FROM t WHERE v IS NOT DISTINCT FROM PARSE_JSON('1')",
                                EQUALS),
                        Arguments.of(
                                "SELECT id FROM t WHERE r = r",
                                "An expression of type ROW<`x` VARIANT> cannot be used in a comparison "
                                        + "with '='"),
                        Arguments.of(
                                "SELECT id FROM t WHERE v IN (PARSE_JSON('1'), PARSE_JSON('2'))",
                                EQUALS),
                        Arguments.of(
                                "SELECT id FROM t WHERE v IN (SELECT v FROM u)",
                                COMPARISON + " 'IN'"),
                        Arguments.of(
                                "SELECT id FROM t WHERE v > ALL (SELECT v FROM u)", COMPARISON),
                        Arguments.of(
                                "SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.v = t.v)",
                                EQUALS),
                        Arguments.of(
                                "SELECT CASE v WHEN PARSE_JSON('1') THEN 1 END FROM t", EQUALS),
                        Arguments.of("SELECT NULLIF(v, PARSE_JSON('1')) FROM t", EQUALS),
                        Arguments.of(
                                "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY v ORDER BY ts "
                                        + "MEASURES A.id AS aid PATTERN (A) DEFINE A AS A.id > 0)",
                                "Column 'v' of type VARIANT cannot be used as a PARTITION BY key "
                                        + "of MATCH_RECOGNIZE"),
                        Arguments.of(
                                "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY id ORDER BY ts, v "
                                        + "MEASURES A.id AS aid PATTERN (A) DEFINE A AS A.id > 0)",
                                "Column 'v' of type VARIANT cannot be used as an ORDER BY key "
                                        + "of MATCH_RECOGNIZE"),
                        Arguments.of(
                                "SELECT * FROM t MATCH_RECOGNIZE (PARTITION BY id ORDER BY ts "
                                        + "MEASURES A.id AS aid PATTERN (A B) "
                                        + "DEFINE B AS B.v = A.v)",
                                EQUALS),
                        Arguments.of(
                                "SELECT * FROM f(r => TABLE t PARTITION BY v)",
                                "Column 'v' of type VARIANT cannot be used as a PARTITION BY key "
                                        + "of a table argument"),
                        Arguments.of(
                                "SELECT * FROM f(r => TABLE t PARTITION BY id ORDER BY (ts, v))",
                                "Column 'v' of type VARIANT cannot be used as an ORDER BY key "
                                        + "of a table argument"));
        // batch mode does not apply watermarks
        final Stream<Arguments> streamingOnly =
                inMode(STREAMING, Arguments.of("SELECT id FROM w", EQUALS));
        return Stream.concat(bothModes, streamingOnly);
    }

    private static Stream<Arguments> allowedQueries() {
        return inBothModes(
                Arguments.of("SELECT v FROM t UNION ALL SELECT v FROM u"),
                Arguments.of("SELECT t.v, u.v FROM t JOIN u ON t.id = u.id"),
                Arguments.of("SELECT v, COUNT(*) OVER (PARTITION BY id ORDER BY ts) FROM t"),
                Arguments.of("SELECT v, ts FROM t ORDER BY ts"),
                Arguments.of("SELECT DISTINCT id, CAST(v AS STRING) FROM t"),
                Arguments.of("SELECT v FROM t WHERE v IS NOT NULL AND r IS NULL"),
                Arguments.of(
                        "SELECT v FROM t WHERE v IS DISTINCT FROM NULL "
                                + "OR v IS NOT DISTINCT FROM NULL"));
    }

    private static Stream<Arguments> rejectedJoinsPerBatchStrategy() {
        return Stream.of(
                        Named.of("SortMergeJoin", "HashJoin, NestedLoopJoin"),
                        Named.of(
                                "ShuffleHashJoin",
                                "SortMergeJoin, NestedLoopJoin, BroadcastHashJoin"),
                        Named.of(
                                "BroadcastHashJoin",
                                "SortMergeJoin, NestedLoopJoin, ShuffleHashJoin"),
                        Named.of("NestedLoopJoin", "SortMergeJoin, HashJoin"))
                .flatMap(
                        disabledOperators ->
                                Stream.of(
                                        Arguments.of(
                                                "SELECT t.id FROM t JOIN u ON t.v = u.v",
                                                disabledOperators),
                                        Arguments.of(
                                                "SELECT t.id FROM t JOIN u ON t.v['id'] = u.v['id']",
                                                disabledOperators)));
    }

    private static Stream<Arguments> rejectedTableApiQueries() {
        final Stream<Arguments> bothModes =
                inBothModes(
                        tableApi(
                                "groupBy",
                                env ->
                                        env.from("t")
                                                .groupBy($("v"))
                                                .select($("v"), $("id").count()),
                                GROUPING_KEY),
                        tableApi(
                                "distinct",
                                env -> env.from("t").select($("v")).distinct(),
                                GROUPING_KEY),
                        tableApi(
                                "orderBy",
                                env -> env.from("t").orderBy($("v")),
                                "Column 'v' of type VARIANT cannot be used as an ORDER BY key"));
        // the Table API supports INTERSECT and MINUS only on bounded tables
        final Stream<Arguments> batchOnly =
                inMode(
                        BATCH,
                        tableApi(
                                "intersect",
                                env ->
                                        env.from("t")
                                                .select($("v"))
                                                .intersect(env.from("u").select($("v"))),
                                "Column 'v' of type VARIANT cannot be used in INTERSECT"),
                        tableApi(
                                "minus",
                                env ->
                                        env.from("t")
                                                .select($("v"))
                                                .minus(env.from("u").select($("v"))),
                                "Column 'v' of type VARIANT cannot be used in EXCEPT"));
        // the Table API supports these OVER windows only in streaming mode
        final Stream<Arguments> streamingOnly =
                inMode(
                        STREAMING,
                        tableApi(
                                "over window partitionBy",
                                env ->
                                        env.from("t")
                                                .window(
                                                        Over.partitionBy($("v"))
                                                                .orderBy($("ts"))
                                                                .preceding(UNBOUNDED_ROW)
                                                                .as("w"))
                                                .select($("id").count().over($("w"))),
                                "An expression of type VARIANT cannot be used as a PARTITION BY key "
                                        + "of an OVER window"),
                        tableApi(
                                "distinct count over window",
                                env ->
                                        env.from("t")
                                                .window(
                                                        Over.partitionBy($("id"))
                                                                .orderBy($("ts"))
                                                                .preceding(UNBOUNDED_ROW)
                                                                .as("w"))
                                                .select($("v").count().distinct().over($("w"))),
                                "An expression " + DISTINCT_ARGUMENT));
        return Stream.concat(bothModes, Stream.concat(batchOnly, streamingOnly));
    }

    @ParameterizedTest(name = "[{index}] {2}: {0}")
    @MethodSource("rejectedQueries")
    void testRejectedQuery(
            final String sql, final String expectedMessage, final RuntimeExecutionMode mode) {
        final TableEnvironment env = createEnvironment(mode);
        assertThatThrownBy(() -> env.explainSql(sql))
                .satisfies(anyCauseMatches(ValidationException.class, expectedMessage));
    }

    @ParameterizedTest(name = "[{index}] {1}: {0}")
    @MethodSource("allowedQueries")
    void testAllowedQuery(final String sql, final RuntimeExecutionMode mode) {
        final TableEnvironment env = createEnvironment(mode);
        assertThatCode(() -> env.explainSql(sql)).doesNotThrowAnyException();
    }

    @ParameterizedTest(name = "[{index}] {1} only: {0}")
    @MethodSource("rejectedJoinsPerBatchStrategy")
    void testRejectedJoinPerBatchStrategy(final String sql, final String disabledOperators) {
        final TableEnvironment env = createEnvironment(BATCH);
        env.getConfig().set(TABLE_EXEC_DISABLED_OPERATORS, disabledOperators);
        assertThatThrownBy(() -> env.explainSql(sql))
                .satisfies(anyCauseMatches(ValidationException.class, EQUALS));
    }

    @ParameterizedTest(name = "[{index}] {2}: {0}")
    @MethodSource("rejectedTableApiQueries")
    void testRejectedTableApiQuery(
            final Function<TableEnvironment, Table> query,
            final String expectedMessage,
            final RuntimeExecutionMode mode) {
        final TableEnvironment env = createEnvironment(mode);
        assertThatThrownBy(() -> query.apply(env).explain())
                .satisfies(anyCauseMatches(ValidationException.class, expectedMessage));
    }

    private static Arguments tableApi(
            final String name,
            final Function<TableEnvironment, Table> query,
            final String expectedMessage) {
        return Arguments.of(Named.of(name, query), expectedMessage);
    }

    private static Stream<Arguments> inBothModes(final Arguments... cases) {
        return Stream.concat(inMode(STREAMING, cases), inMode(BATCH, cases));
    }

    private static Stream<Arguments> inMode(
            final RuntimeExecutionMode mode, final Arguments... cases) {
        return Stream.of(cases).map(c -> withMode(c, mode));
    }

    private static Arguments withMode(final Arguments arguments, final RuntimeExecutionMode mode) {
        final Object[] args = Arrays.copyOf(arguments.get(), arguments.get().length + 1);
        args[args.length - 1] = mode;
        return Arguments.of(args);
    }

    private static TableEnvironment createEnvironment(final RuntimeExecutionMode mode) {
        final TableEnvironment env =
                TableEnvironment.create(
                        mode == STREAMING
                                ? EnvironmentSettings.inStreamingMode()
                                : EnvironmentSettings.inBatchMode());
        for (final String table : List.of("t", "u")) {
            env.executeSql(
                    String.format(
                            "CREATE TABLE %s ("
                                    + "id INT, "
                                    + "v VARIANT, "
                                    + "r ROW<x VARIANT>, "
                                    + "a ARRAY<VARIANT>, "
                                    + "ts TIMESTAMP(3), "
                                    + "WATERMARK FOR ts AS ts"
                                    + ") WITH ('connector' = 'values', 'bounded' = '%s')",
                            table, mode == BATCH));
        }
        env.executeSql(
                String.format(
                        "CREATE TABLE w ("
                                + "id INT, "
                                + "v VARIANT, "
                                + "ts TIMESTAMP(3), "
                                + "WATERMARK FOR ts AS "
                                + "CASE WHEN v = PARSE_JSON('1') THEN ts ELSE ts - INTERVAL '1' SECOND END"
                                + ") WITH ('connector' = 'values', 'bounded' = '%s')",
                        mode == BATCH));
        env.createTemporarySystemFunction("f", PartitionedFunction.class);
        return env;
    }

    /** A function with a set semantic table argument. */
    public static class PartitionedFunction extends ProcessTableFunction<Integer> {
        public void eval(@ArgumentHint(ArgumentTrait.SET_SEMANTIC_TABLE) final Row r) {}
    }
}

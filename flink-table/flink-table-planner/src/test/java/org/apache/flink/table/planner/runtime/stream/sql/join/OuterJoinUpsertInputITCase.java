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

package org.apache.flink.table.planner.runtime.stream.sql.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for FLINK-40681: a streaming outer join whose inputs run without UPDATE_BEFORE
 * (the mode the planner picks when the sink's primary key is satisfied by the query's upsert key)
 * must still emit the null-padded row when the only matching row of the inner side is replaced by a
 * bare UPDATE_AFTER and then deleted.
 *
 * <p>The tests use a gated source so that the replacing UPDATE_AFTER always arrives after the
 * joined row reached the sink; the order of the initial INSERTs does not matter.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class OuterJoinUpsertInputITCase extends StreamingWithStateTestBase {

    private static final String SINK = "sink";

    public OuterJoinUpsertInputITCase(StateBackendMode state) {
        super(state);
    }

    @BeforeEach
    public void before() {
        super.before();
        // keep every changelog in order and all rows in one key group
        env().setParallelism(1);
    }

    /** {@code left_t LEFT JOIN right_t} into an upsert sink keyed on the join key. */
    @TestTemplate
    void testLeftJoinUpsertInputsIntoUpsertSinkWithSamePk() throws Exception {
        registerUpsertValuesTable(
                "left_t",
                "k INT NOT NULL, v STRING, PRIMARY KEY (k) NOT ENFORCED",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 1, "a")));
        registerGatedUpsertTable(
                "right_t",
                Types.ROW_NAMED(new String[] {"k", "w"}, Types.INT, Types.STRING),
                new String[] {"k"},
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "x"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, "y"),
                        Row.ofKind(RowKind.DELETE, 1, "y")),
                1,
                "+I[1, a, x]");
        tEnv().executeSql(
                        "CREATE TABLE sink (k INT NOT NULL, v STRING, w STRING,"
                                + " PRIMARY KEY (k) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        final String sql =
                "INSERT INTO sink SELECT l.k, l.v, r.w FROM left_t l LEFT JOIN right_t r ON l.k = r.k";
        assertJoinsRunWithoutUpdateBefore(tEnv().explainSql(sql, ExplainDetail.CHANGELOG_MODE), 1);

        tEnv().executeSql(sql).await();

        // the right row is gone again, so the left row must be present with null padding
        assertThat(TestValuesTableFactory.getResultsAsStrings(SINK))
                .containsExactly("+I[1, a, null]");
        final List<String> changelog = TestValuesTableFactory.getRawResultsAsStrings(SINK);
        assertThat(changelog.subList(changelog.size() - 2, changelog.size()))
                .containsExactly("-D[1, a, y]", "+I[1, a, null]");
    }

    /**
     * The control query of FLINK-23740 ({@code B LEFT OUTER JOIN C} instead of {@code FULL OUTER
     * JOIN}) into an upsert sink keyed on A's primary key.
     */
    @TestTemplate
    void testNestedLeftJoinsUpsertInputsIntoUpsertSinkOnLeftPk() throws Exception {
        registerUpsertValuesTable(
                "A",
                "k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL, k4 INT NOT NULL,"
                        + " k5 INT NOT NULL, a STRING, PRIMARY KEY (k1, k2, k3, k4, k5) NOT ENFORCED",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 1, 1, 1, 1, 1, "a")));
        registerUpsertValuesTable(
                "B",
                "k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL, b STRING,"
                        + " PRIMARY KEY (k1, k2, k3) NOT ENFORCED",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 1, 1, 1, "b")));
        registerGatedUpsertTable(
                "C",
                Types.ROW_NAMED(
                        new String[] {"k1", "k2", "k3", "c"},
                        Types.INT,
                        Types.INT,
                        Types.INT,
                        Types.STRING),
                new String[] {"k1", "k2", "k3"},
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, 1, 1, "c"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, 1, 1, "c2"),
                        Row.ofKind(RowKind.DELETE, 1, 1, 1, "c2")),
                1,
                "+I[1, 1, 1, 1, 1, a, b, c, d]");
        registerUpsertValuesTable(
                "D",
                "k1 INT NOT NULL, k2 INT NOT NULL, d STRING, PRIMARY KEY (k1, k2) NOT ENFORCED",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 1, 1, "d")));
        tEnv().executeSql(
                        "CREATE TABLE sink (k1 INT NOT NULL, k2 INT NOT NULL, k3 INT NOT NULL,"
                                + " k4 INT NOT NULL, k5 INT NOT NULL, a STRING, b STRING, c STRING, d STRING,"
                                + " PRIMARY KEY (k1, k2, k3, k4, k5) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        final String sql =
                "INSERT INTO sink\n"
                        + "SELECT A.k1, A.k2, A.k3, A.k4, A.k5, A.a, BC.b, BC.c, D.d\n"
                        + "FROM A\n"
                        + "LEFT OUTER JOIN (\n"
                        + "  SELECT B.k1, B.k2, B.k3, B.b, C.c\n"
                        + "  FROM B LEFT OUTER JOIN C\n"
                        + "  ON B.k1 = C.k1 AND B.k2 = C.k2 AND B.k3 = C.k3\n"
                        + ") AS BC ON A.k1 = BC.k1 AND A.k2 = BC.k2 AND A.k3 = BC.k3\n"
                        + "LEFT OUTER JOIN D ON A.k1 = D.k1 AND A.k2 = D.k2";
        assertJoinsRunWithoutUpdateBefore(tEnv().explainSql(sql, ExplainDetail.CHANGELOG_MODE), 3);

        tEnv().executeSql(sql).await();

        // C's row is gone again, so A joined with B, null-padded C and D must be present
        assertThat(TestValuesTableFactory.getResultsAsStrings(SINK))
                .containsExactly("+I[1, 1, 1, 1, 1, a, b, null, d]");
    }

    /**
     * Control: with a sink primary key that the upsert key does not satisfy, the planner keeps
     * UPDATE_BEFORE and the same input produces the correct result.
     */
    @TestTemplate
    void testLeftJoinUpsertInputsIntoUpsertSinkWithLargerPk() throws Exception {
        registerUpsertValuesTable(
                "left_t",
                "k INT NOT NULL, v STRING, PRIMARY KEY (k) NOT ENFORCED",
                Arrays.asList(Row.ofKind(RowKind.INSERT, 1, "a")));
        registerGatedUpsertTable(
                "right_t",
                Types.ROW_NAMED(new String[] {"k", "w"}, Types.INT, Types.STRING),
                new String[] {"k"},
                Arrays.asList(
                        Row.ofKind(RowKind.INSERT, 1, "x"),
                        Row.ofKind(RowKind.UPDATE_AFTER, 1, "y"),
                        Row.ofKind(RowKind.DELETE, 1, "y")),
                1,
                "+I[1, a, x]");
        tEnv().executeSql(
                        "CREATE TABLE sink (k INT NOT NULL, v STRING NOT NULL, w STRING,"
                                + " PRIMARY KEY (k, v) NOT ENFORCED)"
                                + " WITH ('connector' = 'values', 'sink-insert-only' = 'false')");

        final String sql =
                "INSERT INTO sink SELECT l.k, l.v, r.w FROM left_t l LEFT JOIN right_t r ON l.k = r.k";
        final String plan = tEnv().explainSql(sql, ExplainDetail.CHANGELOG_MODE);
        assertThat(joinLines(plan))
                .hasSize(1)
                .allMatch(l -> l.contains("changelogMode=[I,UB,UA,D]"));

        tEnv().executeSql(sql).await();

        assertThat(TestValuesTableFactory.getResultsAsStrings(SINK))
                .containsExactly("+I[1, a, null]");
    }

    // ------------------------------------------------------------------------------------------

    private void registerUpsertValuesTable(String name, String columns, List<Row> rows) {
        final String dataId = TestValuesTableFactory.registerData(rows);
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE %s (%s) WITH ('connector' = 'values', 'data-id' = '%s',"
                                        + " 'changelog-mode' = 'I,UA,D')",
                                name, columns, dataId));
    }

    /**
     * Registers an upsert table backed by a source that emits the given changelog in order and
     * waits before emitting the row at {@code gateIndex} until the sink has seen {@code
     * awaitedRow}.
     */
    private void registerGatedUpsertTable(
            String name,
            TypeInformation<Row> type,
            String[] primaryKey,
            List<Row> rows,
            int gateIndex,
            String awaitedRow) {
        final DataStream<Row> stream =
                env().fromSource(
                                new DataGeneratorSource<>(
                                        new GatedChangelog(rows, gateIndex, SINK, awaitedRow),
                                        rows.size(),
                                        type),
                                WatermarkStrategy.noWatermarks(),
                                name);
        final Table table =
                tEnv().fromChangelogStream(
                                stream,
                                Schema.newBuilder().primaryKey(primaryKey).build(),
                                ChangelogMode.upsert(false));
        tEnv().createTemporaryView(name, table);
    }

    /** Join nodes of the optimized physical plan, the only section that carries changelog modes. */
    private static List<String> joinLines(String plan) {
        final int start = plan.indexOf("== Optimized Physical Plan ==");
        final int end = plan.indexOf("== Optimized Execution Plan ==");
        assertThat(start).as("optimized plan:\n%s", plan).isNotNegative();
        final String physicalPlan = plan.substring(start, end < 0 ? plan.length() : end);
        return Arrays.stream(physicalPlan.split("\n"))
                .filter(l -> l.contains("Join(joinType="))
                .collect(Collectors.toList());
    }

    private static void assertJoinsRunWithoutUpdateBefore(String plan, int expectedJoins) {
        assertThat(joinLines(plan))
                .as("optimized plan:\n%s", plan)
                .hasSize(expectedJoins)
                .allMatch(l -> l.contains("changelogMode=[I,UA,D]"));
        assertThat(plan).doesNotContain("upsertMaterialize=[true]");
    }

    /** Emits a fixed changelog and blocks before one of its rows until the sink shows a row. */
    private static final class GatedChangelog implements GeneratorFunction<Long, Row> {

        private final ArrayList<Row> rows;
        private final int gateIndex;
        private final String sinkName;
        private final String awaitedRow;

        private GatedChangelog(List<Row> rows, int gateIndex, String sinkName, String awaitedRow) {
            this.rows = new ArrayList<>(rows);
            this.gateIndex = gateIndex;
            this.sinkName = sinkName;
            this.awaitedRow = awaitedRow;
        }

        @Override
        public Row map(Long index) throws Exception {
            if (index == gateIndex) {
                final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(60));
                while (!TestValuesTableFactory.getRawResultsAsStrings(sinkName)
                        .contains(awaitedRow)) {
                    if (!deadline.hasTimeLeft()) {
                        throw new IllegalStateException(
                                "Sink "
                                        + sinkName
                                        + " never received "
                                        + awaitedRow
                                        + ", got: "
                                        + TestValuesTableFactory.getRawResultsAsStrings(sinkName));
                    }
                    Thread.sleep(50);
                }
            }
            // fresh instance in case object reuse is enabled downstream
            return Row.copy(rows.get(index.intValue()));
        }
    }
}

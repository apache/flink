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

package org.apache.flink.table.planner.plan.stream.sql.join;

import org.apache.flink.table.api.ApiExpression;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.descriptor;
import static org.apache.flink.table.api.Expressions.lit;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the Table API surface of the {@code LATERAL SNAPSHOT} join produces the exact same
 * optimized physical plan as the equivalent SQL.
 *
 * <p>This is the primary coverage for the Table API surface: it converges on the same {@code
 * LateralSnapshotJoin} {@code RelNode} / {@code StreamExecLateralSnapshotJoin} operator as SQL,
 * whose execution semantics are already fully covered by the SQL semantic tests ({@code
 * LateralSnapshotJoinSemanticTests}) and {@code LateralSnapshotJoinITCase}. {@code
 * LateralSnapshotJoinSemanticTestPrograms#INNER_JOIN_TABLE_API} keeps a single execution smoke on
 * top of this.
 */
public class LateralSnapshotJoinTableApiTest extends TableTestBase {

    private static final String LOAD_COMPLETED_TIME_SQL =
            "CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))";

    private static final ApiExpression LOAD_COMPLETED_TIME_TABLE =
            lit(LocalDateTime.parse("2026-07-01T00:00:00")).cast(DataTypes.TIMESTAMP_LTZ(3));

    private TableTestUtil util;

    @BeforeEach
    void setup() {
        final TableConfig config = TableConfig.getDefault();
        config.setLocalTimeZone(ZoneId.of("UTC"));
        util = streamTestUtil(config);

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE probe ("
                                + "  pk STRING,"
                                + "  pv INT,"
                                + "  pts TIMESTAMP(3),"
                                + "  WATERMARK FOR pts AS pts"
                                + ") WITH ('connector' = 'values', 'bounded' = 'false')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
    }

    // ------------------------------------------------------------------------------------------
    // Plan equivalence: the Table API surface should produce the same optimized plan as SQL.
    // ------------------------------------------------------------------------------------------

    @Test
    void testInnerJoinPlanParity() {
        final String sql =
                "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, on_time => DESCRIPTOR(bts), "
                        + "load_completed_time => "
                        + LOAD_COMPLETED_TIME_SQL
                        + ") AS s "
                        + "ON probe.pk = s.bk";

        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .joinLateral(
                                call(
                                        "SNAPSHOT",
                                        util.tableEnv().from("b").asArgument("input"),
                                        descriptor("bts").asArgument("on_time"),
                                        LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                "load_completed_time")),
                                $("pk").isEqual($("bk")))
                        .select($("pk"), $("pv"), $("bk"), $("bv"));

        assertSameOptimizedPhysicalPlan(sql, apiResult);
    }

    @Test
    void testLeftOuterJoinPlanParity() {
        final String sql =
                "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe LEFT JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, on_time => DESCRIPTOR(bts), "
                        + "load_completed_time => "
                        + LOAD_COMPLETED_TIME_SQL
                        + ") AS s "
                        + "ON probe.pk = s.bk";

        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .leftOuterJoinLateral(
                                call(
                                        "SNAPSHOT",
                                        util.tableEnv().from("b").asArgument("input"),
                                        descriptor("bts").asArgument("on_time"),
                                        LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                "load_completed_time")),
                                $("pk").isEqual($("bk")))
                        .select($("pk"), $("pv"), $("bk"), $("bv"));

        assertSameOptimizedPhysicalPlan(sql, apiResult);
    }

    @Test
    void testAliasedColumnsPlanParity() {
        // No narrowing projection on top: SELECT * keeps the aliased build columns, so this
        // asserts both plan parity and that the Table API and SQL agree on the (aliased) schema.
        final String sql =
                "SELECT * FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, on_time => DESCRIPTOR(bts), "
                        + "load_completed_time => "
                        + LOAD_COMPLETED_TIME_SQL
                        + ") AS s(k, v, t) "
                        + "ON probe.pk = s.k";

        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .joinLateral(
                                call(
                                                "SNAPSHOT",
                                                util.tableEnv().from("b").asArgument("input"),
                                                descriptor("bts").asArgument("on_time"),
                                                LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                        "load_completed_time"))
                                        .as("k", "v", "t"),
                                $("pk").isEqual($("k")));

        assertSameOptimizedPhysicalPlan(sql, apiResult);
        assertThat(apiResult.getResolvedSchema())
                .isEqualTo(util.tableEnv().sqlQuery(sql).getResolvedSchema());
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private void assertSameOptimizedPhysicalPlan(String sql, Table apiResult) {
        final String sqlPlan = extractOptimizedPhysicalPlan(util.tableEnv().explainSql(sql));
        final String apiPlan = extractOptimizedPhysicalPlan(apiResult.explain());
        assertThat(apiPlan).isEqualTo(sqlPlan);
    }

    /** Extracts the full "Optimized Physical Plan" from an {@code EXPLAIN} output. */
    private static String extractOptimizedPhysicalPlan(String plan) {
        final String startMarker = "== Optimized Physical Plan ==";
        final int start = plan.indexOf(startMarker);
        if (start < 0) {
            throw new AssertionError(
                    "No 'Optimized Physical Plan' section found in plan:\n" + plan);
        }
        final int contentStart = start + startMarker.length();
        final int end = plan.indexOf("== Optimized Execution Plan ==", contentStart);
        final String section =
                end >= 0 ? plan.substring(contentStart, end) : plan.substring(contentStart);
        return section.lines().map(String::stripTrailing).collect(Collectors.joining("\n")).strip();
    }
}

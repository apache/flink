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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.descriptor;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy.EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS;
import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that the Table API surface of the {@code LATERAL SNAPSHOT} join produces the exact same
 * optimized physical plan as the equivalent SQL.
 *
 * <p>This is the primary coverage for the Table API surface: it converges on the same {@code
 * LateralSnapshotJoin} {@code RelNode} / {@code StreamExecLateralSnapshotJoin} operator as SQL,
 * whose execution semantics are already fully covered by the SQL semantic tests ({@code
 * LateralSnapshotJoinSemanticTests}) and {@code LateralSnapshotJoinITCase}. {@code
 * LateralSnapshotJoinTableApiSemanticTestPrograms} keeps only a few execution smokes on top of
 * this.
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
    void testTableApiBuildSidePlanParity() {
        // SQL build side goes through a named view (CTE) to mirror how SQL can only reference a
        // TABLE argument by name; the Table API passes the transformed Table directly.
        final String sql =
                "WITH cte AS (SELECT * FROM b WHERE bv > 10) "
                        + "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE cte, on_time => DESCRIPTOR(bts), "
                        + "load_completed_time => "
                        + LOAD_COMPLETED_TIME_SQL
                        + ") AS s "
                        + "ON probe.pk = s.bk";

        final Table filteredBuild = util.tableEnv().from("b").filter($("bv").isGreater(10));
        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .joinLateral(
                                call(
                                        "SNAPSHOT",
                                        filteredBuild.asArgument("input"),
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

    @Test
    void testOptionalArgsPlanParity() {
        // The optional scalar SNAPSHOT arguments (load_completed_idle_timeout, state_ttl) must be
        // passed through the Table API the same way as named SQL arguments and produce the same
        // plan. The order of Table API args is shuffled to assert correct ordering of named args.
        final String sql =
                "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, on_time => DESCRIPTOR(bts), "
                        + "load_completed_time => "
                        + LOAD_COMPLETED_TIME_SQL
                        + ", load_completed_idle_timeout => INTERVAL '10' SECOND"
                        + ", state_ttl => INTERVAL '1' DAY"
                        + ") AS s "
                        + "ON probe.pk = s.bk";

        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .joinLateral(
                                call(
                                        "SNAPSHOT",
                                        LOAD_COMPLETED_TIME_TABLE.asArgument("load_completed_time"),
                                        lit(Duration.ofDays(1)).asArgument("state_ttl"),
                                        descriptor("bts").asArgument("on_time"),
                                        lit(Duration.ofSeconds(10))
                                                .asArgument("load_completed_idle_timeout"),
                                        util.tableEnv().from("b").asArgument("input")),
                                $("pk").isEqual($("bk")))
                        .select($("pk"), $("pv"), $("bk"), $("bv"));

        assertSameOptimizedPhysicalPlan(sql, apiResult);
    }

    @Test
    void testDefaultLoadCompletedTimePlanParity() {
        // With load_completed_time omitted, the planner defaults it to the wall-clock time at
        // compile time (loadCompletedCondition=[compile_time]). SQL and the Table API are compiled
        // at slightly different instants, so the defaulted loadCompletedTime literal differs; mask
        // it before comparing the otherwise-identical plans.
        final String sql =
                "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b, on_time => DESCRIPTOR(bts)"
                        + ") AS s "
                        + "ON probe.pk = s.bk";

        final Table apiResult =
                util.tableEnv()
                        .from("probe")
                        .joinLateral(
                                call(
                                        "SNAPSHOT",
                                        util.tableEnv().from("b").asArgument("input"),
                                        descriptor("bts").asArgument("on_time")),
                                $("pk").isEqual($("bk")))
                        .select($("pk"), $("pv"), $("bk"), $("bv"));

        final String sqlPlan =
                maskLoadCompletedTime(
                        extractOptimizedPhysicalPlan(util.tableEnv().explainSql(sql)));
        final String apiPlan =
                maskLoadCompletedTime(extractOptimizedPhysicalPlan(apiResult.explain()));

        // Guard that the defaulted compile-time mode was actually exercised and the literal masked.
        assertThat(sqlPlan)
                .contains("loadCompletedCondition=[compile_time]")
                .contains("loadCompletedTime=[<masked>]");
        assertThat(apiPlan).isEqualTo(sqlPlan);
    }

    @ParameterizedTest(name = "columnExpansionStrategy={0}")
    @MethodSource("columnExpansionStrategies")
    void testBuildSideRowtimeFromMetadataColumnPlanParity(
            List<ColumnExpansionStrategy> columnExpansionStrategy) {
        // The build-side row-time (on_time) is a read-only VIRTUAL METADATA column.
        // Iterate over the column-expansion strategies, including the one that hides a default
        // virtual metadata column from SELECT *, to confirm the Table API and SQL surfaces still
        // agree and that on_time can reference the column even when it is hidden from expansion.
        util.tableEnv().getConfig().set(TABLE_COLUMN_EXPANSION_STRATEGY, columnExpansionStrategy);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_meta ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3) METADATA VIRTUAL,"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UB,UA,D',"
                                + "  'readable-metadata' = 'bts:TIMESTAMP(3)'"
                                + ")");

        final String sql =
                "SELECT probe.pk, probe.pv, s.bk, s.bv FROM probe JOIN LATERAL SNAPSHOT("
                        + "input => TABLE b_meta, on_time => DESCRIPTOR(bts), "
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
                                        util.tableEnv().from("b_meta").asArgument("input"),
                                        descriptor("bts").asArgument("on_time"),
                                        LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                "load_completed_time")),
                                $("pk").isEqual($("bk")))
                        .select($("pk"), $("pv"), $("bk"), $("bv"));

        assertSameOptimizedPhysicalPlan(sql, apiResult);
    }

    // ------------------------------------------------------------------------------------------
    // Negative cases (validation errors caught at plan time, so no execution is needed)
    // ------------------------------------------------------------------------------------------

    @Test
    void testRejectNonEquiPredicate() {
        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .from("probe")
                                        .joinLateral(snapshotCall(), $("pv").isGreater($("bv"))))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("At least one equi-join predicate is required.");
    }

    @Test
    void testRejectMissingPredicate() {
        // No ON predicate (defaults to always-true): rejected eagerly with the equi-join error
        // instead of a later planner failure.
        assertThatThrownBy(() -> util.tableEnv().from("probe").joinLateral(snapshotCall()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("At least one equi-join predicate is required.");
    }

    @Test
    void testRejectStandaloneSnapshotCall() {
        // SNAPSHOT is only valid as the build side of a LATERAL join; a standalone call is rejected
        // by the planner (ForbidSnapshotOutsideLateralRule) when the plan is produced.
        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .fromCall(
                                                "SNAPSHOT",
                                                util.tableEnv().from("b").asArgument("input"),
                                                descriptor("bts").asArgument("on_time"))
                                        .explain())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The SNAPSHOT function can only be used as the build side "
                                + "(right-hand side) of a LATERAL join");
    }

    @Test
    void testRejectMissingOnTime() {
        // on_time is optional in the signature but required for the streaming join; its absence is
        // rejected by the planner. Confirms the Table API reaches that shared validation.
        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .from("probe")
                                        .joinLateral(
                                                call(
                                                        "SNAPSHOT",
                                                        util.tableEnv()
                                                                .from("b")
                                                                .asArgument("input"),
                                                        LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                                "load_completed_time")),
                                                $("pk").isEqual($("bk")))
                                        .select($("pk"), $("pv"), $("bk"), $("bv"))
                                        .explain())
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("LATERAL SNAPSHOT requires the 'on_time' argument");
    }

    @Test
    void testRejectUnknownOnTimeColumn() {
        // The on_time descriptor references a column that does not exist on the build side.
        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .from("probe")
                                        .joinLateral(
                                                call(
                                                        "SNAPSHOT",
                                                        util.tableEnv()
                                                                .from("b")
                                                                .asArgument("input"),
                                                        descriptor("nonexistent")
                                                                .asArgument("on_time"),
                                                        LOAD_COMPLETED_TIME_TABLE.asArgument(
                                                                "load_completed_time")),
                                                $("pk").isEqual($("bk")))
                                        .select($("pk"), $("pv"), $("bk"), $("bv"))
                                        .explain())
                .isInstanceOf(ValidationException.class)
                .hasStackTraceContaining(
                        "Argument 'on_time' of SNAPSHOT references column 'nonexistent'");
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private ApiExpression snapshotCall() {
        return call(
                "SNAPSHOT",
                util.tableEnv().from("b").asArgument("input"),
                descriptor("bts").asArgument("on_time"),
                LOAD_COMPLETED_TIME_TABLE.asArgument("load_completed_time"));
    }

    private static Stream<List<ColumnExpansionStrategy>> columnExpansionStrategies() {
        // The default (no hiding) plus the strategy that hides a default virtual metadata column
        // from SELECT *, which is the case that could diverge between the Table API and SQL.
        return Stream.of(
                Collections.emptyList(), List.of(EXCLUDE_DEFAULT_VIRTUAL_METADATA_COLUMNS));
    }

    /**
     * Masks the defaulted {@code loadCompletedTime} epoch-millis literal (wall-clock at compile
     * time) so plans compiled at different instants can be compared.
     */
    private static String maskLoadCompletedTime(String plan) {
        return plan.replaceAll("loadCompletedTime=\\[\\d+\\]", "loadCompletedTime=[<masked>]");
    }

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

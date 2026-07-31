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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.internal.CompiledPlanUtils;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.runtime.operators.join.snapshot.LateralSnapshotJoinOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Plan tests for the {@code LATERAL SNAPSHOT} processing-time temporal table join. */
public class LateralSnapshotJoinTest extends TableTestBase {

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

        // Sink for the JSON-plan tests; nullable columns accept the null-padded LEFT-join build
        // side.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink ("
                                + "  pk STRING,"
                                + "  pv INT,"
                                + "  pts TIMESTAMP(3),"
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3)"
                                + ") WITH ('connector' = 'blackhole')");
    }

    @Test
    void testInnerJoin() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testLeftJoin() {
        util.verifyRelPlan(
                "SELECT * FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testInnerJoinWithIdleTimeoutAndStateTtl() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                        + "state_ttl => INTERVAL '1' DAY"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testInnerJoinWithNonEquiCondition() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk AND probe.pv > s.bv");
    }

    @Test
    void testInnerJoinWithCompositeKeys() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk AND probe.pv = s.bv");
    }

    @Test
    void testInnerJoinWithTimeAttributeInCondition() {
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk AND probe.pts >= s.bts");
    }

    @Test
    void testInnerJoinWithCteBuildSide() {
        util.verifyRelPlan(
                "WITH cte AS (SELECT bk, bv + 1 AS bv, bts FROM b) "
                        + "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE cte, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testInnerJoinWithoutBuildTimeColumn() {
        util.verifyRelPlan(
                "SELECT probe.pk, probe.pv, s.bv FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testLeftJoinWithoutBuildTimeColumn() {
        util.verifyRelPlan(
                "SELECT probe.pk, probe.pv, s.bv FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testBuildSideProctimeIsMaterialized() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_proctime ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  pt AS PROCTIME(),"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ('connector' = 'values', 'bounded' = 'false')");
        util.verifyRelPlan(
                "SELECT probe.pk, s.bk, s.bv, s.pt FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b_proctime, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk");
    }

    // ------------------------------------------------------------------------------------------
    // Behavior and compilation smoke tests
    // ------------------------------------------------------------------------------------------

    @Test
    void testBuildRowtimeIsNotForwarded() {
        // Derived table whose SELECT * exposes the probe time attribute as `pts` and the (now
        // materialized) build time attribute as `bts`.
        final String derived =
                "(SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk)";
        // Windowing over the build-side time column is rejected: it is materialized, not a time
        // attribute.
        assertThatThrownBy(
                        () ->
                                util.verifyRelPlan(
                                        "SELECT COUNT(*) FROM "
                                                + derived
                                                + " GROUP BY TUMBLE(bts, INTERVAL '1' MINUTE)"))
                .hasMessageContaining("time attribute");
        // The probe-side time attribute is preserved and remains usable as event time.
        util.tableEnv()
                .explainSql(
                        "SELECT COUNT(*) FROM "
                                + derived
                                + " GROUP BY TUMBLE(pts, INTERVAL '1' MINUTE)");
    }

    @Test
    void testInnerJoinWithUpsertBuildSourceMaterializesRetractions() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_upsert ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts,"
                                + "  PRIMARY KEY (bk) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UA,D'"
                                + ")");
        final String plan =
                util.tableEnv()
                        .explainSql(
                                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b_upsert, "
                                        + "load_completed_condition => 'user_time', "
                                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                                        + ")) AS s ON probe.pk = s.bk");
        assertThat(plan).contains("ChangelogNormalize");
        assertThat(plan).doesNotContain("DropUpdateBefore");
    }

    @Test
    void testNonEquiConditionCompilesEndToEnd() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk AND probe.pv > s.bv AND probe.pts >= s.bts";
        assertThat(util.tableEnv().explainSql(sql)).contains("LateralSnapshotJoin");
    }

    @Test
    void testFoldableConstantArgs() {
        final String plan =
                util.tableEnv()
                        .explainSql(
                                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                                        + "input => TABLE b, "
                                        + "load_completed_condition => 'user_time', "
                                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                                        + ")) AS s ON probe.pk = s.bk");
        assertThat(plan).contains("LateralSnapshotJoin");
    }

    @Test
    void testInnerJoinWithDefaultCompileTimeCompilesEndToEnd() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT(input => TABLE b)) AS s "
                        + "ON probe.pk = s.bk";
        // Compile via the table environment without verifying the plan XML (since
        // load_completed_time embeds wall-clock millis at planning).
        assertThat(util.tableEnv().explainSql(sql))
                .contains("LateralSnapshotJoin")
                .contains("loadCompletedCondition=[compile_time]")
                .contains("joinType=[InnerJoin]")
                .contains("where=[=(pk, bk)]");
    }

    @Test
    void testInnerJoinWithExplicitCompileTimeCompilesEndToEnd() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'compile_time'"
                        + ")) AS s ON probe.pk = s.bk";
        assertThat(util.tableEnv().explainSql(sql))
                .contains("LateralSnapshotJoin")
                .contains("loadCompletedCondition=[compile_time]")
                .contains("joinType=[InnerJoin]")
                .contains("where=[=(pk, bk)]");
    }

    // ------------------------------------------------------------------------------------------
    // Validation: rejection paths
    // ------------------------------------------------------------------------------------------

    @Test
    void testRejectBuildSideWithoutWatermark() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_no_wm ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3)"
                                + ") WITH ('connector' = 'values', 'bounded' = 'false')");
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b_no_wm, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "LATERAL SNAPSHOT requires a watermark on the build-side input.");
    }

    @Test
    void testRejectProbeSideNotAppendOnly() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE probe_updates ("
                                + "  pk STRING,"
                                + "  pv INT,"
                                + "  pts TIMESTAMP(3),"
                                + "  WATERMARK FOR pts AS pts,"
                                + "  PRIMARY KEY (pk) NOT ENFORCED"
                                + ") WITH ("
                                + "  'connector' = 'values',"
                                + "  'bounded' = 'false',"
                                + "  'changelog-mode' = 'I,UB,UA,D'"
                                + ")");
        final String sql =
                "SELECT * FROM probe_updates JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe_updates.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .hasMessageContaining(
                        "The probe (left) input of LATERAL SNAPSHOT join doesn't support "
                                + "consuming update and delete changes");
    }

    @Test
    void testRejectMissingEqualityPredicate() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pv > s.bv";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "LATERAL SNAPSHOT join requires at least one equality predicate.");
    }

    @Test
    void testRejectNonConstantCondition() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => CAST(CURRENT_TIMESTAMP AS STRING)"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Invalid function call")
                .hasMessageContaining("SNAPSHOT");
    }

    @Test
    void testRejectNonConstantLoadCompletedTime() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CURRENT_TIMESTAMP"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Argument 'load_completed_time' of SNAPSHOT must be a constant expression");
    }

    @Test
    void testRejectNonConstantIdleTimeout() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "load_completed_idle_timeout => "
                        + "CASE WHEN RAND() > 0.5 THEN INTERVAL '10' SECOND ELSE INTERVAL '20' SECOND END"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Argument 'load_completed_idle_timeout' of SNAPSHOT must be a constant expression");
    }

    @Test
    void testRejectNonConstantStateTtl() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "state_ttl => "
                        + "CASE WHEN RAND() > 0.5 THEN INTERVAL '1' DAY ELSE INTERVAL '2' DAY END"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Argument 'state_ttl' of SNAPSHOT must be a constant expression");
    }

    @Test
    void testRejectYearMonthIntervalStateTtl() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "state_ttl => INTERVAL '1' YEAR"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("No match found for function signature SNAPSHOT");
    }

    @Test
    void testRejectNegativeIdleTimeout() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "load_completed_idle_timeout => INTERVAL -'10' SECOND"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "Argument 'load_completed_idle_timeout' of SNAPSHOT must not be negative");
    }

    @Test
    void testRejectNegativeStateTtl() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "state_ttl => INTERVAL -'10' MINUTE"
                        + ")) AS s ON probe.pk = s.bk";
        assertThatThrownBy(() -> util.verifyRelPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Argument 'state_ttl' of SNAPSHOT must not be negative");
    }

    // ------------------------------------------------------------------------------------------
    // Exec-node (JSON plan) serialization round-trips — verify StreamExecLateralSnapshotJoin
    // compiles to a CompiledPlan and back, without executing the operator.
    // ------------------------------------------------------------------------------------------

    @Test
    void testInnerJoinJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO sink SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk");
    }

    @Test
    void testLeftJoinJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO sink SELECT * FROM probe LEFT JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk");
    }

    @Test
    void testInnerJoinWithIdleTimeoutAndStateTtlJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO sink SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3)), "
                        + "load_completed_idle_timeout => INTERVAL '10' SECOND, "
                        + "state_ttl => INTERVAL '1' DAY"
                        + ")) AS s ON probe.pk = s.bk");
    }

    @Test
    void testInnerJoinWithCompositeKeysJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO sink SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk AND probe.pv = s.bv");
    }

    @Test
    void testInnerJoinWithNonEquiConditionJsonPlan() {
        util.verifyJsonPlan(
                "INSERT INTO sink SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s ON probe.pk = s.bk AND probe.pv > s.bv");
    }

    // ------------------------------------------------------------------------------------------
    // State TTL resolution — the operator's min state TTL comes from the SNAPSHOT `state_ttl`
    // argument, falling back to the pipeline's `table.exec.state.ttl` when the argument is absent.
    // ------------------------------------------------------------------------------------------

    @Test
    void testStateTtlFallsBackToPipelineStateTtlWhenArgAbsent() {
        util.tableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofHours(12));

        assertThat(resolveStateTtlMs("SNAPSHOT(input => TABLE b)"))
                .isEqualTo(Duration.ofHours(12).toMillis());
    }

    @Test
    void testExplicitStateTtlArgOverridesPipelineDefault() {
        util.tableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ofHours(12));

        assertThat(resolveStateTtlMs("SNAPSHOT(input => TABLE b, state_ttl => INTERVAL '1' DAY)"))
                .isEqualTo(Duration.ofDays(1).toMillis());
    }

    @Test
    void testStateTtlDisabledWhenNeitherArgNorPipelineTtlSet() {
        util.tableEnv().getConfig().set(ExecutionConfigOptions.IDLE_STATE_RETENTION, Duration.ZERO);

        assertThat(resolveStateTtlMs("SNAPSHOT(input => TABLE b)")).isZero();
    }

    /**
     * Compiles a join over the given {@code SNAPSHOT} call and returns the operator's state TTL.
     */
    private long resolveStateTtlMs(String snapshotCall) {
        final CompiledPlan plan =
                util.tableEnv()
                        .compilePlanSql(
                                "INSERT INTO sink SELECT * FROM probe "
                                        + "JOIN LATERAL TABLE("
                                        + snapshotCall
                                        + ") AS s ON probe.pk = s.bk");
        final List<Transformation<?>> transformations =
                CompiledPlanUtils.toTransformations(util.tableEnv(), plan);
        return findJoinOperator(transformations).getMinStateTtlMs();
    }

    private static LateralSnapshotJoinOperator findJoinOperator(
            List<Transformation<?>> transformations) {
        return transformations.stream()
                .flatMap(t -> t.getTransitivePredecessors().stream())
                .filter(TwoInputTransformation.class::isInstance)
                .map(t -> ((TwoInputTransformation<?, ?, ?>) t).getOperatorFactory())
                .filter(SimpleOperatorFactory.class::isInstance)
                .map(f -> ((SimpleOperatorFactory<?>) f).getOperator())
                .filter(LateralSnapshotJoinOperator.class::isInstance)
                .map(LateralSnapshotJoinOperator.class::cast)
                .findFirst()
                .orElseThrow(
                        () ->
                                new AssertionError(
                                        "No LateralSnapshotJoinOperator found in the plan."));
    }
}

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

package org.apache.flink.table.planner.plan.batch.sql.join;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalLateralSnapshotJoinRule;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Plan tests for {@code LATERAL SNAPSHOT} joins in batch mode.
 *
 * <p>In batch all input is bounded and append-only, so the processing-time {@code LATERAL SNAPSHOT}
 * join degenerates to a regular join of the probe side against the (final) build side. {@link
 * BatchPhysicalLateralSnapshotJoinRule} performs this translation and the SNAPSHOT-specific
 * arguments are dropped.
 */
public class LateralSnapshotJoinTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE probe ("
                                + "  pk STRING,"
                                + "  pv INT,"
                                + "  pts TIMESTAMP(3),"
                                + "  WATERMARK FOR pts AS pts"
                                + ") WITH ('connector' = 'values', 'bounded' = 'true')");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3),"
                                + "  WATERMARK FOR bts AS bts"
                                + ") WITH ('connector' = 'values', 'bounded' = 'true')");
    }

    // ------------------------------------------------------------------------------------------
    // Translation to a regular join
    // ------------------------------------------------------------------------------------------

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
    void testBuildSideWithProctime() {
        // A PROCTIME() build column is a time-attribute indicator in batch; the rule materializes
        // it (into a PROCTIME_MATERIALIZE call) when degrading to a regular join.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_proctime ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  pt AS PROCTIME()"
                                + ") WITH ('connector' = 'values', 'bounded' = 'true')");
        util.verifyRelPlan(
                "SELECT probe.pk, s.bk, s.bv, s.pt FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b_proctime)) AS s "
                        + "ON probe.pk = s.bk");
    }

    @Test
    void testBuildSideWithoutWatermark() {
        // A watermark on the build side is required in streaming (drives the LOAD phase) but not in
        // batch, where the build side is already final.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE b_no_wm ("
                                + "  bk STRING,"
                                + "  bv INT,"
                                + "  bts TIMESTAMP(3)"
                                + ") WITH ('connector' = 'values', 'bounded' = 'true')");
        util.verifyRelPlan(
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT(input => TABLE b_no_wm)) AS s "
                        + "ON probe.pk = s.bk");
    }

    // ------------------------------------------------------------------------------------------
    // Validation: rejection paths
    // ------------------------------------------------------------------------------------------

    @Test
    void testRejectMissingEqualityPredicate() {
        final String sql =
                "SELECT * FROM probe JOIN LATERAL TABLE(SNAPSHOT("
                        + "input => TABLE b, "
                        + "load_completed_condition => 'user_time', "
                        + "load_completed_time => CAST(TIMESTAMP '2026-07-01 00:00:00' AS TIMESTAMP_LTZ(3))"
                        + ")) AS s "
                        + "ON probe.pv > s.bv";
        assertThatThrownBy(() -> util.verifyExecPlan(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "LATERAL SNAPSHOT join requires at least one equality predicate.");
    }

    @Test
    void testRejectSnapshotOutsideLateral() {
        // SNAPSHOT used outside a LATERAL context is not rewritten into a join;
        // ForbidSnapshotOutsideLateralRule rejects the surviving SNAPSHOT scan with a clear
        // message.
        assertThatThrownBy(() -> util.verifyExecPlan("SELECT * FROM SNAPSHOT(input => TABLE b)"))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The SNAPSHOT function can only be used as the build side "
                                + "(right-hand side) of a LATERAL join");
    }
}

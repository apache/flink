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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link RedundantWatermarkAssignerRemoveRule}.
 *
 * <p>The rule drops {@link
 * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner}
 * operators whose watermarks are not consumed by any downstream operator that requires watermarks
 * (windowed aggregates, event-time interval / temporal joins, event-time temporal sort, …). This
 * test exercises positive cases (assigner removed) and negative cases (assigner kept).
 */
class RedundantWatermarkAssignerRemoveRuleTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  rt TIMESTAMP(3),\n"
                                + "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false'\n"
                                + ")");

        util.tableEnv().executeSql("CREATE TABLE src2 LIKE src");

        // A source that supports watermark push-down: when push-down kicks in, the runtime
        // operator handles watermarks itself; the rule must still produce a consistent plan.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE src_pushdown (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  rt TIMESTAMP(3),\n"
                                + "  WATERMARK FOR rt AS rt - INTERVAL '1' SECOND\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false',\n"
                                + "  'enable-watermark-push-down' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE rowtime_sink (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING,\n"
                                + "  rt TIMESTAMP(3)\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE plain_sink (\n"
                                + "  a INT,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE window_sink (\n"
                                + "  window_start TIMESTAMP(3),\n"
                                + "  window_end TIMESTAMP(3),\n"
                                + "  a INT,\n"
                                + "  cnt BIGINT\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'true'\n"
                                + ")");

        // A versioned table for event-time temporal joins.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE rates (\n"
                                + "  currency STRING,\n"
                                + "  rate BIGINT,\n"
                                + "  rt TIMESTAMP(3),\n"
                                + "  WATERMARK FOR rt AS rt,\n"
                                + "  PRIMARY KEY(currency) NOT ENFORCED\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false',\n"
                                + "  'changelog-mode' = 'I,UA,UB,D'\n"
                                + ")");

        // A processing-time-only lookup table for lookup joins.
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE lookup_dim (\n"
                                + "  id INT,\n"
                                + "  name STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'true'\n"
                                + ")");
    }

    /** No downstream event-time operator: a plain SELECT * INSERT drops the assigner. */
    @Test
    void testSimpleSelectRemovesAssigner() {
        util.verifyRelPlanInsert("INSERT INTO plain_sink SELECT a, b, c FROM src");
    }

    /** A Calc chain above the assigner is preserved; the assigner is removed underneath. */
    @Test
    void testCalcChainRemovesAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink SELECT a, b + 1, UPPER(c) FROM src WHERE a > 0");
    }

    /**
     * If a rowtime ({@code *ROWTIME*}) column reaches the sink, an upstream rowtime attribute is
     * being consumed as a watermark-bearing column (e.g. for forwarding to an external system). The
     * rule must keep the assigner so the watermark remains available at runtime.
     */
    @Test
    void testRowtimeForwardingSinkKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO rowtime_sink SELECT a, b, c, rt FROM src WHERE a > 0");
    }

    /** Tumble window aggregation requires watermarks. */
    @Test
    void testTumbleWindowKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO window_sink "
                        + "SELECT window_start, window_end, a, COUNT(*) FROM TABLE("
                        + "  TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND)) "
                        + "GROUP BY window_start, window_end, a");
    }

    /** Event-time interval join requires watermarks on both sides. */
    @Test
    void testRowtimeIntervalJoinKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink "
                        + "SELECT s1.a, s1.b, s1.c FROM src s1 JOIN src2 s2 "
                        + "ON s1.a = s2.a AND s1.rt BETWEEN s2.rt - INTERVAL '5' SECOND "
                        + "AND s2.rt + INTERVAL '5' SECOND");
    }

    /**
     * {@code CURRENT_WATERMARK(rt)} reads the upstream watermark at runtime even though its hosting
     * Calc does not declare {@link
     * org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel#requireWatermark()}.
     * The rule must keep the assigner in this case.
     */
    @Test
    void testCurrentWatermarkInProjectionKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink "
                        + "SELECT a, b, CAST(CURRENT_WATERMARK(rt) AS STRING) FROM src");
    }

    /** Same as above but the {@code CURRENT_WATERMARK} call appears inside a WHERE filter. */
    @Test
    void testCurrentWatermarkInFilterKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink "
                        + "SELECT a, b, c FROM src "
                        + "WHERE CURRENT_WATERMARK(rt) IS NULL OR rt > CURRENT_WATERMARK(rt)");
    }

    /**
     * Union where only the right branch feeds a window aggregation. The left branch's assigner is
     * redundant and must be removed; the right branch's assigner must be kept.
     */
    @Test
    void testUnionMixedBranches() {
        util.verifyRelPlanInsert(
                "INSERT INTO window_sink "
                        + "SELECT CAST(NULL AS TIMESTAMP(3)), CAST(NULL AS TIMESTAMP(3)), a, b "
                        + "FROM src "
                        + "UNION ALL "
                        + "SELECT window_start, window_end, a, COUNT(*) FROM TABLE("
                        + "  TUMBLE(TABLE src2, DESCRIPTOR(rt), INTERVAL '5' SECOND)) "
                        + "GROUP BY window_start, window_end, a");
    }

    // ------------------------------------------------------------------------
    // FLINK-14621 review follow-ups
    // ------------------------------------------------------------------------

    /**
     * A statement set with two branches: one drops the assigner (plain select), one keeps it
     * (window aggregation). The rule fires once per sink, so each branch is optimized
     * independently.
     */
    @Test
    void testStatementSetMixedBranches() {
        StatementSet stmtSet = util.tableEnv().createStatementSet();
        stmtSet.addInsertSql("INSERT INTO plain_sink SELECT a, b, c FROM src");
        stmtSet.addInsertSql(
                "INSERT INTO window_sink "
                        + "SELECT window_start, window_end, a, COUNT(*) FROM TABLE("
                        + "  TUMBLE(TABLE src, DESCRIPTOR(rt), INTERVAL '5' SECOND)) "
                        + "GROUP BY window_start, window_end, a");
        util.verifyRelPlan(stmtSet);
    }

    /**
     * Event-time temporal table join (versioned table). The probe-side assigner must be kept
     * because the join is fired by the rowtime watermark. This exercises the {@code
     * containsCurrentWatermarkCall}-style consumer detection that walks any rel's expressions (Join
     * condition lives on a Join, not a Calc/Project/Filter).
     */
    @Test
    void testEventTimeTemporalJoinKeepsAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink "
                        + "SELECT o.a, o.b * r.rate, r.currency "
                        + "FROM src AS o "
                        + "JOIN rates FOR SYSTEM_TIME AS OF o.rt AS r "
                        + "ON CAST(o.a AS STRING) = r.currency");
    }

    /**
     * Process-time lookup join does <em>not</em> consume the probe stream's watermark, so the
     * assigner is redundant and gets removed.
     */
    @Test
    void testProctimeLookupJoinRemovesAssigner() {
        util.verifyRelPlanInsert(
                "INSERT INTO plain_sink "
                        + "SELECT s.a, s.b, d.name "
                        + "FROM (SELECT *, PROCTIME() AS pt FROM src) s "
                        + "JOIN lookup_dim FOR SYSTEM_TIME AS OF s.pt AS d "
                        + "ON s.a = d.id");
    }

    /**
     * Source that supports watermark push-down. Even when push-down applies, the rule must produce
     * a consistent plan (no spurious type demotions, no removal that would break runtime
     * watermarking when it is actually needed).
     */
    @Test
    void testWatermarkPushDownSourceWithoutEventTimeRemovesAssigner() {
        util.verifyRelPlanInsert("INSERT INTO plain_sink SELECT a, b, c FROM src_pushdown");
    }

    /**
     * Watermark push-down source feeding a window aggregate: push-down + window means watermarks
     * must remain wired up end-to-end; the rule must not interfere.
     */
    @Test
    void testWatermarkPushDownSourceWithWindowKeepsWatermarks() {
        util.verifyRelPlanInsert(
                "INSERT INTO window_sink "
                        + "SELECT window_start, window_end, a, COUNT(*) FROM TABLE("
                        + "  TUMBLE(TABLE src_pushdown, DESCRIPTOR(rt), INTERVAL '5' SECOND)) "
                        + "GROUP BY window_start, window_end, a");
    }
}

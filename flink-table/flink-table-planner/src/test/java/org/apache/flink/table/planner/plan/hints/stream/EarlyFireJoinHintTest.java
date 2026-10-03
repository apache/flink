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

package org.apache.flink.table.planner.plan.hints.stream;

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import scala.Enumeration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the EARLY_FIRE join hint surface and option validation. */
class EarlyFireJoinHintTest extends TableTestBase {

    protected StreamTableTestUtil util;

    @BeforeEach
    void before() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT,\n"
                                + "  b VARCHAR,\n"
                                + "  c BIGINT,\n"
                                + "  proctime AS PROCTIME(),\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  WATERMARK FOR rowtime AS rowtime\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable2 (\n"
                                + "  a INT,\n"
                                + "  b VARCHAR,\n"
                                + "  c BIGINT,\n"
                                + "  proctime AS PROCTIME(),\n"
                                + "  rowtime TIMESTAMP(3),\n"
                                + "  WATERMARK FOR rowtime AS rowtime\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'bounded' = 'false'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MySink (\n"
                                + "  a INT,\n"
                                + "  b VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'false'\n"
                                + ")");
    }

    @Test
    void testEarlyFireMissingDelay() {
        String sql =
                "SELECT /*+ EARLY_FIRE('time-mode'='rowtime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql)).hasMessageContaining("incomplete required option(s)");
    }

    @Test
    void testEarlyFireNonPositiveDelay() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='0s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("value should be at least 1 millisecond");
    }

    @Test
    void testEarlyFireSubMillisecondDelay() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='1ns') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("value should be at least 1 millisecond");
    }

    @Test
    void testEarlyFireInvalidTimeMode() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s', 'time-mode'='unknown') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("Invalid EARLY_FIRE hint options");
    }

    @Test
    void testEarlyFireUnknownOption() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s', 'timemode'='proctime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("Unsupported EARLY_FIRE hint option(s) [timemode]");
    }

    @Test
    void testEarlyFireListOptionsRejected() {
        String sql =
                "SELECT /*+ EARLY_FIRE('5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("only support key-value options");
    }

    @Test
    void testEarlyFireUnsupportedTarget() {
        String sql =
                "SELECT /*+ EARLY_FIRE('target'='window_join', 'delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasMessageContaining("target value 'window_join' is not supported");
    }

    @Test
    void testEarlyFireLowerCaseHintNamePreservesOptions() {
        String sql =
                "SELECT /*+ early_fire('delay'='5s', 'time-mode'='rowtime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verify(sql);
    }

    @Test
    void testEarlyFireOnRowTimeLeftOuterJoin() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verify(sql);
    }

    @Test
    void testEarlyFireExplicitTargetIntervalJoin() {
        String sql =
                "SELECT /*+ EARLY_FIRE('target'='interval_join', 'delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verify(sql);
    }

    @Test
    void testEarlyFireRowTimeOnProcTimeJoin() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s', 'time-mode'='rowtime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql))
                .hasStackTraceContaining("requires a row-time interval join");
    }

    @Test
    void testEarlyFireProcTimeOnRowTimeJoin() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s', 'time-mode'='proctime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> verify(sql)).hasStackTraceContaining("not yet supported");
    }

    @Test
    void testEarlyFireOnProcTimeLeftOuterJoin() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR";
        verify(sql);
    }

    @Test
    void testEarlyFireOuterJoinProducesUpdates() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verifyChangelogMode(sql);
    }

    @Test
    void testEarlyFireOuterJoinIntoInsertOnlySinkFails() {
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE InsertOnlySink (\n"
                                + "  a INT,\n"
                                + "  b VARCHAR\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink-insert-only' = 'true'\n"
                                + ")");
        String insert =
                "INSERT INTO InsertOnlySink\n"
                        + "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        assertThatThrownBy(() -> util.verifyRelPlanInsert(insert))
                .hasMessageContaining(
                        "the EARLY_FIRE hint makes this outer interval join produce update");
    }

    @Test
    void testEarlyFireNegativeWindowStaysInsertOnly() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime + INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '5' SECOND";
        verifyChangelogMode(sql);
    }

    @Test
    void testEarlyFireInnerJoinStaysInsertOnly() {
        String sql =
                "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verifyChangelogMode(sql);
    }

    @Test
    void testEarlyFireJsonPlanRoundTrip() {
        String insert =
                "INSERT INTO MySink\n"
                        + "SELECT /*+ EARLY_FIRE('delay'='5s') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        util.verifyJsonPlan(insert);
    }

    private void verify(String sql) {
        util.doVerifyPlan(
                sql,
                new ExplainDetail[] {},
                false,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_EXEC()},
                false);
    }

    private void verifyChangelogMode(String sql) {
        util.verifyRelPlan(sql, Collections.singletonList(ExplainDetail.CHANGELOG_MODE));
    }
}

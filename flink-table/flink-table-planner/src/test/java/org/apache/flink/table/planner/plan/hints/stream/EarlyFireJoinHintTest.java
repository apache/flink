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
    void testEarlyFireLowerCaseHintNamePreservesOptions() {
        String sql =
                "SELECT /*+ early_fire('delay'='5s', 'time-mode'='rowtime') */ t1.a, t2.b\n"
                        + "FROM MyTable t1 LEFT OUTER JOIN MyTable2 t2 ON\n"
                        + "  t1.a = t2.a AND\n"
                        + "  t1.rowtime BETWEEN t2.rowtime - INTERVAL '10' SECOND AND t2.rowtime + INTERVAL '1' HOUR";
        verify(sql);
    }

    private void verify(String sql) {
        util.doVerifyPlan(
                sql,
                new ExplainDetail[] {},
                false,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_EXEC()},
                false);
    }
}

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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.utils.PlanKind;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import scala.Enumeration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link org.apache.flink.table.planner.hint.StateTtlHint}. */
class StateTtlHintTest extends TableTestBase {

    protected StreamTableTestUtil util;

    @BeforeEach
    void before() {
        util = streamTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1 (\n"
                                + "  a1 BIGINT,\n"
                                + "  b1 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T2 (\n"
                                + "  a2 BIGINT,\n"
                                + "  b2 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T3 (\n"
                                + "  a3 BIGINT,\n"
                                + "  b3 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + ")");

        util.tableEnv().executeSql("CREATE View V4 as select a3 as a4, b3 as b4 from T3");

        util.tableEnv()
                .executeSql("create view V5 as select T1.* from T1 join T2 on T1.a1 = T2.a2");

        util.tableEnv()
                .executeSql(
                        "create view V6 as select a1, b1, count(*) as cnt from T1 group by a1, b1");
    }

    @Test
    void testSimpleJoinStateTtlHintWithEachSide() {
        String sql =
                "select /*+ STATE_TTL('T2' = '2d', 'T1' = '1d') */* from T1 join T2 on T1.a1 = T2.a2";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintOnlyWithRightSide() {
        String sql = "select /*+ STATE_TTL('T2' = '2d') */* from T1 join T2 on T1.a1 = T2.a2";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithCascadeJoin() {
        String sql =
                "select /*+ STATE_TTL('T2' = '2d', 'T3' = '3d', 'T1' = '1d') */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithSubQueryContainsJoin() {
        String sql =
                "select /*+ STATE_TTL('T2' = '2d', 'T3' = '3d', 'T1' = '1d') */* from T1 "
                        + "join (select T2.* from T2 join T3 on T2.b2 = T3.b3) TMP on T1.a1 = TMP.b2";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "T2, T3", "STATE_TTL");
    }

    @Test
    void testJoinStateTtlHintWithOneUnknownTable() {
        String sql =
                "select /*+ STATE_TTL('T5' = '2d', 'T1' = '1d') */* from T1 join T2 on T1.a1 = T2.a2";

        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "T5", "STATE_TTL");
    }

    @Test
    void testJoinStateTtlHintWithTwoUnknownTables() {
        String sql =
                "select /*+ STATE_TTL('T5' = '2d', 'T6' = '1d') */* from T1 join T2 on T1.a1 = T2.a2";

        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "T5, T6", "STATE_TTL");
    }

    @Test
    void testJoinStateTtlHintWithView() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d', 'V4' = '1d') */* from T1 join V4 on T1.a1 = V4.a4";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithUnknownView() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d', 'V8' = '1d') */* from T1 join V4 on T1.a1 = V4.a4";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "V8", "STATE_TTL");
    }

    @Test
    void testDuplicateJoinStateTtlHint() {
        String sql =
                "select /*+ STATE_TTL('T2' = '2d', 'T3' = '3d'), STATE_TTL('T1' = '1d', 'T2' = '8d') */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithDuplicatedArgs() {
        String sql =
                "select /*+ STATE_TTL('T2' = '2d', 'T2' = '1d') */* from T1 join T2 on T1.a1 = T2.a2";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintNotPropagateIntoView() {
        String sql = "select /*+ STATE_TTL('T1' = '1d')*/T1.* from T1 join V5 on T1.a1 = V5.a1";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintNotPropagateOutOfView() {
        String sql =
                "select T1.* from T1 join (select /*+ STATE_TTL('T1' = '2d')*/T1.* from T1 join T2 on T1.a1 = T2.a2) tmp on T1.a1 = tmp.a1";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithAgg() {
        String sql =
                "select /*+ STATE_TTL('T1' = '1d', 'T2' = '2d')*/T1.b1, sum(T1.a1) from T1 join T2 on T1.b1 = T2.b2 group by T1.b1";
        verify(sql);
    }

    @Test
    void testStateTtlHintWithJoinHint() {
        String sql =
                "select /*+ STATE_TTL('T1' = '1d', 'T2' = '2d'), BROADCAST(T1) */T1.b1, sum(T1.a1) from T1 join T2 on T1.b1 = T2.b2 group by T1.b1";
        verify(sql);
    }

    @Test
    void testJoinStateTtlHintWithEmptyKV() {
        String sql = "select /*+ STATE_TTL() */* from T1 join T2 on T1.a1 = T2.a2";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(
                        "Invalid STATE_TTL hint, expecting at least one key-value options specified.");
    }

    @Test
    void testSimpleAggStateTtl() {
        String sql = "select /*+ STATE_TTL('T1' = '2d') */ count(*) from T1 group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithUnknownTable() {
        String sql = "select /*+ STATE_TTL('T2' = '2d') */ count(*) from T1 group by a1";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "T2", "STATE_TTL");
    }

    @Test
    void testAggStateTtlWithView() {
        String sql = "select /*+ STATE_TTL('V6' = '2d') */ max(b1) from V6 group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithUnknownView() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d') */ max(b1) from "
                        + "(select a1, b1, count(*) from T1 group by a1, b1) TMP group by a1";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The options of following hints cannot match the name of input tables or views: \n`%s` in `%s`",
                        "T1", "STATE_TTL");
    }

    @Test
    void testMultiAggStateTtl() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d'), STATE_TTL('T1' = '8d') */ count(*) from T1 group by a1";
        verify(sql);
    }

    @Test
    void testSingleAggStateTtlWithMultiKV() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d', 'T1' = '8d') */ count(*) from T1 group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithCascadeAgg() {
        String sql =
                "select /*+ STATE_TTL('TMP' = '2d') */ max(b1) from "
                        + "(select /*+ STATE_TTL('T1' = '4d') */ a1, b1, count(*) from T1 group by a1, b1) TMP group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlNotPropagateOutOfView() {
        String sql =
                "select max(b1) from "
                        + "(select /*+ STATE_TTL('T1' = '4d') */ a1, b1, count(*) from T1 group by a1, b1) TMP group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlNotPropagateIntoView() {
        String sql =
                "select /*+ STATE_TTL('TMP' = '2d') */ max(b1) from "
                        + "(select a1, b1, count(*) from T1 group by a1, b1) TMP group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithJoin() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d') */ max(b1) from "
                        + "(select T1.* from T1 join T2 on T1.a1 = T2.a2) T1 group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithJoinHint() {
        String sql =
                "select /*+ STATE_TTL('T1' = '2d') */ max(b1) from "
                        + "(select  /*+ BROADCAST(T1) */T1.* from T1 join T2 on T1.a1 = T2.a2) T1 group by a1";
        verify(sql);
    }

    @Test
    void testAggStateTtlWithEmptyKV() {
        String sql = "select /*+ STATE_TTL() */ max(b1) from T1 group by a1";
        assertThatThrownBy(() -> verify(sql))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining(
                        "Invalid STATE_TTL hint, expecting at least one key-value options specified.");
    }

    private void verify(String sql) {
        util.doVerifyPlan(
                sql,
                new ExplainDetail[] {},
                false,
                new Enumeration.Value[] {PlanKind.AST(), PlanKind.OPT_REL()},
                true);
    }
}

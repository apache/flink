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

package org.apache.flink.table.planner.plan.batch.sql.join.joinhint;

import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.hint.JoinStrategy;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.collect.Lists;

import org.apache.logging.log4j.util.Strings;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

/** A test base for join hint. */
public abstract class JoinHintTestBase extends TableTestBase {

    private BatchTableTestUtil util;

    private final List<String> allJoinHintNames =
            Lists.newArrayList(JoinStrategy.values()).stream()
                    .map(JoinStrategy::getJoinHintName)
                    .collect(Collectors.toList());

    @Before
    public void before() {
        util = batchTestUtil(TableConfig.getDefault());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1 (\n"
                                + "  a1 BIGINT,\n"
                                + "  b1 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T2 (\n"
                                + "  a2 BIGINT,\n"
                                + "  b2 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T3 (\n"
                                + "  a3 BIGINT,\n"
                                + "  b3 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv().executeSql("CREATE View V4 as select a3 as a4, b3 as b4 from T3");
    }

    protected abstract String getTestSingleJoinHint();

    protected List<String> getOtherJoinHints() {
        return allJoinHintNames.stream()
                .filter(name -> !name.equals(getTestSingleJoinHint()))
                .collect(Collectors.toList());
    }

    @Test
    public void testSimpleJoinHintWithLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testSimpleJoinHintWithRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstSideAsBuildSide1() {
        // the T1 will be the build side in first join
        String sql =
                "select /*+ %s(T1, T2) */* from T1, T2, T3 where T1.a1 = T2.a2 and T1.b1 = T3.b3";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstSideAsBuildSide2() {
        String sql =
                "select /*+ %s(T1, T2) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiJoinAndSecondThirdSideAsBuildSides1() {
        String sql =
                "select /*+ %s(T2, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T1.b1 = T3.b3";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiJoinAndSecondThirdSideAsBuildSides2() {
        String sql =
                "select /*+ %s(T2, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiJoinAndFirstThirdSideAsBuildSides() {
        String sql =
                "select /*+ %s(T1, T3) */* from T1, T2, T3 where T1.a1 = T2.a2 and T2.b2 = T3.b3";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithUnknownTable() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        "The options of following hints cannot match the name of input tables or views:");
        String sql = "select /*+ %s(T99) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithView() {
        String sql = "select /*+ %s(V4) */* from T1 join V4 on T1.a1 = V4.a4";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithUnknownView() {
        thrown().expect(ValidationException.class);
        thrown().expectMessage(
                        String.format(
                                "The options of following hints cannot match the name of input tables or views: \n"
                                        + "`%s(V99)`",
                                getTestSingleJoinHint()));
        String sql = "select /*+ %s(V99) */* from T1 join V4 on T1.a1 = V4.a4";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithEquiPred() {
        String sql = "select /*+ %s(T1) */* from T1, T2 where T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithEquiPredAndFilter() {
        String sql = "select /*+ %s(T1) */* from T1, T2 where T1.a1 = T2.a2 and T1.a1 > 1";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithEquiAndLocalPred() {
        String sql = "select /*+ %s(T1) */* from T1 inner join T2 on T1.a1 = T2.a2 and T1.a1 < 1";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithEquiAndNonEquiPred() {
        String sql =
                "select /*+ %s(T1) */* from T1 inner join T2 on T1.b1 = T2.b2 and T1.a1 < 1 and T1.a1 < T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithoutJoinPred() {
        String sql = "select /*+ %s(T1) */* from T1, T2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithNonEquiPred() {
        String sql = "select /*+ %s(T1) */* from T1 inner join T2 on T1.a1 > T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithLeftJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 left join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithLeftJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 left join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithRightJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithRightJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithFullJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 full join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithFullJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 full join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    // TODO currently join hint is not supported on SEMI join, it will use default join strategy by
    // planner
    @Test
    public void testJoinHintWithSemiJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 where a1 in (select a2 from T2)";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    // TODO currently join hint is not supported on SEMI join, it will use default join strategy by
    // planner
    @Test
    public void testJoinHintWithSemiJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 where a1 in (select a2 from T2)";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    // TODO currently join hint is not supported on ANTI join, it will use default join strategy by
    // planner
    @Test
    public void testJoinHintWithAntiJoinAndLeftSideAsBuildSide() {
        String sql = "select /*+ %s(T1) */* from T1 where a1 not in (select a2 from T2)";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    // TODO currently join hint is not supported on ANTI join, it will use default join strategy by
    // planner
    @Test
    public void testJoinHintWithAntiJoinAndRightSideAsBuildSide() {
        String sql = "select /*+ %s(T2) */* from T1 where a1 not in (select a2 from T2)";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiArgsAndLeftSideFirst() {
        // the first arg will be chosen as the build side
        String sql = "select /*+ %s(T1, T2) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithMultiArgsAndRightSideFirst() {
        // the first arg will be chosen as the build side
        String sql = "select /*+ %s(T2, T1) */* from T1 right join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testMultiJoinHints() {
        // the first join hint will be chosen
        String sql = "select /*+ %s(T1), %s */* from T1 join T2 on T1.a1 = T2.a2";

        String otherJoinHints =
                Strings.join(
                        getOtherJoinHints().stream()
                                .map(name -> String.format("%s(T1)", name))
                                .collect(Collectors.toList()),
                        ',');

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint(), otherJoinHints));
    }

    @Test
    public void testMultiJoinHintsWithTheFirstOneIsInvalid() {
        // the first join hint is invalid because it is not equi join except NEST_LOOP
        String sql = "select /*+ %s(T1), NEST_LOOP(T1) */* from T1 join T2 on T1.a1 > T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithoutAffectingJoinInView() {
        // the join in V2 will use the planner's default join strategy,
        // and the join between T1 and V2 will use BROADCAST
        util.tableEnv()
                .executeSql("create view V2 as select T1.* from T1 join T2 on T1.a1 = T2.a2");

        String sql = "select /*+ %s(T1)*/T1.* from T1 join V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithoutAffectingJoinInSubQuery() {
        // the join in sub-query will use the planner's default join strategy,
        // and the join outside will use BROADCAST
        String sql =
                "select /*+ %s(T1)*/T1.* from T1 join (select T1.* from T1 join T2 on T1.a1 = T2.a2) V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintWithTableAlias() {
        // the join in sub-query will use the planner's default join strategy,
        // and the join between T1 and alias V2 will use BROADCAST
        String sql =
                "select /*+ %s(V2)*/T1.* from T1 join (select T1.* from T1 join T2 on T1.a1 = T2.a2) V2 on T1.a1 = V2.a1";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintsWithMultiSameJoinHintsAndSingleArg() {
        // the first join hint will be chosen and T1 will be chosen as the build side
        String sql = "select /*+ %s(T1), %s(T2) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint(), getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintsWithDuplicatedArgs() {
        // T1 will be chosen as the build side
        String sql = "select /*+ %s(T1, T1) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint(), getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintsWithMultiSameJoinHintsAndMultiArgs() {
        // the first join hint will be chosen and T1 will be chosen as the build side
        String sql = "select /*+ %s(T1, T2), %s(T2, T1) */* from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint(), getTestSingleJoinHint()));
    }

    @Test
    public void testJoinHintsWithMultiHintsThrowException() {
        thrown().expect(SqlParserException.class);
        thrown().expectMessage("SQL parse failed.");
        String sql = "select /*+ %s(T1) */ /*+ %s(T2) */ * from T1 join T2 on T1.a1 = T2.a2";

        util.verifyRelPlan(String.format(sql, getTestSingleJoinHint(), getTestSingleJoinHint()));
    }
}

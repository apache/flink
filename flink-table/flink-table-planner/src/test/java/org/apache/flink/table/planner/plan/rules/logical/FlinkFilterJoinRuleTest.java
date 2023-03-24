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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link FlinkFilterJoinRule}. */
public class FlinkFilterJoinRuleTest extends TableTestBase {

    private BatchTableTestUtil util;

    @Before
    public void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(
                                        HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(
                                        RuleSets.ofList(
                                                CoreRules.FILTER_PROJECT_TRANSPOSE,
                                                FlinkFilterJoinRule.FILTER_INTO_JOIN,
                                                FlinkFilterJoinRule.JOIN_CONDITION_PUSH))
                                .build());
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE leftT (\n"
                                + "  a INT,\n"
                                + "  b BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE rightT (\n"
                                + "  c INT,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable1 (\n"
                                + "  a1 INT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable2 (\n"
                                + "  b2 BIGINT,\n"
                                + "  c2 VARCHAR,\n"
                                + "  a2 INT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    public void testFilterPushDownLeftSemi1() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT)) T WHERE T.b > 2");
    }

    @Test
    public void testFilterPushDownLeftSemi2() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT)) T WHERE T.b > 2");
    }

    @Test
    public void testFilterPushDownLeftSemi3() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c)) T WHERE T.b > 2");
    }

    @Test
    public void testJoinConditionPushDownLeftSemi1() {
        util.verifyRelPlan("SELECT * FROM leftT WHERE a IN (SELECT c FROM rightT WHERE b > 2)");
    }

    @Test
    public void testJoinConditionPushDownLeftSemi2() {
        util.verifyRelPlan("SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE b > 2)");
    }

    @Test
    public void testJoinConditionPushDownLeftSemi3() {
        util.verifyRelPlan(
                "SELECT * FROM leftT WHERE EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)");
    }

    @Test
    public void testFilterPushDownLeftAnti1() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE c < 3)) T WHERE T.b > 2");
    }

    @Test
    public void testFilterPushDownLeftAnti2() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT where c > 10)) T WHERE T.b > 2");
    }

    @Test
    public void testFilterPushDownLeftAnti3() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND c < 3)) T WHERE T.b > 2");
    }

    @Test
    public void testFilterPushDownLeftAnti4() {
        util.verifyRelPlan(
                "SELECT * FROM (SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c)) T WHERE T.b > 2");
    }

    @Test
    public void testJoinConditionPushDownLeftAnti1() {
        util.verifyRelPlan("SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b > 2)");
    }

    @Test
    public void testJoinConditionPushDownLeftAnti2() {
        util.verifyRelPlan(
                "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE b > 2)");
    }

    @Test
    public void testJoinConditionPushDownLeftAnti3() {
        util.verifyRelPlan(
                "SELECT * FROM leftT WHERE a NOT IN (SELECT c FROM rightT WHERE b = d AND b > 1)");
    }

    @Test
    public void testJoinConditionPushDownLeftAnti4() {
        util.verifyRelPlan(
                "SELECT * FROM leftT WHERE NOT EXISTS (SELECT * FROM rightT WHERE a = c AND b > 2)");
    }

    @Test
    public void testInnerJoinWithAllFilterFromBothSide() {
        // can not be pushed down
        util.verifyRelPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2 WHERE a1 = a2 + 2");
    }

    @Test
    public void testInnerJoinWithAllFilterInONClause() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2 AND b1 = b2 AND a1 = 2 AND b2 > 10");
    }

    @Test
    public void testInnerJoinWithSomeFiltersFromLeftSide() {
        util.verifyRelPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2 WHERE a1 = 2");
    }

    @Test
    public void testInnerJoinWithSomeFiltersFromRightSide() {
        util.verifyRelPlan("SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2 WHERE a2 = 2");
    }

    @Test
    public void testInnerJoinWithSomeFiltersFromLeftRightSide() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 JOIN MyTable2 ON a1 = a2 AND b1 = b2 AND c1 = c2 WHERE a2 = 2 AND b2 > 10 AND c1 IS NOT NULL");
    }

    @Test
    public void testInnerJoinWithAllFiltersFromWhere() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable2, MyTable1 WHERE b1 = b2 AND c1 = c2 AND a2 = 2 AND b2 > 10 AND COALESCE(c1, c2) <> '' ");
    }

    @Test
    public void testInnerJoinWithNullFilter() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 IS NULL");
    }

    @Test
    public void testInnerJoinWithNullFilter2() {
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 IS NULL AND a1 < 10");
    }

    @Test
    public void testInnerJoinWithFilter1() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 < 1");
    }

    @Test
    public void testInnerJoinWithFilter2() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 <> 1");
    }

    @Test
    public void testInnerJoinWithFilter3() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 > 1");
    }

    @Test
    public void testInnerJoinWithFilter4() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 >= 1");
    }

    @Test
    public void testInnerJoinWithFilter5() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 <= 1");
    }

    @Test
    public void testInnerJoinWithFilter6() {
        util.verifyRelPlan("SELECT * FROM MyTable1 INNER JOIN MyTable2 ON a1 = a2 WHERE a2 = null");
    }

    @Test
    public void testLeftJoinWithSomeFiltersFromLeftSide() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a1 = 2");
    }

    @Test
    public void testLeftJoinWithAllFilterInONClause() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 AND a2 = 2");
    }

    @Test
    public void testLeftJoinWithSomeFiltersFromLeftRightSide() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 AND b1 = b2 AND c1 = c2 WHERE a2 = 2 AND b2 > 10 AND c1 IS NOT NULL");
    }

    @Test
    public void testLeftJoinWithAllFiltersFromWhere() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON true WHERE b1 = b2 AND c1 = c2 AND a2 = 2 AND b2 > 10 AND COALESCE(c1, c2) <> '' ");
    }

    @Test
    public void testLeftJoinWithNullFilterInRightSide() {
        // Even if there is a filter 'a2 IS NULL', the 'a1 IS NULL' cannot be generated for left
        // join and this filter cannot be pushed down to both MyTable1 and MyTable2.
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 IS NULL");
    }

    @Test
    public void testLeftJoinWithNullFilterInRightSide2() {
        // 'a2 IS NULL' cannot infer that 'a1 IS NULL'.
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 IS NULL AND a1 < 10");
    }

    @Test
    public void testLeftJoinWithFilter1() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 < 1");
    }

    @Test
    public void testLeftJoinWithFilter2() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 <> 1");
    }

    @Test
    public void testLeftJoinWithFilter3() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 > 1");
    }

    @Test
    public void testLeftJoinWithFilter4() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 >= 1");
    }

    @Test
    public void testLeftJoinWithFilter5() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 <= 1");
    }

    @Test
    public void testLeftJoinWithFilter6() {
        util.verifyRelPlan("SELECT * FROM MyTable1 LEFT JOIN MyTable2 ON a1 = a2 WHERE a2 = null");
    }

    @Test
    public void testRightJoinWithAllFilterInONClause() {
        util.verifyRelPlan("SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON a1 = a2 AND a1 = 2");
    }

    @Test
    public void testRightJoinWithSomeFiltersFromRightSide() {
        util.verifyRelPlan("SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON a1 = a2 WHERE a2 = 2");
    }

    @Test
    public void testRightJoinWithSomeFiltersFromLeftRightSide() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON a1 = a2 AND b1 = b2 AND c1 = c2 WHERE a2 = 2 AND b2 > 10 AND c1 IS NOT NULL");
    }

    @Test
    public void testRightJoinWithAllFiltersFromWhere() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON true WHERE b1 = b2 AND c1 = c2 AND a2 = 2 AND b2 > 10 AND COALESCE(c1, c2) <> '' ");
    }

    @Test
    public void testRightJoinWithNullFilterInLeftSide() {
        // Even if there is a filter 'a1 IS NULL', the 'a2 IS NULL' cannot be generated for right
        // join and this filter cannot be pushed down to both MyTable1 and MyTable2.
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON a1 = a2 WHERE a1 IS NULL");
    }

    @Test
    public void testRightJoinWithNullFilterInRightSide2() {
        // 'a1 IS NULL' cannot infer that 'a2 IS NULL'. However, 'a2 < 10' can infer that 'a1 < 10',
        // and both of them can be pushed down.
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 RIGHT JOIN MyTable2 ON a1 = a2 WHERE a1 IS NULL AND a2 < 10");
    }

    @Test
    public void testFullJoinWithAllFilterInONClause() {
        util.verifyRelPlan("SELECT * FROM MyTable1 FULL JOIN MyTable2 ON a1 = a2 AND a1 = 2");
    }

    @Test
    public void testFullJoinWithSomeFiltersFromLeftSide() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 FULL JOIN MyTable2 ON a1 = a2 AND b1 = b2 WHERE a1 = 2");
    }

    @Test
    public void testFullJoinWithSomeFiltersFromRightSide() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 FULL JOIN MyTable2 ON a1 = a2 AND b1 = b2 WHERE a2 = 2");
    }

    @Test
    public void testFullJoinWithSomeFiltersFromLeftRightSide() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 FULL JOIN MyTable2 ON a1 = a2 AND b1 = b2 AND c1 = c2 WHERE a2 = 2 AND b2 > 10 AND c1 IS NOT NULL");
    }

    @Test
    public void testFullJoinWithAllFiltersFromWhere() {
        // will be converted to inner join
        util.verifyRelPlan(
                "SELECT * FROM MyTable2, MyTable1 WHERE b1 = b2 AND c1 = c2 AND a2 = 2 AND b2 > 10 AND COALESCE(c1, c2) <> '' ");
    }

    @Test
    public void testSemiJoin() {
        // TODO can not be pushed down now, support it later
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 WHERE (a1, b1, c1) IN (SELECT a2, b2, c2 FROM MyTable2 WHERE a2 = 2 AND b2 > 10) AND c1 IS NOT NULL");
    }

    @Test
    public void testAntiJoin() {
        // can not be pushed down
        util.verifyRelPlan(
                "SELECT * FROM MyTable1 WHERE (a1, b1, c1) NOT IN (select a2, b2, c2 FROM MyTable2 WHERE a2 = 2 AND b2 > 10) AND c1 IS NOT NULL");
    }
}

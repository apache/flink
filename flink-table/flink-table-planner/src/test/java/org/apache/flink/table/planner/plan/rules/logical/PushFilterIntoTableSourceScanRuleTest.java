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

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link PushFilterIntoTableSourceScanRule}. */
public class PushFilterIntoTableSourceScanRuleTest
        extends PushFilterIntoTableSourceScanRuleTestBase {

    @Before
    public void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        ((BatchTableTestUtil) util).buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
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
                                                PushFilterIntoTableSourceScanRule.INSTANCE,
                                                CoreRules.FILTER_PROJECT_TRANSPOSE))
                                .build());

        // name: STRING, id: LONG, amount: INT, price: DOUBLE
        String ddl1 =
                "CREATE TABLE MyTable (\n"
                        + "  name STRING,\n"
                        + "  id bigint,\n"
                        + "  amount int,\n"
                        + "  price double\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        String ddl2 =
                "CREATE TABLE VirtualTable (\n"
                        + "  name STRING,\n"
                        + "  id bigint,\n"
                        + "  amount int,\n"
                        + "  virtualField as amount + 1,\n"
                        + "  price double\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'amount',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";

        util.tableEnv().executeSql(ddl2);

        String ddl3 =
                "CREATE TABLE NestedTable (\n"
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  nested row<name string, `value` int>,\n"
                        + "  `deepNestedWith.` row<`.value` int, nested row<```name` string, `.value` int>>,\n"
                        + "  name string,\n"
                        + "  testMap Map<string, string>\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = '`deepNested.nested1.value`;`deepNestedWith..nested..value`;`deepNestedWith..nested.``name`;',"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl3);

        String ddl4 =
                "CREATE TABLE NestedItemTable (\n"
                        + "  `ID` INT,\n"
                        + "  `Timestamp` TIMESTAMP(3),\n"
                        + "  `Result` ROW<\n"
                        + "    `Mid` ROW<"
                        + "      `data_arr` ROW<`value` BIGINT> ARRAY,\n"
                        + "      `data_map` MAP<STRING, ROW<`value` BIGINT>>"
                        + "     >"
                        + "   >,\n"
                        + "   WATERMARK FOR `Timestamp` AS `Timestamp`\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'Result_Mid_data_map;',"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl4);
    }

    @Test
    public void testLowerUpperPushdown() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "  a STRING,\n"
                        + "  b STRING\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'filterable-fields' = 'a;b',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl);
        super.testLowerUpperPushdown();
    }

    @Test
    public void testWithInterval() {
        String ddl =
                "CREATE TABLE MTable (\n"
                        + "a TIMESTAMP(3),\n"
                        + "b TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true',\n"
                        + " 'filterable-fields' = 'a;b',\n"
                        + " 'disable-lookup' = 'true'"
                        + ")";
        util.tableEnv().executeSql(ddl);
        super.testWithInterval();
    }

    @Test
    public void testBasicNestedFilter() {
        util.verifyRelPlan("SELECT * FROM NestedTable WHERE deepNested.nested1.`value` > 2");
    }

    @Test
    public void testNestedFilterWithDotInTheName() {
        util.verifyRelPlan(
                "SELECT id FROM NestedTable WHERE `deepNestedWith.`.nested.`.value` > 5");
    }

    @Test
    public void testNestedFilterWithBacktickInTheName() {
        util.verifyRelPlan(
                "SELECT id FROM NestedTable WHERE `deepNestedWith.`.nested.```name` = 'foo'");
    }

    @Test
    public void testNestedFilterOnMapKey() {
        util.verifyRelPlan(
                "SELECT * FROM NestedItemTable WHERE"
                        + " `Result`.`Mid`.data_map['item'].`value` = 3");
    }

    @Test
    public void testNestedFilterOnArrayField() {
        util.verifyRelPlan(
                "SELECT * FROM NestedItemTable WHERE `Result`.`Mid`.data_arr[2].`value` = 3");
    }
}

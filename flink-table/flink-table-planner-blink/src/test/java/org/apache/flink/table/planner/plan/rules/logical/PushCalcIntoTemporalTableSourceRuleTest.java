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
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.rules.FlinkStreamRuleSets;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link PushCalcIntoTemporalTableSourceRule}. */
public class PushCalcIntoTemporalTableSourceRuleTest extends TableTestBase {
    private StreamTableTestUtil util = streamTestUtil(new TableConfig());

    @Before
    public void setup() {
        FlinkChainedProgram<StreamOptimizeContext> program = new FlinkChainedProgram<>();
        program.addLast(
                "Transforms",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(FlinkStreamRuleSets.EXPAND_PLAN_RULES())
                        .add(RuleSets.ofList(CoreRules.FILTER_INTO_JOIN))
                        .build());
        program.addLast(
                "Converters",
                FlinkVolcanoProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .add(
                                RuleSets.ofList(
                                        CoreRules.PROJECT_TO_CALC,
                                        CoreRules.FILTER_TO_CALC,
                                        FlinkLogicalCalc.CONVERTER(),
                                        FlinkLogicalTableSourceScan.CONVERTER(),
                                        FlinkLogicalSnapshot.CONVERTER(),
                                        FlinkLogicalJoin.CONVERTER(),
                                        new FlinkProjectJoinTransposeRule(
                                                PushProjector.ExprCondition.FALSE,
                                                RelFactories.LOGICAL_BUILDER)))
                        .setRequiredOutputTraits(new Convention[] {FlinkConventions.LOGICAL()})
                        .build());
        program.addLast(
                "Logical rewrite",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(
                                RuleSets.ofList(
                                        CalcSnapshotTransposeRule.INSTANCE(),
                                        FlinkCalcMergeRule.INSTANCE()))
                        .build());
        program.addLast(
                "PushCalcIntoTemporalTableSourceRule",
                FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                        .add(RuleSets.ofList(PushCalcIntoTemporalTableSourceRule.INSTANCE))
                        .build());
        util.replaceStreamProgram(program);

        String ddl1 =
                "CREATE TABLE table1 (\n"
                        + "  `a` bigint,\n"
                        + "  `b` bigint,\n"
                        + "  `c` string,\n"
                        + "  `proctime` as proctime()\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);

        String lookupDDL1 =
                "CREATE TABLE LookupTable (\n"
                        + "  `id` bigint,\n"
                        + "  `name` string,\n"
                        + "  `age` int\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(lookupDDL1);

        String lookupDDL2 =
                "CREATE TABLE LookupTableWithComputedColumn (\n"
                        + "  `id` bigint,\n"
                        + "  `name` string,\n"
                        + "  `age` int,\n"
                        + "  `computed_age` as age + 1\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(lookupDDL2);
    }

    @Test
    public void testLookupWithCalcPushIntoLookupTableSource() {
        String sqlQuery =
                "SELECT T.a, D.id FROM table1 AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id";

        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLookupWithConditionAndCalcPushIntoLookupTableSource() {
        String sqlQuery =
                "SELECT T.a, D.id FROM table1 AS T JOIN LookupTable "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id WHERE D.age + 1 > 60";

        util.verifyRelPlan(sqlQuery);
    }

    @Test
    public void testLookupWithComputedColumnAndCalcPushIntoLookupTableSource() {
        String sqlQuery =
                "SELECT T.a, D.id FROM table1 AS T JOIN LookupTableWithComputedColumn "
                        + "FOR SYSTEM_TIME AS OF T.proctime AS D ON T.a = D.id WHERE D.computed_age + 1 > 60";

        util.verifyRelPlan(sqlQuery);
    }
}
